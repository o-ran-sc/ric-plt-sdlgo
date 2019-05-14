/*
   Copyright (c) 2019 AT&T Intellectual Property.
   Copyright (c) 2018-2019 Nokia.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sdlgo

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
)

type iDatabase interface {
	SubscribeChannelDB(cb sdlgoredis.ChannelNotificationCb, channelPrefix, eventSeparator string, channels ...string)
	UnsubscribeChannelDB(channels ...string)
	MSet(pairs ...interface{}) error
	MSetPub(ns, message string, pairs ...interface{}) error
	MGet(keys []string) ([]interface{}, error)
	CloseDB() error
	Del(keys []string) error
	DelPub(channel, message string, keys []string) error
	Keys(key string) ([]string, error)
	SetIE(key string, oldData, newData interface{}) (bool, error)
	SetIEPub(channel, message, key string, oldData, newData interface{}) (bool, error)
	SetNX(key string, data interface{}) (bool, error)
	SetNXPub(channel, message, key string, data interface{}) (bool, error)
	DelIE(key string, data interface{}) (bool, error)
	DelIEPub(channel, message, key string, data interface{}) (bool, error)
}

//SdlInstance provides an API to read, write and modify
//key-value pairs in a given namespace.
type SdlInstance struct {
	nameSpace      string
	nsPrefix       string
	eventSeparator string
	iDatabase
}

//NewDatabase creates a connection to database that will be used
//as a backend for the key-value storage. The returned value shall
//be given as a parameter when calling NewKeyValStorage
func NewDatabase() *sdlgoredis.DB {
	return sdlgoredis.Create()
}

//NewSdlInstance creates a new sdl instance using the given namespace.
//The database used as a backend is given as a parameter
func NewSdlInstance(NameSpace string, db iDatabase) *SdlInstance {
	return &SdlInstance{
		nameSpace:      NameSpace,
		nsPrefix:       "{" + NameSpace + "},",
		eventSeparator: "___",
		iDatabase:      db,
	}
}

//SubscribeChannel lets you to subscribe for a events on a given channels.
//SDL notifications are events that are published on a specific channels.
//Both the channel and events are defined by the entity that is publishing
//the events.
//
//When subscribing for a channel, a callback function is given as a parameter.
//Whenever a notification is received from a channel, this callback is called
//with channel and notifications as parameter (several notifications could be
//packed to a single callback function call). A call to SubscribeChannel function
//returns immediatelly, callbacks will be called asyncronously.
//
//It is possible to subscribe to different channels using different callbacks. In
//this case simply use SubscribeChannel function separately for each channel.
//
//As there is a goroutine listening for notifications (and triggering the callbacks),
//it is a good practice not to use the callback function to do SDL operations.
//Instead, it should use Go channels to indicate that certain oprations to SDL is
//required.
func (s *SdlInstance) SubscribeChannel(cb func(string, ...string), channels ...string) error {
	s.SubscribeChannelDB(cb, s.nsPrefix, s.eventSeparator, s.setNamespaceToChannels(channels...)...)
	return nil
}

//UnsubscribeChannel removes subscription from one or several channels.
func (s *SdlInstance) UnsubscribeChannel(channels ...string) error {
	s.UnsubscribeChannelDB(s.setNamespaceToChannels(channels...)...)
	return nil
}

//Close connection to backend database.
func (s *SdlInstance) Close() error {
	return s.CloseDB()
}

func (s *SdlInstance) checkChannelsAndEvents(cmd string, channelsAndEvents []string) error {
	if len(channelsAndEvents)%2 != 0 {
		return fmt.Errorf("%s: Channels and events must be given as pairs", cmd)
	}
	for i, v := range channelsAndEvents {
		if i%2 != 0 {
			if strings.Contains(v, s.eventSeparator) {
				return fmt.Errorf("%s: event %s contains illegal substring (\"%s\")", cmd, v, s.eventSeparator)
			}
		}
	}
	return nil
}
func (s *SdlInstance) setNamespaceToChannels(channels ...string) []string {
	var retVal []string
	for _, v := range channels {
		retVal = append(retVal, s.nsPrefix+v)
	}
	return retVal
}

func (s *SdlInstance) setNamespaceToKeys(pairs ...interface{}) ([]interface{}, error) {
	retVal := make([]interface{}, 0)
	shouldBeKey := true
	for _, v := range pairs {
		reflectType := reflect.TypeOf(v)
		switch reflectType.Kind() {
		case reflect.Slice:
			if shouldBeKey {
				x := reflect.ValueOf(v)
				if x.Len()%2 != 0 {
					return []interface{}{}, errors.New("Key/value pairs doesn't match")
				}
				for i2 := 0; i2 < x.Len(); i2++ {
					if i2%2 == 0 {
						retVal = append(retVal, s.nsPrefix+x.Index(i2).Interface().(string))
					} else {
						retVal = append(retVal, x.Index(i2).Interface())
					}
				}
			} else {
				return []interface{}{}, errors.New("Key/value pairs doesn't match")
			}
		case reflect.Array:
			if shouldBeKey {
				x := reflect.ValueOf(v)
				if x.Len()%2 != 0 {
					return []interface{}{}, errors.New("Key/value pairs doesn't match")
				}
				for i2 := 0; i2 < x.Len(); i2++ {
					if i2%2 == 0 {
						retVal = append(retVal, s.nsPrefix+x.Index(i2).Interface().(string))
					} else {
						retVal = append(retVal, x.Index(i2).Interface())
					}
				}
			} else {
				return []interface{}{}, errors.New("Key/value pairs doesn't match")
			}
		default:
			if shouldBeKey {
				retVal = append(retVal, s.nsPrefix+v.(string))
				shouldBeKey = false
			} else {
				retVal = append(retVal, v)
				shouldBeKey = true
			}
		}
	}
	if len(retVal)%2 != 0 {
		return []interface{}{}, errors.New("Key/value pairs doesn't match")
	}
	return retVal, nil
}

func (s *SdlInstance) prepareChannelsAndEvents(channelsAndEvents []string) []string {
	channelEventMap := make(map[string]string)
	for i, v := range channelsAndEvents {
		if i%2 != 0 {
			continue
		}
		_, exists := channelEventMap[v]
		if exists {
			channelEventMap[v] = channelEventMap[v] + s.eventSeparator + channelsAndEvents[i+1]
		} else {
			channelEventMap[v] = channelsAndEvents[i+1]
		}
	}
	retVal := make([]string, 0)
	for k, v := range channelEventMap {
		retVal = append(retVal, s.nsPrefix+k)
		retVal = append(retVal, v)
	}
	return retVal
}

//SetAndPublish function writes data to shared data layer storage and send an event to
//a channel. Writing is done atomically, i.e. all succeeds or fails.
//Data to be written is given as key-value pairs. Several key-value
//pairs can be written with one call.
//The key is expected to be string whereas value can be anything, string,
//number, slice array or map
//
//Channels and events are given as pairs is channelsAndEvents parameter.
//Although it is possible to give sevral channel-event pairs, current implementation
//supports sending events to one channel only due to missing support in DB backend.
func (s *SdlInstance) SetAndPublish(channelsAndEvents []string, pairs ...interface{}) error {
	if len(pairs)%2 != 0 {
		return errors.New("Invalid pairs parameter")
	}

	keyAndData, err := s.setNamespaceToKeys(pairs...)
	if err != nil {
		return err
	}
	if len(channelsAndEvents) == 0 {
		return s.MSet(keyAndData...)
	}
	if err := s.checkChannelsAndEvents("SetAndPublish", channelsAndEvents); err != nil {
		return err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(channelsAndEvents)
	return s.MSetPub(channelsAndEventsPrepared[0], channelsAndEventsPrepared[1], keyAndData...)
}

//Set function writes data to shared data layer storage. Writing is done
//atomically, i.e. all succeeds or fails.
//Data to be written is given as key-value pairs. Several key-value
//pairs can be written with one call.
//The key is expected to be string whereas value can be anything, string,
//number, slice array or map
func (s *SdlInstance) Set(pairs ...interface{}) error {
	if len(pairs) == 0 {
		return nil
	}

	keyAndData, err := s.setNamespaceToKeys(pairs...)
	if err != nil {
		return err
	}
	return s.MSet(keyAndData...)
}

//Get function atomically reads one or more keys from SDL. The returned map has the
//requested keys as index and data as value. If the requested key is not found
//from SDL, it's value is nil
func (s *SdlInstance) Get(keys []string) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	if len(keys) == 0 {
		return m, nil
	}

	var keysWithNs []string
	for _, v := range keys {
		keysWithNs = append(keysWithNs, s.nsPrefix+v)
	}
	val, err := s.MGet(keysWithNs)
	if err != nil {
		return m, err
	}
	for i, v := range val {
		m[keys[i]] = v
	}
	return m, err
}

//SetIfAndPublish atomically replaces existing data with newData in SDL if data matches the oldData.
//If replace was done successfully, true will be returned. Also, if publishing was successfull, an event
//is published to a given channel.
func (s *SdlInstance) SetIfAndPublish(channelsAndEvents []string, key string, oldData, newData interface{}) (bool, error) {
	if len(channelsAndEvents) == 0 {
		return s.SetIE(s.nsPrefix+key, oldData, newData)
	}
	if err := s.checkChannelsAndEvents("SetIfAndPublish", channelsAndEvents); err != nil {
		return false, err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(channelsAndEvents)
	return s.SetIEPub(channelsAndEventsPrepared[0], channelsAndEventsPrepared[1], s.nsPrefix+key, oldData, newData)
}

//SetIf atomically replaces existing data with newData in SDL if data matches the oldData.
//If replace was done successfully, true will be returned.
func (s *SdlInstance) SetIf(key string, oldData, newData interface{}) (bool, error) {
	return s.SetIE(s.nsPrefix+key, oldData, newData)
}

//SetIfNotExistsAndPublish conditionally sets the value of a key. If key already exists in SDL,
//then it's value is not changed. Checking the key existence and potential set operation
//is done atomically. If the set operation was done successfully, an event is published to a
//given channel.
func (s *SdlInstance) SetIfNotExistsAndPublish(channelsAndEvents []string, key string, data interface{}) (bool, error) {
	if len(channelsAndEvents) == 0 {
		return s.SetNX(s.nsPrefix+key, data)
	}
	if err := s.checkChannelsAndEvents("SetIfNotExistsAndPublish", channelsAndEvents); err != nil {
		return false, err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(channelsAndEvents)
	return s.SetNXPub(channelsAndEventsPrepared[0], channelsAndEventsPrepared[1], s.nsPrefix+key, data)
}

//SetIfNotExists conditionally sets the value of a key. If key already exists in SDL,
//then it's value is not changed. Checking the key existence and potential set operation
//is done atomically.
func (s *SdlInstance) SetIfNotExists(key string, data interface{}) (bool, error) {
	return s.SetNX(s.nsPrefix+key, data)
}

//RemoveAndPublish removes data from SDL. Operation is done atomically, i.e. either all succeeds or fails.
//An event is published into a given channel if remove operation is successfull.
func (s *SdlInstance) RemoveAndPublish(channelsAndEvents []string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	var keysWithNs []string
	for _, v := range keys {
		keysWithNs = append(keysWithNs, s.nsPrefix+v)
	}
	if len(channelsAndEvents) == 0 {
		return s.Del(keysWithNs)
	}
	if err := s.checkChannelsAndEvents("RemoveAndPublish", channelsAndEvents); err != nil {
		return err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(channelsAndEvents)
	return s.DelPub(channelsAndEventsPrepared[0], channelsAndEventsPrepared[1], keysWithNs)
}

//Remove data from SDL. Operation is done atomically, i.e. either all succeeds or fails.
func (s *SdlInstance) Remove(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	var keysWithNs []string
	for _, v := range keys {
		keysWithNs = append(keysWithNs, s.nsPrefix+v)
	}
	err := s.Del(keysWithNs)
	return err
}

//RemoveIfAndPublish removes data from SDL conditionally and if remove was done successfully,
//a given event is published to channel. If existing data matches given data,
//key and data are removed from SDL. If remove was done successfully, true is returned.
func (s *SdlInstance) RemoveIfAndPublish(channelsAndEvents []string, key string, data interface{}) (bool, error) {
	if len(channelsAndEvents) == 0 {
		return s.DelIE(s.nsPrefix+key, data)
	}
	if err := s.checkChannelsAndEvents("RemoveIfAndPublish", channelsAndEvents); err != nil {
		return false, err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(channelsAndEvents)
	return s.DelIEPub(channelsAndEventsPrepared[0], channelsAndEventsPrepared[1], s.nsPrefix+key, data)
}

//RemoveIf removes data from SDL conditionally. If existing data matches given data,
//key and data are removed from SDL. If remove was done successfully, true is returned.
func (s *SdlInstance) RemoveIf(key string, data interface{}) (bool, error) {
	status, err := s.DelIE(s.nsPrefix+key, data)
	if err != nil {
		return false, err
	}
	return status, nil
}

//GetAll returns all keys under the namespace. No prior knowledge about the keys in the
//given namespace exists, thus operation is not guaranteed to be atomic or isolated.
func (s *SdlInstance) GetAll() ([]string, error) {
	keys, err := s.Keys(s.nsPrefix + "*")
	var retVal []string
	if err != nil {
		return retVal, err
	}
	for _, v := range keys {
		retVal = append(retVal, strings.Split(v, s.nsPrefix)[1])
	}
	return retVal, err
}

//RemoveAll removes all keys under the namespace. Remove operation is not atomic, thus
//it is not guaranteed that all keys are removed.
func (s *SdlInstance) RemoveAll() error {
	keys, err := s.Keys(s.nsPrefix + "*")
	if err != nil {
		return err
	}
	if (keys != nil) && (len(keys) != 0) {
		err = s.Del(keys)
	}
	return err
}

func (s *SdlInstance) RemoveAllAndPublish(channelsAndEvents []string) error {
	keys, err := s.Keys(s.nsPrefix + "*")
	if err != nil {
		return err
	}
	if (keys != nil) && (len(keys) != 0) {
		if len(channelsAndEvents) == 0 {
			return s.Del(keys)
		}
		if err := s.checkChannelsAndEvents("RemoveIfAndPublish", channelsAndEvents); err != nil {
			return err
		}
		channelsAndEventsPrepared := s.prepareChannelsAndEvents(channelsAndEvents)
		err = s.DelPub(channelsAndEventsPrepared[0], channelsAndEventsPrepared[1], keys)
	}
	return err

}
