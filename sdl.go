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

/*
 * This source code is part of the near-RT RIC (RAN Intelligent Controller)
 * platform project (RICP).
 */

package sdlgo

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
)

//SdlInstance provides an API to read, write and modify
//key-value pairs in a given namespace.
type SdlInstance struct {
	nameSpace      string
	nsPrefix       string
	eventSeparator string
	mutex          sync.Mutex
	tmp            []byte
	iDatabase
}

//Database struct is a holder for the internal database instance. Applications
//can use this exported data type to locally store a reference to database
//instance returned from NewDabase() function.
type Database struct {
	instance iDatabase
}

//NewDatabase creates a connection to database that will be used
//as a backend for the key-value storage. The returned value
//can be reused between multiple SDL instances in which case each instance
//is using the same connection.
func NewDatabase() *Database {
	return &Database{
		instance: sdlgoredis.Create(),
	}
}

//NewSdlInstance creates a new sdl instance using the given namespace.
//The database used as a backend is given as a parameter
func NewSdlInstance(NameSpace string, db *Database) *SdlInstance {
	return &SdlInstance{
		nameSpace:      NameSpace,
		nsPrefix:       "{" + NameSpace + "},",
		eventSeparator: "___",
		iDatabase:      db.instance,
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
//When receiving events in callback routine, it is a good practive to return from
//callback as quickly as possible. E.g. reading in callback context should be avoided
//and using of Go signals is recommended. Also it should be noted that in case of several
//events received from different channels, callbacks are called in series one by one.
//
//This function is NOT SAFE FOR CONCURRENT USE by multiple goroutines.
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
		case reflect.Map:
			x := reflect.ValueOf(v).MapRange()
			for x.Next() {
				retVal = append(retVal, s.nsPrefix+x.Key().Interface().(string))
				retVal = append(retVal, x.Value().Interface())
			}
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
				if reflectType.Elem().Kind() == reflect.Uint8 {
					retVal = append(retVal, v)
					shouldBeKey = true
				} else {
					return []interface{}{}, errors.New("Key/value pairs doesn't match")
				}
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
				if reflectType.Elem().Kind() == reflect.Uint8 {
					retVal = append(retVal, v)
					shouldBeKey = true
				} else {
					return []interface{}{}, errors.New("Key/value pairs doesn't match")
				}
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

//SetAndPublish function writes data to shared data layer storage and sends an event to
//a channel. Writing is done atomically, i.e. all succeeds or fails.
//Data to be written is given as key-value pairs. Several key-value
//pairs can be written with one call.
//The key is expected to be string whereas value can be anything, string,
//number, slice array or map
//
//If data was set successfully, an event is sent to a channel.
//Channels and events are given as pairs is channelsAndEvents parameter.
//It is possible to send several events to several channels by giving several
//channel-event pairs.
//  E.g. []{"channel1", "event1", "channel2", "event2", "channel1", "event3"}
//will send event1 and event3 to channel1 and event2 to channel2.
func (s *SdlInstance) SetAndPublish(channelsAndEvents []string, pairs ...interface{}) error {
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
	return s.MSetMPub(channelsAndEventsPrepared, keyAndData...)
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
		return s.SetNX(s.nsPrefix+key, data, 0)
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
	return s.SetNX(s.nsPrefix+key, data, 0)
}

//RemoveAndPublish removes data from SDL. Operation is done atomically, i.e. either all succeeds or fails.
//Trying to remove a nonexisting key is not considered as an error.
//An event is published into a given channel if remove operation is successfull and
//at least one key is removed (if several keys given). If the given key(s) doesn't exist
//when trying to remove, no event is published.
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
	return s.DelMPub(channelsAndEventsPrepared, keysWithNs)
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

//RemoveAllAndPublish removes all keys under the namespace and if successfull, it
//will publish an event to given channel. This operation is not atomic, thus it is
//not guaranteed that all keys are removed.
func (s *SdlInstance) RemoveAllAndPublish(channelsAndEvents []string) error {
	keys, err := s.Keys(s.nsPrefix + "*")
	if err != nil {
		return err
	}
	if (keys != nil) && (len(keys) != 0) {
		if len(channelsAndEvents) == 0 {
			return s.Del(keys)
		}
		if err := s.checkChannelsAndEvents("RemoveAllAndPublish", channelsAndEvents); err != nil {
			return err
		}
		channelsAndEventsPrepared := s.prepareChannelsAndEvents(channelsAndEvents)
		err = s.DelMPub(channelsAndEventsPrepared, keys)
	}
	return err
}

//AddMember adds a new members to a group.
//
//SDL groups are unordered collections of members where each member is
//unique. It is possible to add the same member several times without the
//need to check if it already exists.
func (s *SdlInstance) AddMember(group string, member ...interface{}) error {
	return s.SAdd(s.nsPrefix+group, member...)
}

//RemoveMember removes members from a group.
func (s *SdlInstance) RemoveMember(group string, member ...interface{}) error {
	return s.SRem(s.nsPrefix+group, member...)
}

//RemoveGroup removes the whole group along with it's members.
func (s *SdlInstance) RemoveGroup(group string) error {
	return s.Del([]string{s.nsPrefix + group})
}

//GetMembers returns all the members from a group.
func (s *SdlInstance) GetMembers(group string) ([]string, error) {
	retVal, err := s.SMembers(s.nsPrefix + group)
	if err != nil {
		return []string{}, err
	}
	return retVal, err
}

//IsMember returns true if given member is found from a group.
func (s *SdlInstance) IsMember(group string, member interface{}) (bool, error) {
	retVal, err := s.SIsMember(s.nsPrefix+group, member)
	if err != nil {
		return false, err
	}
	return retVal, err
}

//GroupSize returns the number of members in a group.
func (s *SdlInstance) GroupSize(group string) (int64, error) {
	retVal, err := s.SCard(s.nsPrefix + group)
	if err != nil {
		return 0, err
	}
	return retVal, err
}

func (s *SdlInstance) randomToken() (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.tmp) == 0 {
		s.tmp = make([]byte, 16)
	}

	if _, err := io.ReadFull(rand.Reader, s.tmp); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(s.tmp), nil
}

//LockResource function is used for locking a resource. The resource lock in
//practice is a key with random value that is set to expire after a time
//period. The value written to key is a random value, thus only the instance
//created a lock, can release it. Resource locks are per namespace.
func (s *SdlInstance) LockResource(resource string, expiration time.Duration, opt *Options) (*Lock, error) {
	value, err := s.randomToken()
	if err != nil {
		return nil, err
	}

	var retryTimer *time.Timer
	for i, attempts := 0, opt.getRetryCount()+1; i < attempts; i++ {
		ok, err := s.SetNX(s.nsPrefix+resource, value, expiration)
		if err != nil {
			return nil, err
		} else if ok {
			return &Lock{s: s, key: resource, value: value}, nil
		}
		if retryTimer == nil {
			retryTimer = time.NewTimer(opt.getRetryWait())
			defer retryTimer.Stop()
		} else {
			retryTimer.Reset(opt.getRetryWait())
		}

		select {
		case <-retryTimer.C:
		}
	}
	return nil, errors.New("Lock not obtained")
}

//ReleaseResource removes the lock from a resource. If lock is already
//expired or some other instance is keeping the lock (lock taken after expiration),
//an error is returned.
func (l *Lock) ReleaseResource() error {
	ok, err := l.s.DelIE(l.s.nsPrefix+l.key, l.value)

	if err != nil {
		return err
	}
	if !ok {
		return errors.New("Lock not held")
	}
	return nil
}

//RefreshResource function can be used to set a new expiration time for the
//resource lock (if the lock still exists). The old remaining expiration
//time is overwritten with the given new expiration time.
func (l *Lock) RefreshResource(expiration time.Duration) error {
	err := l.s.PExpireIE(l.s.nsPrefix+l.key, l.value, expiration)
	return err
}

//CheckResource returns the expiration time left for a resource.
//If the resource doesn't exist, -2 is returned.
func (s *SdlInstance) CheckResource(resource string) (time.Duration, error) {
	result, err := s.PTTL(s.nsPrefix + resource)
	if err != nil {
		return 0, err
	}
	if result == time.Duration(-1) {
		return 0, errors.New("invalid resource given, no expiration time attached")
	}
	return result, nil
}

//Options struct defines the behaviour for getting the resource lock.
type Options struct {
	//The number of time the lock will be tried.
	//Default: 0 = no retry
	RetryCount int

	//Wait between the retries.
	//Default: 100ms
	RetryWait time.Duration
}

func (o *Options) getRetryCount() int {
	if o != nil && o.RetryCount > 0 {
		return o.RetryCount
	}
	return 0
}

func (o *Options) getRetryWait() time.Duration {
	if o != nil && o.RetryWait > 0 {
		return o.RetryWait
	}
	return 100 * time.Millisecond
}

//Lock struct identifies the resource lock instance. Releasing and adjusting the
//expirations are done using the methods defined for this struct.
type Lock struct {
	s     *SdlInstance
	key   string
	value string
}

type iDatabase interface {
	SubscribeChannelDB(cb func(string, ...string), channelPrefix, eventSeparator string, channels ...string)
	UnsubscribeChannelDB(channels ...string)
	MSet(pairs ...interface{}) error
	MSetMPub(channelsAndEvents []string, pairs ...interface{}) error
	MGet(keys []string) ([]interface{}, error)
	CloseDB() error
	Del(keys []string) error
	DelMPub(channelsAndEvents []string, keys []string) error
	Keys(key string) ([]string, error)
	SetIE(key string, oldData, newData interface{}) (bool, error)
	SetIEPub(channel, message, key string, oldData, newData interface{}) (bool, error)
	SetNX(key string, data interface{}, expiration time.Duration) (bool, error)
	SetNXPub(channel, message, key string, data interface{}) (bool, error)
	DelIE(key string, data interface{}) (bool, error)
	DelIEPub(channel, message, key string, data interface{}) (bool, error)
	SAdd(key string, data ...interface{}) error
	SRem(key string, data ...interface{}) error
	SMembers(key string) ([]string, error)
	SIsMember(key string, data interface{}) (bool, error)
	SCard(key string) (int64, error)
	PTTL(key string) (time.Duration, error)
	PExpireIE(key string, data interface{}, expiration time.Duration) error
}
