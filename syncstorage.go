/*
   Copyright (c) 2021 AT&T Intellectual Property.
   Copyright (c) 2018-2021 Nokia.

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
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"hash/crc32"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"
)

//SyncStorage provides multi-namespace APIs to read, write and modify key-value
//pairs. key-values are belonging to a namespace and SyncStorage provides APIs
//where namespace can be given in every API call. This means that with
//SyncStorage you can easily set key-values under different namespace compared to
//SdlInstance where namespace can be defined only at SdlInstance instance creation
//time.
type SyncStorage struct {
	mutex sync.Mutex
	tmp   []byte
	db    *Database
}

//NewSyncStorage creates a new sdl instance.
//The database used as a backend is given as a parameter
func NewSyncStorage() *SyncStorage {
	return newSyncStorage(NewDatabase())
}

func newSyncStorage(db *Database) *SyncStorage {
	return &SyncStorage{
		db: db,
	}
}

//selectDbInstance Selects DB instance what provides DB services for the namespace
func (s *SyncStorage) getDbBackend(ns string) iDatabase {
	instanceCount := uint32(len(s.db.instances))
	instanceID := getHash(ns) % instanceCount
	return s.db.instances[instanceID]
}

//getHash Returns hash value calculated from the string
func getHash(s string) uint32 {
	tbl := crc32.MakeTable(crc32.IEEE)
	return crc32.Checksum([]byte(s), tbl)
}

//SubscribeChannel lets you to subscribe for a events on a given channels.
//SDL notifications are events that are published on a specific channels.
//Both the channel and events are defined by the entity that is publishing
//the events under given namespace.
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
func (s *SyncStorage) SubscribeChannel(ns string, cb func(string, ...string), channels ...string) error {
	nsPrefix := getNsPrefix(ns)
	return s.getDbBackend(ns).SubscribeChannelDB(cb, s.setNamespaceToChannels(nsPrefix, channels...)...)
}

//UnsubscribeChannel removes subscription from one or several channels under given
//namespace.
func (s *SyncStorage) UnsubscribeChannel(ns string, channels ...string) error {
	nsPrefix := getNsPrefix(ns)
	return s.getDbBackend(ns).UnsubscribeChannelDB(s.setNamespaceToChannels(nsPrefix, channels...)...)
}

//Close connection to backend database.
func (s *SyncStorage) Close() error {
	var ret error
	for _, db := range s.db.instances {
		if err := db.CloseDB(); err != nil {
			ret = err
		}
	}
	return ret
}

func (s *SyncStorage) checkChannelsAndEvents(cmd string, channelsAndEvents []string) error {
	if len(channelsAndEvents)%2 != 0 {
		return fmt.Errorf("%s: Channels and events must be given as pairs", cmd)
	}
	for i, v := range channelsAndEvents {
		if i%2 != 0 {
			if strings.Contains(v, sdlgoredis.EventSeparator) {
				return fmt.Errorf("%s: event %s contains illegal substring (\"%s\")", cmd, v, sdlgoredis.EventSeparator)
			}
		}
	}
	return nil
}

func (s *SyncStorage) setNamespaceToChannels(nsPrefix string, channels ...string) []string {
	var retVal []string
	for _, v := range channels {
		retVal = append(retVal, nsPrefix+v)
	}
	return retVal
}

func (s *SyncStorage) setNamespaceToKeys(nsPrefix string, pairs ...interface{}) ([]interface{}, error) {
	retVal := make([]interface{}, 0)
	shouldBeKey := true
	for _, v := range pairs {
		reflectType := reflect.TypeOf(v)
		switch reflectType.Kind() {
		case reflect.Map:
			x := reflect.ValueOf(v).MapRange()
			for x.Next() {
				retVal = append(retVal, nsPrefix+x.Key().Interface().(string))
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
						retVal = append(retVal, nsPrefix+x.Index(i2).Interface().(string))
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
						retVal = append(retVal, nsPrefix+x.Index(i2).Interface().(string))
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
				retVal = append(retVal, nsPrefix+v.(string))
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

func (s *SyncStorage) prepareChannelsAndEvents(nsPrefix string, channelsAndEvents []string) []string {
	channelEventMap := make(map[string]string)
	for i, v := range channelsAndEvents {
		if i%2 != 0 {
			continue
		}
		_, exists := channelEventMap[v]
		if exists {
			channelEventMap[v] = channelEventMap[v] + sdlgoredis.EventSeparator + channelsAndEvents[i+1]
		} else {
			channelEventMap[v] = channelsAndEvents[i+1]
		}
	}
	retVal := make([]string, 0)
	for k, v := range channelEventMap {
		retVal = append(retVal, nsPrefix+k)
		retVal = append(retVal, v)
	}
	return retVal
}

//SetAndPublish function writes data to shared data layer storage and sends an event to
//a channel. Writing is done atomically, i.e. all succeeds or fails.
//Data is written under the namespace what is given as a parameter for this function.
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
func (s *SyncStorage) SetAndPublish(ns string, channelsAndEvents []string, pairs ...interface{}) error {
	nsPrefix := getNsPrefix(ns)
	keyAndData, err := s.setNamespaceToKeys(nsPrefix, pairs...)
	if err != nil {
		return err
	}
	if len(channelsAndEvents) == 0 {
		return s.getDbBackend(ns).MSet(keyAndData...)
	}
	if err := s.checkChannelsAndEvents("SetAndPublish", channelsAndEvents); err != nil {
		return err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(nsPrefix, channelsAndEvents)
	return s.getDbBackend(ns).MSetMPub(channelsAndEventsPrepared, keyAndData...)
}

//Set function writes data to shared data layer storage. Writing is done
//atomically, i.e. all succeeds or fails.
//Data is written under the namespace what is given as a parameter for this function.
//Data to be written is given as key-value pairs. Several key-value
//pairs can be written with one call.
//The key is expected to be string whereas value can be anything, string,
//number, slice array or map
func (s *SyncStorage) Set(ns string, pairs ...interface{}) error {
	if len(pairs) == 0 {
		return nil
	}

	keyAndData, err := s.setNamespaceToKeys(getNsPrefix(ns), pairs...)
	if err != nil {
		return err
	}
	return s.getDbBackend(ns).MSet(keyAndData...)
}

//Get function atomically reads one or more keys from SDL. The returned map has the
//requested keys as index and data as value. If the requested key is not found
//from SDL, it's value is nil
//Read operation is targeted to the namespace what is given as a parameter for this
//function.
func (s *SyncStorage) Get(ns string, keys []string) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	if len(keys) == 0 {
		return m, nil
	}

	var keysWithNs []string
	for _, v := range keys {
		keysWithNs = append(keysWithNs, getNsPrefix(ns)+v)
	}
	val, err := s.getDbBackend(ns).MGet(keysWithNs)
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
//Data is written under the namespace what is given as a parameter for this function.
func (s *SyncStorage) SetIfAndPublish(ns string, channelsAndEvents []string, key string, oldData, newData interface{}) (bool, error) {
	nsPrefix := getNsPrefix(ns)
	if len(channelsAndEvents) == 0 {
		return s.getDbBackend(ns).SetIE(nsPrefix+key, oldData, newData)
	}
	if err := s.checkChannelsAndEvents("SetIfAndPublish", channelsAndEvents); err != nil {
		return false, err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(nsPrefix, channelsAndEvents)
	return s.getDbBackend(ns).SetIEPub(channelsAndEventsPrepared, nsPrefix+key, oldData, newData)
}

//SetIf atomically replaces existing data with newData in SDL if data matches the oldData.
//If replace was done successfully, true will be returned.
//Data is written under the namespace what is given as a parameter for this function.
func (s *SyncStorage) SetIf(ns string, key string, oldData, newData interface{}) (bool, error) {
	return s.getDbBackend(ns).SetIE(getNsPrefix(ns)+key, oldData, newData)
}

//SetIfNotExistsAndPublish conditionally sets the value of a key. If key already exists in SDL,
//then it's value is not changed. Checking the key existence and potential set operation
//is done atomically. If the set operation was done successfully, an event is published to a
//given channel.
//Data is written under the namespace what is given as a parameter for this function.
func (s *SyncStorage) SetIfNotExistsAndPublish(ns string, channelsAndEvents []string, key string, data interface{}) (bool, error) {
	nsPrefix := getNsPrefix(ns)
	if len(channelsAndEvents) == 0 {
		return s.getDbBackend(ns).SetNX(nsPrefix+key, data, 0)
	}
	if err := s.checkChannelsAndEvents("SetIfNotExistsAndPublish", channelsAndEvents); err != nil {
		return false, err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(nsPrefix, channelsAndEvents)
	return s.getDbBackend(ns).SetNXPub(channelsAndEventsPrepared, nsPrefix+key, data)
}

//SetIfNotExists conditionally sets the value of a key. If key already exists in SDL,
//then it's value is not changed. Checking the key existence and potential set operation
//is done atomically.
//Data is written under the namespace what is given as a parameter for this function.
func (s *SyncStorage) SetIfNotExists(ns string, key string, data interface{}) (bool, error) {
	return s.getDbBackend(ns).SetNX(getNsPrefix(ns)+key, data, 0)
}

//RemoveAndPublish removes data from SDL. Operation is done atomically, i.e. either all succeeds or fails.
//Trying to remove a nonexisting key is not considered as an error.
//An event is published into a given channel if remove operation is successfull and
//at least one key is removed (if several keys given). If the given key(s) doesn't exist
//when trying to remove, no event is published.
//Data is removed under the namespace what is given as a parameter for this function.
func (s *SyncStorage) RemoveAndPublish(ns string, channelsAndEvents []string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	var keysWithNs []string
	nsPrefix := getNsPrefix(ns)
	for _, v := range keys {
		keysWithNs = append(keysWithNs, nsPrefix+v)
	}
	if len(channelsAndEvents) == 0 {
		return s.getDbBackend(ns).Del(keysWithNs)
	}
	if err := s.checkChannelsAndEvents("RemoveAndPublish", channelsAndEvents); err != nil {
		return err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(nsPrefix, channelsAndEvents)
	return s.getDbBackend(ns).DelMPub(channelsAndEventsPrepared, keysWithNs)
}

//Remove data from SDL. Operation is done atomically, i.e. either all succeeds or fails.
//Data is removed under the namespace what is given as a parameter for this function.
func (s *SyncStorage) Remove(ns string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	var keysWithNs []string
	for _, v := range keys {
		keysWithNs = append(keysWithNs, getNsPrefix(ns)+v)
	}
	err := s.getDbBackend(ns).Del(keysWithNs)
	return err
}

//RemoveIfAndPublish removes data from SDL conditionally and if remove was done successfully,
//a given event is published to channel. If existing data matches given data,
//key and data are removed from SDL. If remove was done successfully, true is returned.
//Data is removed under the namespace what is given as a parameter for this function.
func (s *SyncStorage) RemoveIfAndPublish(ns string, channelsAndEvents []string, key string, data interface{}) (bool, error) {
	nsPrefix := getNsPrefix(ns)
	if len(channelsAndEvents) == 0 {
		return s.getDbBackend(ns).DelIE(nsPrefix+key, data)
	}
	if err := s.checkChannelsAndEvents("RemoveIfAndPublish", channelsAndEvents); err != nil {
		return false, err
	}
	channelsAndEventsPrepared := s.prepareChannelsAndEvents(nsPrefix, channelsAndEvents)
	return s.getDbBackend(ns).DelIEPub(channelsAndEventsPrepared, nsPrefix+key, data)
}

//RemoveIf removes data from SDL conditionally. If existing data matches given data,
//key and data are removed from SDL. If remove was done successfully, true is returned.
//Data is removed under the namespace what is given as a parameter for this function.
func (s *SyncStorage) RemoveIf(ns string, key string, data interface{}) (bool, error) {
	status, err := s.getDbBackend(ns).DelIE(getNsPrefix(ns)+key, data)
	if err != nil {
		return false, err
	}
	return status, nil
}

//GetAll returns all keys under the namespace. No prior knowledge about the keys in the
//given namespace exists, thus operation is not guaranteed to be atomic or isolated.
func (s *SyncStorage) GetAll(ns string) ([]string, error) {
	nsPrefix := getNsPrefix(ns)
	keys, err := s.getDbBackend(ns).Keys(nsPrefix + "*")
	var retVal []string
	if err != nil {
		return retVal, err
	}
	for _, v := range keys {
		retVal = append(retVal, strings.Split(v, nsPrefix)[1])
	}
	return retVal, err
}

// ListKeys returns all keys in the given namespace matching key search pattern.
//
//  Supported search glob-style patterns:
//    h?llo matches hello, hallo and hxllo
//    h*llo matches hllo and heeeello
//    h[ae]llo matches hello and hallo, but not hillo
//    h[^e]llo matches hallo, hbllo, ... but not hello
//    h[a-b]llo matches hallo and hbllo
//
//  The \ escapes character in key search pattern and those will be treated as a normal
//  character:
//    h\[?llo\* matches h[ello* and h[allo*
//
// No prior knowledge about the keys in the given namespace exists,
// thus operation is not guaranteed to be atomic or isolated.
func (s *SyncStorage) ListKeys(ns string, pattern string) ([]string, error) {
	nsPrefix := getNsPrefix(ns)
	nsKeys, err := s.getDbBackend(ns).Keys(nsPrefix + pattern)
	var keys []string
	if err != nil {
		return keys, err
	}
	for _, key := range nsKeys {
		keys = append(keys, strings.Split(key, nsPrefix)[1])
	}
	return keys, err
}

//RemoveAll removes all keys under the namespace. Remove operation is not atomic, thus
//it is not guaranteed that all keys are removed.
func (s *SyncStorage) RemoveAll(ns string) error {
	keys, err := s.getDbBackend(ns).Keys(getNsPrefix(ns) + "*")
	if err != nil {
		return err
	}
	if (keys != nil) && (len(keys) != 0) {
		err = s.getDbBackend(ns).Del(keys)
	}
	return err
}

//RemoveAllAndPublish removes all keys under the namespace and if successfull, it
//will publish an event to given channel. This operation is not atomic, thus it is
//not guaranteed that all keys are removed.
func (s *SyncStorage) RemoveAllAndPublish(ns string, channelsAndEvents []string) error {
	nsPrefix := getNsPrefix(ns)
	keys, err := s.getDbBackend(ns).Keys(nsPrefix + "*")
	if err != nil {
		return err
	}
	if (keys != nil) && (len(keys) != 0) {
		if len(channelsAndEvents) == 0 {
			return s.getDbBackend(ns).Del(keys)
		}
		if err := s.checkChannelsAndEvents("RemoveAllAndPublish", channelsAndEvents); err != nil {
			return err
		}
		channelsAndEventsPrepared := s.prepareChannelsAndEvents(nsPrefix, channelsAndEvents)
		err = s.getDbBackend(ns).DelMPub(channelsAndEventsPrepared, keys)
	}
	return err
}

//AddMember adds a new members to a group under given namespace.
//
//SDL groups are unordered collections of members where each member is
//unique. It is possible to add the same member several times without the
//need to check if it already exists.
func (s *SyncStorage) AddMember(ns string, group string, member ...interface{}) error {
	return s.getDbBackend(ns).SAdd(getNsPrefix(ns)+group, member...)
}

//RemoveMember removes members from a group under given namespace.
func (s *SyncStorage) RemoveMember(ns string, group string, member ...interface{}) error {
	return s.getDbBackend(ns).SRem(getNsPrefix(ns)+group, member...)
}

//RemoveGroup removes the whole group along with it's members under given namespace.
func (s *SyncStorage) RemoveGroup(ns string, group string) error {
	return s.getDbBackend(ns).Del([]string{getNsPrefix(ns) + group})
}

//GetMembers returns all the members from a group under given namespace.
func (s *SyncStorage) GetMembers(ns string, group string) ([]string, error) {
	retVal, err := s.getDbBackend(ns).SMembers(getNsPrefix(ns) + group)
	if err != nil {
		return []string{}, err
	}
	return retVal, err
}

//IsMember returns true if given member is found from a group under given namespace.
func (s *SyncStorage) IsMember(ns string, group string, member interface{}) (bool, error) {
	retVal, err := s.getDbBackend(ns).SIsMember(getNsPrefix(ns)+group, member)
	if err != nil {
		return false, err
	}
	return retVal, err
}

//GroupSize returns the number of members in a group under given namespace.
func (s *SyncStorage) GroupSize(ns string, group string) (int64, error) {
	retVal, err := s.getDbBackend(ns).SCard(getNsPrefix(ns) + group)
	if err != nil {
		return 0, err
	}
	return retVal, err
}

func (s *SyncStorage) randomToken() (string, error) {
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

//LockResource function is used for locking a resource under given namespace.
//The resource lock in practice is a key with random value that is set to expire
//after a time period. The value written to key is a random value, thus only the
//instance created a lock, can release it. Resource locks are per namespace.
func (s *SyncStorage) LockResource(ns string, resource string, expiration time.Duration, opt *Options) (*SyncStorageLock, error) {
	value, err := s.randomToken()
	if err != nil {
		return nil, err
	}

	var retryTimer *time.Timer
	for i, attempts := 0, opt.getRetryCount()+1; i < attempts; i++ {
		ok, err := s.getDbBackend(ns).SetNX(getNsPrefix(ns)+resource, value, expiration)
		if err != nil {
			return nil, err
		} else if ok {
			return &SyncStorageLock{s: s, key: resource, value: value}, nil
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

//ReleaseResource removes the lock from a resource under given namespace. If lock
//is already expired or some other instance is keeping the lock (lock taken after
//expiration), an error is returned.
func (l *SyncStorageLock) ReleaseResource(ns string) error {
	ok, err := l.s.getDbBackend(ns).DelIE(getNsPrefix(ns)+l.key, l.value)

	if err != nil {
		return err
	}
	if !ok {
		return errors.New("Lock not held")
	}
	return nil
}

//RefreshResource function can be used to set a new expiration time for the
//resource lock (if the lock still exists) under given namespace. The old
//remaining expiration time is overwritten with the given new expiration time.
func (l *SyncStorageLock) RefreshResource(ns string, expiration time.Duration) error {
	err := l.s.getDbBackend(ns).PExpireIE(getNsPrefix(ns)+l.key, l.value, expiration)
	return err
}

//CheckResource returns the expiration time left for a resource under given
//namespace. If the resource doesn't exist, -2 is returned.
func (s *SyncStorage) CheckResource(ns string, resource string) (time.Duration, error) {
	result, err := s.getDbBackend(ns).PTTL(getNsPrefix(ns) + resource)
	if err != nil {
		return 0, err
	}
	if result == time.Duration(-1) {
		return 0, errors.New("invalid resource given, no expiration time attached")
	}
	return result, nil
}

//SyncStorageLock struct identifies the resource lock instance. Releasing and adjusting the
//expirations are done using the methods defined for this struct.
type SyncStorageLock struct {
	s     *SyncStorage
	key   string
	value string
}

func getNsPrefix(ns string) string {
	return "{" + ns + "}" + sdlgoredis.NsSeparator
}

type iDatabase interface {
	SubscribeChannelDB(cb func(string, ...string), channels ...string) error
	UnsubscribeChannelDB(channels ...string) error
	MSet(pairs ...interface{}) error
	MSetMPub(channelsAndEvents []string, pairs ...interface{}) error
	MGet(keys []string) ([]interface{}, error)
	CloseDB() error
	Del(keys []string) error
	DelMPub(channelsAndEvents []string, keys []string) error
	Keys(key string) ([]string, error)
	SetIE(key string, oldData, newData interface{}) (bool, error)
	SetIEPub(channelsAndEvents []string, key string, oldData, newData interface{}) (bool, error)
	SetNX(key string, data interface{}, expiration time.Duration) (bool, error)
	SetNXPub(channelsAndEvents []string, key string, data interface{}) (bool, error)
	DelIE(key string, data interface{}) (bool, error)
	DelIEPub(channelsAndEvents []string, key string, data interface{}) (bool, error)
	SAdd(key string, data ...interface{}) error
	SRem(key string, data ...interface{}) error
	SMembers(key string) ([]string, error)
	SIsMember(key string, data interface{}) (bool, error)
	SCard(key string) (int64, error)
	PTTL(key string) (time.Duration, error)
	PExpireIE(key string, data interface{}, expiration time.Duration) error
}
