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
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"time"
)

//SdlInstance provides an API to read, write and modify
//key-value pairs in a given namespace.
//Deprecated: Will be removed in a future release, please use instead SyncStorage
//type defined in syncstorage.go.
type SdlInstance struct {
	nameSpace string
	nsPrefix  string
	storage   *SyncStorage
}

//Database struct is a holder for the internal database instance. Applications
//can use this exported data type to locally store a reference to database
//instance returned from NewDabase() function.
type Database struct {
	instances []iDatabase
}

//NewDatabase creates a connection to database that will be used
//as a backend for the key-value storage. The returned value
//can be reused between multiple SDL instances in which case each instance
//is using the same connection.
//Deprecated: Will be removed in a future release, because there is no need to
//create a database before NewSyncStorage function is called, database will
//be created automatically by NewSyncStorage function.
func NewDatabase() *Database {
	db := &Database{}
	for _, v := range sdlgoredis.Create() {
		db.instances = append(db.instances, v)
	}
	return db
}

//NewSdlInstance creates a new sdl instance using the given namespace.
//The database used as a backend is given as a parameter
//Deprecated: Will be removed in a future release, please use NewSyncStorage
//function instead.
func NewSdlInstance(NameSpace string, db *Database) *SdlInstance {
	return &SdlInstance{
		nameSpace: NameSpace,
		nsPrefix:  "{" + NameSpace + "},",
		storage:   newSyncStorage(db),
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
//Deprecated: Will be removed in a future release, please use the SubscribeChannel
//receiver function of the SyncStorage type.
func (s *SdlInstance) SubscribeChannel(cb func(string, ...string), channels ...string) error {
	return s.storage.SubscribeChannel(s.nameSpace, cb, channels...)
}

//UnsubscribeChannel removes subscription from one or several channels.
//Deprecated: Will be removed in a future release, please use the UnsubscribeChannel
//receiver function of the SyncStorage type.
func (s *SdlInstance) UnsubscribeChannel(channels ...string) error {
	return s.storage.UnsubscribeChannel(s.nameSpace, channels...)
}

//Close connection to backend database.
//Deprecated: Will be removed in a future release, please use the Close receiver
//function of the SyncStorage type.
func (s *SdlInstance) Close() error {
	return s.storage.Close()
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
//
//Deprecated: Will be removed in a future release, please use the SetAndPublish
//receiver function of the SyncStorage type.
func (s *SdlInstance) SetAndPublish(channelsAndEvents []string, pairs ...interface{}) error {
	return s.storage.SetAndPublish(s.nameSpace, channelsAndEvents, pairs...)
}

//Set function writes data to shared data layer storage. Writing is done
//atomically, i.e. all succeeds or fails.
//Data to be written is given as key-value pairs. Several key-value
//pairs can be written with one call.
//The key is expected to be string whereas value can be anything, string,
//number, slice array or map
//Deprecated: Will be removed in a future release, please use the Set receiver
//function of the SyncStorage type.
func (s *SdlInstance) Set(pairs ...interface{}) error {
	return s.storage.Set(s.nameSpace, pairs...)
}

//Get function atomically reads one or more keys from SDL. The returned map has the
//requested keys as index and data as value. If the requested key is not found
//from SDL, it's value is nil.
//Deprecated: Will be removed in a future release, please use the Get receiver
//function of the SyncStorage type.
func (s *SdlInstance) Get(keys []string) (map[string]interface{}, error) {
	return s.storage.Get(s.nameSpace, keys)
}

//SetIfAndPublish atomically replaces existing data with newData in SDL if data matches the oldData.
//If replace was done successfully, true will be returned. Also, if publishing was successfull, an event
//is published to a given channel.
//Deprecated: Will be removed in a future release, please use the SetIfAndPublish
//receiver function of the SyncStorage type.
func (s *SdlInstance) SetIfAndPublish(channelsAndEvents []string, key string, oldData, newData interface{}) (bool, error) {
	return s.storage.SetIfAndPublish(s.nameSpace, channelsAndEvents, key, oldData, newData)
}

//SetIf atomically replaces existing data with newData in SDL if data matches the oldData.
//If replace was done successfully, true will be returned.
//Deprecated: Will be removed in a future release, please use the SetIf receiver
//function of the SyncStorage type.
func (s *SdlInstance) SetIf(key string, oldData, newData interface{}) (bool, error) {
	return s.storage.SetIf(s.nameSpace, key, oldData, newData)
}

//SetIfNotExistsAndPublish conditionally sets the value of a key. If key already exists in SDL,
//then it's value is not changed. Checking the key existence and potential set operation
//is done atomically. If the set operation was done successfully, an event is published to a
//given channel.
//Deprecated: Will be removed in a future release, please use the SetIfNotExistsAndPublish
//receiver function of the SyncStorage type.
func (s *SdlInstance) SetIfNotExistsAndPublish(channelsAndEvents []string, key string, data interface{}) (bool, error) {
	return s.storage.SetIfNotExistsAndPublish(s.nameSpace, channelsAndEvents, key, data)
}

//SetIfNotExists conditionally sets the value of a key. If key already exists in SDL,
//then it's value is not changed. Checking the key existence and potential set operation
//is done atomically.
//Deprecated: Will be removed in a future release, please use the SetIfNotExists
//receiver function of the SyncStorage type.
func (s *SdlInstance) SetIfNotExists(key string, data interface{}) (bool, error) {
	return s.storage.SetIfNotExists(s.nameSpace, key, data)
}

//RemoveAndPublish removes data from SDL. Operation is done atomically, i.e. either all succeeds or fails.
//Trying to remove a nonexisting key is not considered as an error.
//An event is published into a given channel if remove operation is successfull and
//at least one key is removed (if several keys given). If the given key(s) doesn't exist
//when trying to remove, no event is published.
//Deprecated: Will be removed in a future release, please use the RemoveAndPublish
//receiver function of the SyncStorage type.
func (s *SdlInstance) RemoveAndPublish(channelsAndEvents []string, keys []string) error {
	return s.storage.RemoveAndPublish(s.nameSpace, channelsAndEvents, keys)
}

//Remove data from SDL. Operation is done atomically, i.e. either all succeeds or fails.
//Deprecated: Will be removed in a future release, please use the Remove receiver
//function of the SyncStorage type.
func (s *SdlInstance) Remove(keys []string) error {
	return s.storage.Remove(s.nameSpace, keys)
}

//RemoveIfAndPublish removes data from SDL conditionally and if remove was done successfully,
//a given event is published to channel. If existing data matches given data,
//key and data are removed from SDL. If remove was done successfully, true is returned.
//Deprecated: Will be removed in a future release, please use the RemoveIfAndPublish
//receiver function of the SyncStorage type.
func (s *SdlInstance) RemoveIfAndPublish(channelsAndEvents []string, key string, data interface{}) (bool, error) {
	return s.storage.RemoveIfAndPublish(s.nameSpace, channelsAndEvents, key, data)
}

//RemoveIf removes data from SDL conditionally. If existing data matches given data,
//key and data are removed from SDL. If remove was done successfully, true is returned.
//Deprecated: Will be removed in a future release, please use the RemoveIf receiver
//function of the SyncStorage type.
func (s *SdlInstance) RemoveIf(key string, data interface{}) (bool, error) {
	return s.storage.RemoveIf(s.nameSpace, key, data)
}

//GetAll returns all keys under the namespace. No prior knowledge about the keys in the
//given namespace exists, thus operation is not guaranteed to be atomic or isolated.
//Deprecated: Will be removed in a future release, please use the GetAll receiver
//function of the SyncStorage type.
func (s *SdlInstance) GetAll() ([]string, error) {
	return s.storage.GetAll(s.nameSpace)
}

//RemoveAll removes all keys under the namespace. Remove operation is not atomic, thus
//it is not guaranteed that all keys are removed.
//Deprecated: Will be removed in a future release, please use the RemoveAll receiver
//function of the SyncStorage type.
func (s *SdlInstance) RemoveAll() error {
	return s.storage.RemoveAll(s.nameSpace)
}

//RemoveAllAndPublish removes all keys under the namespace and if successfull, it
//will publish an event to given channel. This operation is not atomic, thus it is
//not guaranteed that all keys are removed.
//Deprecated: Will be removed in a future release, please use the RemoveAllAndPublish
//receiver function of the SyncStorage type.
func (s *SdlInstance) RemoveAllAndPublish(channelsAndEvents []string) error {
	return s.storage.RemoveAllAndPublish(s.nameSpace, channelsAndEvents)
}

//AddMember adds a new members to a group.
//
//SDL groups are unordered collections of members where each member is
//unique. It is possible to add the same member several times without the
//need to check if it already exists.
//Deprecated: Will be removed in a future release, please use the AddMember
//receiver function of the SyncStorage type.
func (s *SdlInstance) AddMember(group string, member ...interface{}) error {
	return s.storage.AddMember(s.nameSpace, group, member...)
}

//RemoveMember removes members from a group.
//Deprecated: Will be removed in a future release, please use the RemoveMember
//receiver function of the SyncStorage type.
func (s *SdlInstance) RemoveMember(group string, member ...interface{}) error {
	return s.storage.RemoveMember(s.nameSpace, group, member...)
}

//RemoveGroup removes the whole group along with it's members.
//Deprecated: Will be removed in a future release, please use the RemoveGroup
//receiver function of the SyncStorage type.
func (s *SdlInstance) RemoveGroup(group string) error {
	return s.storage.RemoveGroup(s.nameSpace, group)
}

//GetMembers returns all the members from a group.
//Deprecated: Will be removed in a future release, please use the GetMembers
//receiver function of the SyncStorage type.
func (s *SdlInstance) GetMembers(group string) ([]string, error) {
	return s.storage.GetMembers(s.nameSpace, group)
}

//IsMember returns true if given member is found from a group.
func (s *SdlInstance) IsMember(group string, member interface{}) (bool, error) {
	return s.storage.IsMember(s.nameSpace, group, member)
}

//GroupSize returns the number of members in a group.
//Deprecated: Will be removed in a future release, please use the GroupSize
//receiver function of the SyncStorage type.
func (s *SdlInstance) GroupSize(group string) (int64, error) {
	return s.storage.GroupSize(s.nameSpace, group)
}

//LockResource function is used for locking a resource. The resource lock in
//practice is a key with random value that is set to expire after a time
//period. The value written to key is a random value, thus only the instance
//created a lock, can release it. Resource locks are per namespace.
//Deprecated: Will be removed in a future release, please use the LockResource
//receiver function of the SyncStorage type.
func (s *SdlInstance) LockResource(resource string, expiration time.Duration, opt *Options) (*Lock, error) {
	l, err := s.storage.LockResource(s.nameSpace, resource, expiration, opt)
	if l != nil {
		return &Lock{
			s:           s,
			storageLock: l,
		}, err
	}
	return nil, err
}

//ReleaseResource removes the lock from a resource. If lock is already
//expired or some other instance is keeping the lock (lock taken after expiration),
//an error is returned.
//Deprecated: Will be removed in a future release, please use the ReleaseResource
//receiver function of the SyncStorageLock type.
func (l *Lock) ReleaseResource() error {
	return l.storageLock.ReleaseResource(l.s.nameSpace)
}

//RefreshResource function can be used to set a new expiration time for the
//resource lock (if the lock still exists). The old remaining expiration
//time is overwritten with the given new expiration time.
//Deprecated: Will be removed in a future release, please use the RefreshResource
//receiver function of the SyncStorageLock type.
func (l *Lock) RefreshResource(expiration time.Duration) error {
	return l.storageLock.RefreshResource(l.s.nameSpace, expiration)
}

//CheckResource returns the expiration time left for a resource.
//If the resource doesn't exist, -2 is returned.
//Deprecated: Will be removed in a future release, please use the CheckResource
//receiver function of the SyncStorage type.
func (s *SdlInstance) CheckResource(resource string) (time.Duration, error) {
	return s.storage.CheckResource(s.nameSpace, resource)
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
//Deprecated: Will be removed in a future release, please use instead the SyncStorageLock
//type defined in syncstorage.go.
type Lock struct {
	s           *SdlInstance
	storageLock *SyncStorageLock
}
