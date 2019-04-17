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
	"reflect"
	"strings"

	"gerrit.oran-osc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
)

type iDatabase interface {
	MSet(pairs ...interface{}) error
	MGet(keys []string) ([]interface{}, error)
	Close() error
	Del(keys []string) error
	Keys(key string) ([]string, error)
	SetIE(key string, oldData, newData interface{}) (bool, error)
	SetNX(key string, data interface{}) (bool, error)
	DelIE(key string, data interface{}) (bool, error)
}

type SdlInstance struct {
	nameSpace string
	nsPrefix  string
	iDatabase
}

//NewDatabase creates a connection to database that will be used
//as a backend for the key-value storage. The returned value shall
//be given as a parameter when calling NewKeyValStorage
func NewDatabase() *sdlgoredis.DB {
	db := sdlgoredis.Create()
	return db
}

//NewSdlInstance creates a new sdl instance using the given namespace.
//The database used as a backend is given as a parameter
func NewSdlInstance(NameSpace string, db iDatabase) *SdlInstance {
	s := SdlInstance{
		nameSpace: NameSpace,
		nsPrefix:  "{" + NameSpace + "},",
		iDatabase: db,
	}

	return &s
}

func (s *SdlInstance) Close() error {
	return s.Close()
}

func (s *SdlInstance) setNamespaceToKeys(pairs ...interface{}) []interface{} {
	var retVal []interface{}
	for i, v := range pairs {
		if i%2 == 0 {
			reflectType := reflect.TypeOf(v)
			switch reflectType.Kind() {
			case reflect.Slice:
				x := reflect.ValueOf(v)
				for i2 := 0; i2 < x.Len(); i2++ {
					if i2%2 == 0 {
						retVal = append(retVal, s.nsPrefix+x.Index(i2).Interface().(string))
					} else {
						retVal = append(retVal, x.Index(i2).Interface())
					}
				}
			case reflect.Array:
				x := reflect.ValueOf(v)
				for i2 := 0; i2 < x.Len(); i2++ {
					if i2%2 == 0 {
						retVal = append(retVal, s.nsPrefix+x.Index(i2).Interface().(string))
					} else {
						retVal = append(retVal, x.Index(i2).Interface())
					}
				}
			default:
				retVal = append(retVal, s.nsPrefix+v.(string))
			}
		} else {
			retVal = append(retVal, v)
		}
	}
	return retVal
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

	keyAndData := s.setNamespaceToKeys(pairs...)
	err := s.MSet(keyAndData...)
	return err
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

//SetIf atomically replaces existing data with newData in SDL if data matches the oldData.
//If replace was done successfully, true will be returned.
func (s *SdlInstance) SetIf(key string, oldData, newData interface{}) (bool, error) {
	status, err := s.SetIE(s.nsPrefix+key, oldData, newData)
	if err != nil {
		return false, err
	}
	return status, nil
}

//SetIfNotExists conditionally sets the value of a key. If key already exists in SDL,
//then it's value is not changed. Checking the key existence and potential set operation
//is done atomically.
func (s *SdlInstance) SetIfNotExists(key string, data interface{}) (bool, error) {
	status, err := s.SetNX(s.nsPrefix+key, data)
	if err != nil {
		return false, err
	}
	return status, nil
}

//Remove data from SDL. Operation is done atomically, i.e. either all succeeds or fails
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
	var retVal []string = nil
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
	if keys != nil {
		err = s.Del(keys)
	}
	return err
}
