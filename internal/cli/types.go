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

package cli

import (
	"fmt"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"strings"
)

//iDatabase is an interface towards database backend, for the time being
//sdlgoredis.DB implements this interface.
type iDatabase interface {
	Info() (*sdlgoredis.DbInfo, error)
	State() (*sdlgoredis.DbState, error)
	Keys(pattern string) ([]string, error)
	Statistics() (*sdlgoredis.DbStatistics, error)
}

//Database struct is a holder for the internal database instances.
type Database struct {
	Instances []iDatabase
}

//DbCreateCb callback function type to create a new database
type DbCreateCb func() *Database

//iSyncStorage is an interface towards SDL SyncStorage API
type ISyncStorage interface {
	Get(ns string, keys []string) (map[string]interface{}, error)
	ListKeys(ns string, pattern string) ([]string, error)
	Set(ns string, pairs ...interface{}) error
	Remove(ns string, keys []string) error
}

//SyncStorageCreateCb callback function type to create a new SyncStorageInterface
type SyncStorageCreateCb func() ISyncStorage

//keysArgs struct is used for keys command arguments.
type keysArgs struct {
	ns      string
	pattern string
}

//NewKeysArgs constructs a new keysArgs struct.
func NewKeysArgs(ns string, pattern string) keysArgs {
	return keysArgs{
		ns:      ns,
		pattern: pattern,
	}
}

//Validate command arguments in keysArgs.
func (k keysArgs) Validate() error {
	if strings.Contains(k.ns, "*") {
		return fmt.Errorf("Invalid character (*) in given %s namespace argument.", k.ns)
	}
	return nil
}

//nsMap is a map having SDL DB cluster address as a key and namespace map of type nsKeyMap as a value
type nsMap map[string]nsKeyMap

//nsKeyMap is a map having namespace as a key and DB key count as a value
type nsKeyMap map[string]uint32

//SdlCliApp constant defines the name of the SDL CLI application
const SdlCliApp = "sdlcli"
