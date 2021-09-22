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

//NewSdlInstanceForTest is used in unit tests only in order to replace the
//underlying redis implementation with mock
//Deprecated: Will be removed in a future release.
func NewSdlInstanceForTest(NameSpace string, instance iDatabase) *SdlInstance {
	db := &Database{}
	db.instances = append(db.instances, instance)

	return &SdlInstance{
		nameSpace: NameSpace,
		nsPrefix:  "{" + NameSpace + "},",
		storage:   newSyncStorage(db),
	}
}

// NewSyncStorageForTest is used only in unit tests to mock database.
func NewSyncStorageForTest(dbMock iDatabase) *SyncStorage {
	db := &Database{}
	db.instances = append(db.instances, dbMock)

	return newSyncStorage(db)
}
