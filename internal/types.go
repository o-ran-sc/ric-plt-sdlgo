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

package internal

//iDatabase is an interface towards database backend, for the time being
//sdlgoredis.DB implements this interface.
type iDatabase interface {
	Info() (*DbInfo, error)
}

//Database struct is a holder for the internal database instances.
type Database struct {
	Instances []iDatabase
}

//DbCreateCb callback function type to create a new database
type DbCreateCb func() *Database

//DbInfo struct is a holder for DB information, which is received from
//sdlgoredis 'info' call's output.
type DbInfo struct {
	MasterRole      bool
	ConfReplicasCnt uint32
	Replicas        []DbInfoReplica
}

//DbInfoReplica struct is a holder for DB replica information, which is
//received from sdlgoredis 'info' call's output.
type DbInfoReplica struct {
	Addr   string
	Online bool
}
