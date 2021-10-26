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

package sdlgoredis

import (
	"fmt"
)

//DbState struct is a holder for DB state information, which is received from
//sdlgoredis sentinel 'Master' and 'Slaves' calls output.
type DbState struct {
	MasterDbState   MasterDbState
	ReplicasDbState *ReplicasDbState
}

//MasterDbState struct is a holder for master Redis state information.
type MasterDbState struct {
	Err    error
	Fields MasterDbStateFields
}

//ReplicasDbState struct is a holder for Redis slaves state information.
type ReplicasDbState struct {
	Err    error
	States []*ReplicaDbState
}

//ReplicaDbState struct is a holder for one Redis slave state information.
type ReplicaDbState struct {
	Fields ReplicaDbStateFields
}

//MasterDbStateFields struct is a holder for master Redis state information
//fields which are read from sdlgoredis sentinel 'Master' call output.
type MasterDbStateFields struct {
	Role        string
	Ip          string
	Port        string
	ReplicasCnt string
	Flags       string
}

//ReplicaDbStateFields struct is a holder for master Redis state information
//fields which are read from sdlgoredis sentinel 'Slaves' call output.
type ReplicaDbStateFields struct {
	Role             string
	Ip               string
	Port             string
	MasterLinkStatus string
	Flags            string
}

func (dbst *DbState) IsOnline() error {
	if err := dbst.MasterDbState.IsOnline(); err != nil {
		return err
	}
	if dbst.ReplicasDbState != nil {
		if err := dbst.ReplicasDbState.IsOnline(); err != nil {
			return err
		}
	}
	return nil
}

func (mdbst *MasterDbState) IsOnline() error {
	if mdbst.Err != nil {
		return mdbst.Err
	}
	if mdbst.Fields.Role != "master" {
		return fmt.Errorf("No master DB, current role '%s'", mdbst.Fields.Role)
	}
	if mdbst.Fields.Flags != "master" {
		return fmt.Errorf("Master flags are '%s', expected 'master'", mdbst.Fields.Flags)
	}
	return nil
}

func (mdbst *MasterDbState) GetAddress() string {
	if mdbst.Fields.Ip != "" || mdbst.Fields.Port != "" {
		return mdbst.Fields.Ip + ":" + mdbst.Fields.Port
	} else {
		return ""
	}
}

func (rdbst *ReplicasDbState) IsOnline() error {
	if rdbst.Err != nil {
		return rdbst.Err
	}
	for _, info := range rdbst.States {
		if err := info.IsOnline(); err != nil {
			return err
		}
	}
	return nil
}

func (rdbst *ReplicaDbState) IsOnline() error {
	if rdbst.Fields.Role != "slave" {
		return fmt.Errorf("Replica role is '%s', expected 'slave'", rdbst.Fields.Role)
	}

	if rdbst.Fields.MasterLinkStatus != "ok" {
		return fmt.Errorf("Replica link to the master is down")
	}

	if rdbst.Fields.Flags != "slave" {
		return fmt.Errorf("Replica flags are '%s', expected 'slave'", rdbst.Fields.Flags)
	}
	return nil
}

func (rdbst *ReplicaDbState) GetAddress() string {
	if rdbst.Fields.Ip != "" || rdbst.Fields.Port != "" {
		return rdbst.Fields.Ip + ":" + rdbst.Fields.Port
	} else {
		return ""
	}
}
