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
	Err           error
	ConfigNodeCnt int

	PrimaryDbState   PrimaryDbState
	ReplicasDbState  *ReplicasDbState
	SentinelsDbState *SentinelsDbState
}

//PrimaryDbState struct is a holder for primary Redis state information.
type PrimaryDbState struct {
	Err    error
	Fields PrimaryDbStateFields
}

//ReplicasDbState struct is a holder for Redis replicas state information.
type ReplicasDbState struct {
	Err    error
	States []*ReplicaDbState
}

//ReplicaDbState struct is a holder for one Redis replica state information.
type ReplicaDbState struct {
	Fields ReplicaDbStateFields
}

//SentinelsDbState struct is a holder for Redis sentinels state information.
type SentinelsDbState struct {
	Err    error
	States []*SentinelDbState
}

//SentinelDbState struct is a holder for one Redis sentinel state information.
type SentinelDbState struct {
	Fields SentinelDbStateFields
}

//PrimaryDbStateFields struct is a holder for primary Redis state information
//fields which are read from sdlgoredis sentinel 'Master' call output.
type PrimaryDbStateFields struct {
	Role  string
	Ip    string
	Port  string
	Flags string
}

//ReplicaDbStateFields struct is a holder for replica Redis state information
//fields which are read from sdlgoredis sentinel 'Slaves' call output.
type ReplicaDbStateFields struct {
	Role              string
	Ip                string
	Port              string
	PrimaryLinkStatus string
	Flags             string
}

//SentinelDbStateFields struct is a holder for sentinel Redis state information
//fields which are read from sdlgoredis sentinel 'Sentinels' call output.
type SentinelDbStateFields struct {
	Ip    string
	Port  string
	Flags string
}

func (dbst *DbState) IsOnline() error {
	if err := dbst.Err; err != nil {
		return err
	}

	if err := dbst.PrimaryDbState.IsOnline(); err != nil {
		return err
	}
	if dbst.ReplicasDbState != nil {
		if err := dbst.ReplicasDbState.IsOnline(); err != nil {
			return err
		}
	}
	if dbst.SentinelsDbState != nil {
		if err := dbst.SentinelsDbState.IsOnline(); err != nil {
			return err
		}
	}
	return nil
}

func (pdbst *PrimaryDbState) IsOnline() error {
	if pdbst.Err != nil {
		return pdbst.Err
	}
	if pdbst.Fields.Role != "master" {
		return fmt.Errorf("No primary DB, current role '%s'", pdbst.Fields.Role)
	}
	if pdbst.Fields.Flags != "master" {
		return fmt.Errorf("Primary flags are '%s', expected 'master'", pdbst.Fields.Flags)
	}
	return nil
}

func (pdbst *PrimaryDbState) GetAddress() string {
	if pdbst.Fields.Ip != "" || pdbst.Fields.Port != "" {
		return pdbst.Fields.Ip + ":" + pdbst.Fields.Port
	} else {
		return ""
	}
}

func (rdbst *ReplicasDbState) IsOnline() error {
	if rdbst.Err != nil {
		return rdbst.Err
	}
	for _, state := range rdbst.States {
		if err := state.IsOnline(); err != nil {
			return err
		}
	}
	return nil
}

func (rdbst *ReplicaDbState) IsOnline() error {
	if rdbst.Fields.Role != "slave" {
		return fmt.Errorf("Replica role is '%s', expected 'slave'", rdbst.Fields.Role)
	}

	if rdbst.Fields.PrimaryLinkStatus != "ok" {
		return fmt.Errorf("Replica link to the primary is down")
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

func (sdbst *SentinelsDbState) IsOnline() error {
	if sdbst.Err != nil {
		return sdbst.Err
	}
	for _, state := range sdbst.States {
		if err := state.IsOnline(); err != nil {
			return err
		}
	}
	return nil
}

func (sdbst *SentinelDbState) IsOnline() error {
	if sdbst.Fields.Flags != "sentinel" {
		return fmt.Errorf("Sentinel flags are '%s', expected 'sentinel'", sdbst.Fields.Flags)
	}
	return nil
}

func (sdbst *SentinelDbState) GetAddress() string {
	if sdbst.Fields.Ip != "" || sdbst.Fields.Port != "" {
		return sdbst.Fields.Ip + ":" + sdbst.Fields.Port
	} else {
		return ""
	}
}
