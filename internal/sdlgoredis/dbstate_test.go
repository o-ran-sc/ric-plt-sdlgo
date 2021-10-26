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

package sdlgoredis_test

import (
	"errors"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/stretchr/testify/assert"
	"testing"
)

type dbStateMock struct {
	state sdlgoredis.DbState
}

func setupDbState() *dbStateMock {
	return new(dbStateMock)
}

func (ds *dbStateMock) setMasterError(err error) {
	ds.state.MasterDbState.Err = err
}

func (ds *dbStateMock) setMasterFields(role, ip, port, rCnt, flags string) {
	ds.state.MasterDbState.Fields.Role = role
	ds.state.MasterDbState.Fields.Ip = ip
	ds.state.MasterDbState.Fields.Port = port
	ds.state.MasterDbState.Fields.ReplicasCnt = rCnt
	ds.state.MasterDbState.Fields.Flags = flags
}

func (ds *dbStateMock) setReplicaError(err error) {
	if ds.state.ReplicasDbState == nil {
		ds.state.ReplicasDbState = new(sdlgoredis.ReplicasDbState)
	}
	ds.state.ReplicasDbState.Err = err
}

func (ds *dbStateMock) addReplicaFields(role, ip, port, mls, flags string) {
	if ds.state.ReplicasDbState == nil {
		ds.state.ReplicasDbState = new(sdlgoredis.ReplicasDbState)
	}
	newState := new(sdlgoredis.ReplicaDbState)
	newState.Fields.Role = role
	newState.Fields.Ip = ip
	newState.Fields.Port = port
	newState.Fields.MasterLinkStatus = mls
	newState.Fields.Flags = flags
	ds.state.ReplicasDbState.States = append(ds.state.ReplicasDbState.States, newState)
}

func TestIsOnlineWhenSingleMasterSuccessfully(t *testing.T) {
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "0", "master")
	err := st.state.IsOnline()
	assert.Nil(t, err)
}

func TestIsOnlineWhenSingleMasterFailureIfErrorHasSet(t *testing.T) {
	testErr := errors.New("Some error")
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "0", "master")
	st.setMasterError(testErr)
	err := st.state.IsOnline()
	assert.Equal(t, testErr, err)
}

func TestIsOnlineWhenSingleMasterFailureIfNotMasterRole(t *testing.T) {
	expErr := errors.New("No master DB, current role 'not-master'")
	st := setupDbState()
	st.setMasterFields("not-master", "1.2.3.4", "60000", "0", "master")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestIsOnlineWhenSingleMasterFailureIfErrorFlags(t *testing.T) {
	expErr := errors.New("Master flags are 'any-error,master', expected 'master'")
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "0", "any-error,master")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestGetAddressMasterSuccessfully(t *testing.T) {
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "0", "master")
	addr := st.state.MasterDbState.GetAddress()
	assert.Equal(t, "1.2.3.4:60000", addr)
}

func TestGetAddressMasterFailureNoIpPort(t *testing.T) {
	st := setupDbState()
	st.setMasterFields("master", "", "", "0", "master")
	addr := st.state.MasterDbState.GetAddress()
	assert.Equal(t, "", addr)
}

func TestIsOnlineWhenMasterAndTwoReplicasSuccessfully(t *testing.T) {
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	err := st.state.IsOnline()
	assert.Nil(t, err)
}

func TestIsOnlineWhenMasterAndTwoReplicasFailureIfErrorHasSet(t *testing.T) {
	testErr := errors.New("Some error")
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	st.setReplicaError(testErr)
	err := st.state.IsOnline()
	assert.Equal(t, testErr, err)
}

func TestIsOnlineWhenMasterAndTwoReplicasFailureIfNotSlaveRole(t *testing.T) {
	expErr := errors.New("Replica role is 'not-slave', expected 'slave'")
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("not-slave", "6.7.8.10", "3450", "ok", "slave")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestIsOnlineWhenMasterAndTwoReplicasFailureIfMasterLinkDown(t *testing.T) {
	expErr := errors.New("Replica link to the master is down")
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "nok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestIsOnlineWhenMasterAndTwoReplicasFailureIfErrorFlags(t *testing.T) {
	expErr := errors.New("Replica flags are 'any-error,slave', expected 'slave'")
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "any-error,slave")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestGetAddressReplicasSuccessfully(t *testing.T) {
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	addr := st.state.ReplicasDbState.States[0].GetAddress()
	assert.Equal(t, "6.7.8.9:1234", addr)
	addr = st.state.ReplicasDbState.States[1].GetAddress()
	assert.Equal(t, "6.7.8.10:3450", addr)
}

func TestGetAddressReplicasNoIpPort(t *testing.T) {
	st := setupDbState()
	st.setMasterFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "", "", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	addr := st.state.ReplicasDbState.States[0].GetAddress()
	assert.Equal(t, "", addr)
	addr = st.state.ReplicasDbState.States[1].GetAddress()
	assert.Equal(t, "6.7.8.10:3450", addr)
}
