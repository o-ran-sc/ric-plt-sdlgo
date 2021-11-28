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

func (ds *dbStateMock) setError(err error) {
	ds.state.Err = err
}

func (ds *dbStateMock) setPrimaryError(err error) {
	ds.state.PrimaryDbState.Err = err
}

func (ds *dbStateMock) setPrimaryFields(role, ip, port, rCnt, flags string) {
	ds.state.PrimaryDbState.Fields.Role = role
	ds.state.PrimaryDbState.Fields.Ip = ip
	ds.state.PrimaryDbState.Fields.Port = port
	ds.state.PrimaryDbState.Fields.Flags = flags
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
	newState.Fields.PrimaryLinkStatus = mls
	newState.Fields.Flags = flags
	ds.state.ReplicasDbState.States = append(ds.state.ReplicasDbState.States, newState)
}

func (ds *dbStateMock) setSentinelError(err error) {
	if ds.state.SentinelsDbState == nil {
		ds.state.SentinelsDbState = new(sdlgoredis.SentinelsDbState)
	}
	ds.state.SentinelsDbState.Err = err
}

func (ds *dbStateMock) addSentinelFields(ip, port, flags string) {
	if ds.state.SentinelsDbState == nil {
		ds.state.SentinelsDbState = new(sdlgoredis.SentinelsDbState)
	}
	newState := new(sdlgoredis.SentinelDbState)
	newState.Fields.Ip = ip
	newState.Fields.Port = port
	newState.Fields.Flags = flags
	ds.state.SentinelsDbState.States = append(ds.state.SentinelsDbState.States, newState)
}

func TestIsOnlineFailureIfErrorHasSet(t *testing.T) {
	testErr := errors.New("Some error")
	st := setupDbState()
	st.setError(testErr)
	err := st.state.IsOnline()
	assert.Equal(t, testErr, err)
}

func TestIsOnlineWhenSinglePrimarySuccessfully(t *testing.T) {
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "0", "master")
	err := st.state.IsOnline()
	assert.Nil(t, err)
}

func TestIsOnlineWhenSinglePrimaryFailureIfErrorHasSet(t *testing.T) {
	testErr := errors.New("Some error")
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "0", "master")
	st.setPrimaryError(testErr)
	err := st.state.IsOnline()
	assert.Equal(t, testErr, err)
}

func TestIsOnlineWhenSinglePrimaryFailureIfNotPrimaryRole(t *testing.T) {
	expErr := errors.New("No primary DB, current role 'not-master'")
	st := setupDbState()
	st.setPrimaryFields("not-master", "1.2.3.4", "60000", "0", "master")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestIsOnlineWhenSinglePrimaryFailureIfErrorFlags(t *testing.T) {
	expErr := errors.New("Primary flags are 'any-error,master', expected 'master'")
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "0", "any-error,master")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestGetAddressPrimarySuccessfully(t *testing.T) {
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "0", "master")
	addr := st.state.PrimaryDbState.GetAddress()
	assert.Equal(t, "1.2.3.4:60000", addr)
}

func TestGetAddressPrimaryFailureNoIpPort(t *testing.T) {
	st := setupDbState()
	st.setPrimaryFields("master", "", "", "0", "master")
	addr := st.state.PrimaryDbState.GetAddress()
	assert.Equal(t, "", addr)
}

func TestIsOnlineWhenPrimaryAndTwoReplicasSuccessfully(t *testing.T) {
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	err := st.state.IsOnline()
	assert.Nil(t, err)
}

func TestIsOnlineWhenPrimaryAndTwoReplicasFailureIfErrorHasSet(t *testing.T) {
	testErr := errors.New("Some error")
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	st.setReplicaError(testErr)
	err := st.state.IsOnline()
	assert.Equal(t, testErr, err)
}

func TestIsOnlineWhenPrimaryAndOneReplicaFailureIfSentinelErrorHasSet(t *testing.T) {
	testErr := errors.New("Some error")
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.setSentinelError(testErr)
	err := st.state.IsOnline()
	assert.Equal(t, testErr, err)
}

func TestIsOnlineWhenPrimaryAndTwoReplicasFailureIfNotReplicaRole(t *testing.T) {
	expErr := errors.New("Replica role is 'not-slave', expected 'slave'")
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("not-slave", "6.7.8.10", "3450", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestIsOnlineWhenPrimaryAndTwoReplicasFailureIfPrimaryLinkDown(t *testing.T) {
	expErr := errors.New("Replica link to the primary is down")
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "nok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestIsOnlineWhenPrimaryAndTwoReplicasFailureIfErrorFlags(t *testing.T) {
	expErr := errors.New("Replica flags are 'any-error,slave', expected 'slave'")
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "any-error,slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestIsOnlineWhenPrimaryAndOneReplicaFailureIfSentinelErrorFlags(t *testing.T) {
	expErr := errors.New("Sentinel flags are 'any-error,sentinel', expected 'sentinel'")
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "112345", "any-error,sentinel")
	err := st.state.IsOnline()
	assert.Equal(t, expErr, err)
}

func TestGetAddressReplicasSuccessfully(t *testing.T) {
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	addr := st.state.ReplicasDbState.States[0].GetAddress()
	assert.Equal(t, "6.7.8.9:1234", addr)
	addr = st.state.ReplicasDbState.States[1].GetAddress()
	assert.Equal(t, "6.7.8.10:3450", addr)
}

func TestGetAddressReplicasNoIpPort(t *testing.T) {
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "", "", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	addr := st.state.ReplicasDbState.States[0].GetAddress()
	assert.Equal(t, "", addr)
	addr = st.state.ReplicasDbState.States[1].GetAddress()
	assert.Equal(t, "6.7.8.10:3450", addr)
}

func TestGetAddressSentinelsSuccessfully(t *testing.T) {
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "6.7.8.9", "1234", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	st.addSentinelFields("6.7.8.9", "11234", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	addr := st.state.SentinelsDbState.States[0].GetAddress()
	assert.Equal(t, "6.7.8.9:11234", addr)
	addr = st.state.SentinelsDbState.States[1].GetAddress()
	assert.Equal(t, "6.7.8.10:13450", addr)
}

func TestGetAddressSentinelsNoIpPort(t *testing.T) {
	st := setupDbState()
	st.setPrimaryFields("master", "1.2.3.4", "60000", "2", "master")
	st.addReplicaFields("slave", "", "", "ok", "slave")
	st.addReplicaFields("slave", "6.7.8.10", "3450", "ok", "slave")
	st.addSentinelFields("", "", "sentinel")
	st.addSentinelFields("6.7.8.10", "13450", "sentinel")
	addr := st.state.SentinelsDbState.States[0].GetAddress()
	assert.Equal(t, "", addr)
	addr = st.state.SentinelsDbState.States[1].GetAddress()
	assert.Equal(t, "6.7.8.10:13450", addr)
}
