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

package cli_test

import (
	"bytes"
	"errors"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/cli"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

var DbMock *mocks.MockDB

var heathCheckMockDbErr error
var heathCheckMockDbInfo *mDbInfo

type mDbInfo struct {
	info internal.DbInfo
}

func newMasterInfo(confReplicas uint32) *mDbInfo {
	dbInfo := new(mDbInfo)
	dbInfo.info.MasterRole = true
	dbInfo.info.ConfReplicasCnt = confReplicas
	return dbInfo
}

func (mdb *mDbInfo) addNewReplica(addr, state string) {
	mdb.info.Replicas = append(mdb.info.Replicas, internal.DbInfoReplica{
		Addr:  addr,
		State: state,
	})
}

func (mdb *mDbInfo) getInfo() *internal.DbInfo {
	return &mdb.info
}

func newReplicaInfo() *mDbInfo {
	dbInfo := new(mDbInfo)
	dbInfo.info.MasterRole = false
	return dbInfo
}

func newMockDatabase() *internal.Database {
	db := &internal.Database{}
	DbMock = new(mocks.MockDB)
	DbMock.On("Info").Return(heathCheckMockDbInfo.getInfo(), heathCheckMockDbErr)
	db.Instances = append(db.Instances, DbMock)
	return db
}

func TestCliHealthcheckCanShowHelp(t *testing.T) {
	var expOkErr error
	expHelp := "Usage:\n  " + "healthcheck [flags]"
	expNokErr := errors.New("unknown flag: --some-unknown-flag")
	tests := []struct {
		args      string
		expErr    error
		expOutput string
	}{
		{args: "-h", expErr: expOkErr, expOutput: expHelp},
		{args: "--help", expErr: expOkErr, expOutput: expHelp},
		{args: "--some-unknown-flag", expErr: expNokErr, expOutput: expHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewHealthCheckCmd()
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs([]string{test.args})
		err := cmd.Execute()
		result := buf.String()
		assert.Equal(t, test.expErr, err)
		assert.Contains(t, result, test.expOutput)
	}
}

func TestCliHealthcheckCanShowHaDeploymentOkStatusCorrectly(t *testing.T) {
	m := newMasterInfo(2)
	m.addNewReplica("1.2.3.4:6379", "online")
	m.addNewReplica("5.6.7.8:6379", "online")
	heathCheckMockDbInfo = m

	buf := new(bytes.Buffer)
	cmd := cli.NewHealthCheckCmdForTest(newMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()

	result := buf.String()
	assert.Nil(t, err)
	assert.Contains(t, result, "Overall status: OK")
}

func TestCliHealthcheckCanShowHaDeploymentStatusCorrectlyWhenOneReplicaMissing(t *testing.T) {
	m := newMasterInfo(2)
	m.addNewReplica("1.2.3.4:6379", "online")
	heathCheckMockDbInfo = m

	buf := new(bytes.Buffer)
	cmd := cli.NewHealthCheckCmdForTest(newMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()

	result := buf.String()
	assert.Nil(t, err)
	assert.Contains(t, result, "Overall status: NOK")
}

func TestCliHealthcheckCanShowHaDeploymentStatusCorrectlyWhenOneReplicaStateNotUp(t *testing.T) {
	m := newMasterInfo(2)
	m.addNewReplica("1.2.3.4:6379", "online")
	m.addNewReplica("5.6.7.8:6379", "wait_bgsave")
	heathCheckMockDbInfo = m

	buf := new(bytes.Buffer)
	cmd := cli.NewHealthCheckCmdForTest(newMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()

	result := buf.String()
	assert.Nil(t, err)
	assert.Contains(t, result, "Overall status: NOK")
}

func TestCliHealthcheckCanShowHaDeploymentStatusCorrectlyWhenDbInfoQueryFails(t *testing.T) {
	expCliErr := errors.New("SDL API error: Some error")
	heathCheckMockDbErr = errors.New("Some error")
	heathCheckMockDbInfo = newMasterInfo(0)

	buf := new(bytes.Buffer)
	cmd := cli.NewHealthCheckCmdForTest(newMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()

	result := buf.String()
	assert.Equal(t, expCliErr, err)
	assert.Contains(t, result, "Error: SDL API error: Some error")
}

func TestCliHealthcheckCanShowHaDeploymentOkStatusCorrectlyWhenDbInfoIsFromReplicaServer(t *testing.T) {
	m := newReplicaInfo()
	heathCheckMockDbInfo = m
	heathCheckMockDbErr = nil

	buf := new(bytes.Buffer)
	cmd := cli.NewHealthCheckCmdForTest(newMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()

	result := buf.String()
	assert.Nil(t, err)
	assert.Contains(t, result, "Overall status: NOK")
}
