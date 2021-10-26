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

var hcMocks *healthCheckMocks

type healthCheckMocks struct {
	dbIface *mocks.MockDB
	dbErr   error
	dbInfo  internal.DbInfo
}

func setupHcMockMasterDb(confReplicas uint32) {
	hcMocks = new(healthCheckMocks)
	hcMocks.dbInfo.MasterRole = true
	hcMocks.dbInfo.ConfReplicasCnt = confReplicas
}

func setupHcMockReplicaDb() {
	hcMocks = new(healthCheckMocks)
	hcMocks.dbInfo.MasterRole = false
}

func addHcMockReplicaInfo(addr string, online bool) {
	hcMocks.dbInfo.Replicas = append(hcMocks.dbInfo.Replicas, internal.DbInfoReplica{
		Addr:   addr,
		Online: online,
	})
}

func newMockDatabase() *internal.Database {
	db := &internal.Database{}
	hcMocks.dbIface = new(mocks.MockDB)
	hcMocks.dbIface.On("Info").Return(&hcMocks.dbInfo, hcMocks.dbErr)
	db.Instances = append(db.Instances, hcMocks.dbIface)
	return db
}

func runHcCli() (string, error) {
	buf := new(bytes.Buffer)
	cmd := cli.NewHealthCheckCmdForTest(newMockDatabase)
	cmd.SetOut(buf)

	err := cmd.Execute()

	return buf.String(), err
}

func TestCliHealthCheckCanShowHelp(t *testing.T) {
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
		cmd.SetArgs([]string{test.args})

		err := cmd.Execute()

		stdout := buf.String()
		assert.Equal(t, test.expErr, err)
		assert.Contains(t, stdout, test.expOutput)
	}
}

func TestCliHealthCheckCanShowHaDeploymentOkStatusCorrectly(t *testing.T) {
	setupHcMockMasterDb(2)
	addHcMockReplicaInfo("1.2.3.4:6379", true)
	addHcMockReplicaInfo("5.6.7.8:6379", true)

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: OK")
	assert.Contains(t, stdout, "Connected replicas count 2: OK")
	assert.Contains(t, stdout, "Replica #1 (1.2.3.4:6379): OK")
	assert.Contains(t, stdout, "Replica #2 (5.6.7.8:6379): OK")

}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenOneReplicaMissing(t *testing.T) {
	setupHcMockMasterDb(2)
	addHcMockReplicaInfo("1.2.3.4:6379", true)

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: NOK")
	assert.Contains(t, stdout, "Connected replicas count 1: NOK\n      expected: 2")
}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenOneReplicaStateNotUp(t *testing.T) {
	setupHcMockMasterDb(2)
	addHcMockReplicaInfo("1.2.3.4:6379", true)
	addHcMockReplicaInfo("5.6.7.8:6379", false)

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: NOK")
	assert.Contains(t, stdout, "Replica #2 (5.6.7.8:6379) not online: NOK")
}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenDbInfoQueryFails(t *testing.T) {
	setupHcMockMasterDb(0)
	hcMocks.dbErr = errors.New("Some error")
	expCliErr := errors.New("SDL CLI error: Some error")

	buf := new(bytes.Buffer)
	cmd := cli.NewHealthCheckCmdForTest(newMockDatabase)
	cmd.SetErr(buf)

	err := cmd.Execute()
	stderr := buf.String()

	assert.Equal(t, expCliErr, err)
	assert.Contains(t, stderr, "Error: "+expCliErr.Error())
}

func TestCliHealthCheckCanShowHaDeploymentOkStatusCorrectlyWhenDbInfoIsFromReplicaServer(t *testing.T) {
	setupHcMockReplicaDb()

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: NOK")
	assert.Contains(t, stdout, "Connection to master: NOK")
}

func TestCliHealthCheckCanShowStandaloneDeploymentOkStatusCorrectly(t *testing.T) {
	setupHcMockMasterDb(0)

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: OK")
	assert.Contains(t, stdout, "Standalone DB: OK")
}
