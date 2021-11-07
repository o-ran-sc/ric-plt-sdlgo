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
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/cli"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/mocks"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/stretchr/testify/assert"
	"testing"
)

var hcMocks *healthCheckMocks

type healthCheckMocks struct {
	dbIface *mocks.MockDB
	dbErr   error
	dbState sdlgoredis.DbState
}

func setupHcMockMasterDb(ip, port string) {
	hcMocks = new(healthCheckMocks)
	hcMocks.dbState.MasterDbState.Fields.Role = "master"
	hcMocks.dbState.MasterDbState.Fields.Ip = ip
	hcMocks.dbState.MasterDbState.Fields.Port = port
	hcMocks.dbState.MasterDbState.Fields.Flags = "master"
}

func setupHcMockReplicaDb(ip, port string) {
	hcMocks = new(healthCheckMocks)
	hcMocks.dbState.ReplicasDbState = new(sdlgoredis.ReplicasDbState)
	hcMocks.dbState.ReplicasDbState.States = []*sdlgoredis.ReplicaDbState{
		&sdlgoredis.ReplicaDbState{
			Fields: sdlgoredis.ReplicaDbStateFields{
				Role: "slave",
			},
		},
	}
}

func setupHcMockSentinelDb(ip, port string) {
	hcMocks = new(healthCheckMocks)
	hcMocks.dbState.SentinelsDbState = new(sdlgoredis.SentinelsDbState)
	hcMocks.dbState.SentinelsDbState.States = []*sdlgoredis.SentinelDbState{
		&sdlgoredis.SentinelDbState{
			Fields: sdlgoredis.SentinelDbStateFields{
				Ip:   ip,
				Port: port,
			},
		},
	}
}

func addHcMockReplicaDbState(ip, port, masterLinkOk string) {
	if hcMocks.dbState.ReplicasDbState == nil {
		hcMocks.dbState.ReplicasDbState = new(sdlgoredis.ReplicasDbState)
	}
	hcMocks.dbState.ReplicasDbState.States = append(hcMocks.dbState.ReplicasDbState.States,
		&sdlgoredis.ReplicaDbState{
			Fields: sdlgoredis.ReplicaDbStateFields{
				Role:             "slave",
				Ip:               ip,
				Port:             port,
				MasterLinkStatus: masterLinkOk,
				Flags:            "slave",
			},
		},
	)
}

func addHcMockSentinelDbState(ip, port, flags string) {
	if hcMocks.dbState.SentinelsDbState == nil {
		hcMocks.dbState.SentinelsDbState = new(sdlgoredis.SentinelsDbState)
	}
	hcMocks.dbState.SentinelsDbState.States = append(hcMocks.dbState.SentinelsDbState.States,
		&sdlgoredis.SentinelDbState{
			Fields: sdlgoredis.SentinelDbStateFields{
				Ip:    ip,
				Port:  port,
				Flags: flags,
			},
		},
	)
}

func newMockDatabase() *cli.Database {
	db := &cli.Database{}
	hcMocks.dbIface = new(mocks.MockDB)
	hcMocks.dbIface.On("State").Return(&hcMocks.dbState, hcMocks.dbErr)
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
	setupHcMockMasterDb("10.20.30.40", "6379")
	addHcMockReplicaDbState("1.2.3.4", "6379", "ok")
	addHcMockReplicaDbState("5.6.7.8", "6379", "ok")
	addHcMockSentinelDbState("1.2.3.4", "26379", "sentinel")
	addHcMockSentinelDbState("5.6.7.8", "26379", "sentinel")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: OK")
	assert.Contains(t, stdout, "Master (10.20.30.40:6379): OK")
	assert.Contains(t, stdout, "Replica #1 (1.2.3.4:6379): OK")
	assert.Contains(t, stdout, "Replica #2 (5.6.7.8:6379): OK")

}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenOneReplicaStateNotUp(t *testing.T) {
	setupHcMockMasterDb("10.20.30.40", "6379")
	addHcMockReplicaDbState("1.2.3.4", "6379", "ok")
	addHcMockReplicaDbState("5.6.7.8", "6379", "nok")
	addHcMockSentinelDbState("1.2.3.4", "26379", "sentinel")
	addHcMockSentinelDbState("5.6.7.8", "26379", "sentinel")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: NOK")
	assert.Contains(t, stdout, "Replica #2 (5.6.7.8:6379): NOK")
	assert.Contains(t, stdout, "Replica link to the master is down")
}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenOneSentinelStateNotUp(t *testing.T) {
	setupHcMockMasterDb("10.20.30.40", "6379")
	addHcMockReplicaDbState("1.2.3.4", "6379", "ok")
	addHcMockReplicaDbState("5.6.7.8", "6379", "ok")
	addHcMockSentinelDbState("1.2.3.4", "26379", "some-failure")
	addHcMockSentinelDbState("5.6.7.8", "26379", "sentinel")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: NOK")
	assert.Contains(t, stdout, "Replica #1 (1.2.3.4:6379): OK")
	assert.Contains(t, stdout, "Replica #2 (5.6.7.8:6379): OK")
	assert.Contains(t, stdout, "Sentinel #1 (1.2.3.4:26379): NOK")
	assert.Contains(t, stdout, "Sentinel flags are 'some-failure', expected 'sentinel'")
}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenDbStateQueryFails(t *testing.T) {
	setupHcMockMasterDb("10.20.30.40", "6379")
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

func TestCliHealthCheckCanShowHaDeploymentOkStatusCorrectlyWhenDbStateIsFromReplicaOnly(t *testing.T) {
	setupHcMockReplicaDb("1.2.3.4", "6379")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: NOK")
	assert.Contains(t, stdout, "Master (): NOK")
}

func TestCliHealthCheckCanShowHaDeploymentOkStatusCorrectlyWhenDbStateIsFromSentinelOnly(t *testing.T) {
	setupHcMockSentinelDb("1.2.3.4", "26379")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: NOK")
	assert.Contains(t, stdout, "Master (): NOK")
}

func TestCliHealthCheckCanShowStandaloneDeploymentOkStatusCorrectly(t *testing.T) {
	setupHcMockMasterDb("10.20.30.40", "6379")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Contains(t, stdout, "Overall status: OK")
	assert.Contains(t, stdout, "Master (10.20.30.40:6379): OK")
}
