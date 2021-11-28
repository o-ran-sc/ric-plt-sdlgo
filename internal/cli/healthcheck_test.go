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

func setupHcMockPrimaryDb(ip, port string, nodes int) {
	hcMocks = new(healthCheckMocks)
	hcMocks.dbState.ConfigNodeCnt = nodes
	hcMocks.dbState.PrimaryDbState.Fields.Role = "master"
	hcMocks.dbState.PrimaryDbState.Fields.Ip = ip
	hcMocks.dbState.PrimaryDbState.Fields.Port = port
	hcMocks.dbState.PrimaryDbState.Fields.Flags = "master"
	hcMocks.dbState.ReplicasDbState = new(sdlgoredis.ReplicasDbState)
	hcMocks.dbState.ReplicasDbState.States = []*sdlgoredis.ReplicaDbState{}
	hcMocks.dbState.SentinelsDbState = new(sdlgoredis.SentinelsDbState)
	hcMocks.dbState.SentinelsDbState.States = []*sdlgoredis.SentinelDbState{}
}

func setupHcMockReplicaDb(nodes int) {
	hcMocks = new(healthCheckMocks)
	hcMocks.dbState.ConfigNodeCnt = nodes
	hcMocks.dbState.ReplicasDbState = new(sdlgoredis.ReplicasDbState)
	hcMocks.dbState.ReplicasDbState.States = []*sdlgoredis.ReplicaDbState{}
	hcMocks.dbState.SentinelsDbState = new(sdlgoredis.SentinelsDbState)
	hcMocks.dbState.SentinelsDbState.States = []*sdlgoredis.SentinelDbState{}
}

func setupHcMockSentinelDb(ip, port string, nodes int) {
	hcMocks = new(healthCheckMocks)
	hcMocks.dbState.ConfigNodeCnt = nodes
	hcMocks.dbState.SentinelsDbState = new(sdlgoredis.SentinelsDbState)
	hcMocks.dbState.SentinelsDbState.States = []*sdlgoredis.SentinelDbState{}
}

func addHcMockReplicaDbState(ip, port, primaryLinkOk string) {
	if hcMocks.dbState.ReplicasDbState == nil {
		hcMocks.dbState.ReplicasDbState = new(sdlgoredis.ReplicasDbState)
	}
	hcMocks.dbState.ReplicasDbState.States = append(hcMocks.dbState.ReplicasDbState.States,
		&sdlgoredis.ReplicaDbState{
			Fields: sdlgoredis.ReplicaDbStateFields{
				Role:              "slave",
				Ip:                ip,
				Port:              port,
				PrimaryLinkStatus: primaryLinkOk,
				Flags:             "slave",
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
	cmd := cli.NewHealthCheckCmd(newMockDatabase)
	cmd.SetOut(buf)

	err := cmd.Execute()

	return buf.String(), err
}

func TestCliHealthCheckCanShowHelp(t *testing.T) {
	var expOkErr error
	expHelp := "Usage:\n  " + "healthcheck [flags]"
	expNokErr := errors.New("unknown flag: --some-unknown-flag")
	expArgCntErr := errors.New("accepts 0 arg(s), received 1")
	tests := []struct {
		args      string
		expErr    error
		expOutput string
	}{
		{args: "-h", expErr: expOkErr, expOutput: expHelp},
		{args: "--help", expErr: expOkErr, expOutput: expHelp},
		{args: "--some-unknown-flag", expErr: expNokErr, expOutput: expHelp},
		{args: "some-extra-argument", expErr: expArgCntErr, expOutput: expHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewHealthCheckCmd(newMockDatabase)
		cmd.SetOut(buf)
		cmd.SetArgs([]string{test.args})

		err := cmd.Execute()

		stdout := buf.String()
		assert.Equal(t, test.expErr, err)
		assert.Contains(t, stdout, test.expOutput)
	}
}

func TestCliHealthCheckCanShowHaDeploymentOkStatusCorrectly(t *testing.T) {
	expOut :=
		"Overall status: OK\n\n" +
			"CLUSTER   ROLE      ADDRESS            STATUS   ERROR    \n" +
			"0         primary   10.20.30.40:6379   OK       <none>   \n" +
			"0         replica   1.2.3.4:6379       OK       <none>   \n" +
			"0         replica   5.6.7.8:6379       OK       <none>   \n"
	setupHcMockPrimaryDb("10.20.30.40", "6379", 3)
	addHcMockReplicaDbState("1.2.3.4", "6379", "ok")
	addHcMockReplicaDbState("5.6.7.8", "6379", "ok")
	addHcMockSentinelDbState("1.2.3.4", "26379", "sentinel")
	addHcMockSentinelDbState("5.6.7.8", "26379", "sentinel")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenOneReplicaStateNotUp(t *testing.T) {
	expOut :=
		"Overall status: NOK\n\n" +
			"CLUSTER   ROLE      ADDRESS            STATUS   ERROR                                 \n" +
			"0         primary   10.20.30.40:6379   OK       <none>                                \n" +
			"0         replica   1.2.3.4:6379       OK       <none>                                \n" +
			"0         replica   5.6.7.8:6379       NOK      Replica link to the primary is down   \n"
	setupHcMockPrimaryDb("10.20.30.40", "6379", 3)
	addHcMockReplicaDbState("1.2.3.4", "6379", "ok")
	addHcMockReplicaDbState("5.6.7.8", "6379", "nok")
	addHcMockSentinelDbState("1.2.3.4", "26379", "sentinel")
	addHcMockSentinelDbState("5.6.7.8", "26379", "sentinel")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenOneSentinelStateNotUp(t *testing.T) {
	expOut :=
		"Overall status: NOK\n\n" +
			"CLUSTER   ROLE       ADDRESS            STATUS   ERROR                                                    \n" +
			"0         primary    10.20.30.40:6379   OK       <none>                                                   \n" +
			"0         replica    1.2.3.4:6379       OK       <none>                                                   \n" +
			"0         replica    5.6.7.8:6379       OK       <none>                                                   \n" +
			"0         sentinel   1.2.3.4:26379      NOK      Sentinel flags are 'some-failure', expected 'sentinel'   \n"
	setupHcMockPrimaryDb("10.20.30.40", "6379", 3)
	addHcMockReplicaDbState("1.2.3.4", "6379", "ok")
	addHcMockReplicaDbState("5.6.7.8", "6379", "ok")
	addHcMockSentinelDbState("1.2.3.4", "26379", "some-failure")
	addHcMockSentinelDbState("5.6.7.8", "26379", "sentinel")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenNoReplicas(t *testing.T) {
	expOut :=
		"Overall status: NOK\n\n" +
			"CLUSTER   ROLE      ADDRESS            STATUS   ERROR                                                        \n" +
			"0         primary   10.20.30.40:6379   OK       <none>                                                       \n" +
			"0         replica   <none>             NOK      Configured DBAAS nodes 3 but only 1 primary and 0 replicas   \n"
	setupHcMockPrimaryDb("10.20.30.40", "6379", 3)
	addHcMockSentinelDbState("1.2.3.4", "26379", "sentinel")
	addHcMockSentinelDbState("5.6.7.8", "26379", "sentinel")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliHealthCheckCanShowHaDeploymentStatusCorrectlyWhenDbStateQueryFails(t *testing.T) {
	setupHcMockPrimaryDb("10.20.30.40", "6379", 3)
	hcMocks.dbErr = errors.New("Some error")

	buf := new(bytes.Buffer)
	cmd := cli.NewHealthCheckCmd(newMockDatabase)
	cmd.SetErr(buf)

	err := cmd.Execute()
	stderr := buf.String()

	assert.Equal(t, hcMocks.dbErr, err)
	assert.Contains(t, stderr, "Error: Some error")
}

func TestCliHealthCheckCanShowHaDeploymentOkStatusCorrectlyWhenDbStateIsFromReplicaOnly(t *testing.T) {
	expOut :=
		"Overall status: NOK\n\n" +
			"CLUSTER   ROLE      ADDRESS        STATUS   ERROR                                 \n" +
			"0         primary   <none>         NOK      No primary DB, current role ''        \n" +
			"0         replica   1.2.3.4:6379   NOK      Replica link to the primary is down   \n" +
			"0         replica   5.6.7.8:6379   NOK      Replica link to the primary is down   \n"
	setupHcMockReplicaDb(3)
	addHcMockReplicaDbState("1.2.3.4", "6379", "nok")
	addHcMockReplicaDbState("5.6.7.8", "6379", "nok")

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliHealthCheckCanShowHaDeploymentOkStatusCorrectlyWhenDbStateIsFromSentinelOnly(t *testing.T) {
	expOut :=
		"Overall status: NOK\n\n" +
			"CLUSTER   ROLE      ADDRESS   STATUS   ERROR                            \n" +
			"0         primary   <none>    NOK      No primary DB, current role ''   \n"
	setupHcMockSentinelDb("1.2.3.4", "26379", 3)

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliHealthCheckCanShowStandaloneDeploymentOkStatusCorrectly(t *testing.T) {
	expOut :=
		"Overall status: OK\n\n" +
			"CLUSTER   ROLE      ADDRESS            STATUS   ERROR    \n" +
			"0         primary   10.20.30.40:6379   OK       <none>   \n"
	setupHcMockPrimaryDb("10.20.30.40", "6379", 1)

	stdout, err := runHcCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}
