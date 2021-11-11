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
	"fmt"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/cli"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

var setMocks *SetMocks

type SetMocks struct {
	sdlIface *mocks.MockSdlApi
	ns       string
	key      string
	value    string
	ret      error
}

func setupSetCliMock(ns, key, value string, ret error) {
	setMocks = new(SetMocks)
	setMocks.ns = ns
	setMocks.key = key
	setMocks.value = value
	setMocks.ret = ret
}

func newMockSdlSetApi() cli.ISyncStorage {
	setMocks.sdlIface = new(mocks.MockSdlApi)
	setMocks.sdlIface.On("Set", setMocks.ns, []interface{}{setMocks.key, setMocks.value}).Return(setMocks.ret)
	return setMocks.sdlIface
}

func runSetCli() (string, string, error) {
	bufStdout := new(bytes.Buffer)
	bufStderr := new(bytes.Buffer)
	cmd := cli.NewSetCmdForTest(newMockSdlSetApi)
	cmd.SetOut(bufStdout)
	cmd.SetErr(bufStderr)
	cmd.SetArgs([]string{setMocks.ns, setMocks.key, setMocks.value})
	err := cmd.Execute()

	return bufStdout.String(), bufStderr.String(), err
}

func TestCliSetCanShowHelp(t *testing.T) {
	var expOkErr error
	expHelp := "Usage:\n  " + "set <namespace> <key> <value> [flags]"
	expFlagErr := fmt.Errorf("unknown flag: --some-unknown-flag")
	expArgCntLtErr := fmt.Errorf("accepts 3 arg(s), received 2")
	expArgCntGtErr := fmt.Errorf("accepts 3 arg(s), received 4")
	tests := []struct {
		args      []string
		expErr    error
		expOutput string
	}{
		{args: []string{"-h"}, expErr: expOkErr, expOutput: expHelp},
		{args: []string{"--help"}, expErr: expOkErr, expOutput: expHelp},
		{args: []string{"--some-unknown-flag"}, expErr: expFlagErr, expOutput: expHelp},
		{args: []string{"some-ns", "some-key"}, expErr: expArgCntLtErr, expOutput: expHelp},
		{args: []string{"some-ns", "some-key", "some-value", "some-extra"}, expErr: expArgCntGtErr, expOutput: expHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewSetCmdForTest(newMockSdlSetApi)
		cmd.SetOut(buf)
		cmd.SetArgs(test.args)

		err := cmd.Execute()

		stdout := buf.String()
		assert.Equal(t, test.expErr, err)
		assert.Contains(t, stdout, test.expOutput)
	}
}

func TestCliSetCommandSuccess(t *testing.T) {
	setupSetCliMock("some-ns", "some-key", "some-value", nil)

	stdout, stderr, err := runSetCli()

	assert.Nil(t, err)
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
	setMocks.sdlIface.AssertExpectations(t)
}

func TestCliSetCommandFailure(t *testing.T) {
	expErr := fmt.Errorf("some-error")
	setupSetCliMock("some-ns", "some-key", "some-value", expErr)

	_, stderr, err := runSetCli()

	assert.Equal(t, expErr, err)
	assert.Contains(t, stderr, expErr.Error())
	setMocks.sdlIface.AssertExpectations(t)
}
