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

var removeMocks *RemoveMocks

type RemoveMocks struct {
	sdlIface *mocks.MockSdlApi
	ns       string
	keys     []string
	ret      error
}

func setupRemoveCliMock(ns string, keys []string, ret error) {
	removeMocks = new(RemoveMocks)
	removeMocks.ns = ns
	removeMocks.keys = keys
	removeMocks.ret = ret
}

func newMockSdlRemoveApi() cli.ISyncStorage {
	removeMocks.sdlIface = new(mocks.MockSdlApi)
	removeMocks.sdlIface.On("Remove", removeMocks.ns, removeMocks.keys).Return(removeMocks.ret)
	return removeMocks.sdlIface
}

func runRemoveCli() (string, string, error) {
	bufStdout := new(bytes.Buffer)
	bufStderr := new(bytes.Buffer)
	cmd := cli.NewRemoveCmdForTest(newMockSdlRemoveApi)
	cmd.SetOut(bufStdout)
	cmd.SetErr(bufStderr)
	args := []string{removeMocks.ns}
	args = append(args, removeMocks.keys...)
	cmd.SetArgs(args)
	err := cmd.Execute()

	return bufStdout.String(), bufStderr.String(), err
}

func TestCliRemoveCanShowHelp(t *testing.T) {
	var expOkErr error
	expHelp := "Usage:\n  " + "remove <namespace> <key> [<key2>... <keyN>] [flags]"
	expFlagErr := fmt.Errorf("unknown flag: --some-unknown-flag")
	expArgCntLtErr := fmt.Errorf("requires at least 2 arg(s), only received 1")
	tests := []struct {
		args      []string
		expErr    error
		expOutput string
	}{
		{args: []string{"-h"}, expErr: expOkErr, expOutput: expHelp},
		{args: []string{"--help"}, expErr: expOkErr, expOutput: expHelp},
		{args: []string{"--some-unknown-flag"}, expErr: expFlagErr, expOutput: expHelp},
		{args: []string{"some-ns"}, expErr: expArgCntLtErr, expOutput: expHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewRemoveCmdForTest(newMockSdlRemoveApi)
		cmd.SetOut(buf)
		cmd.SetArgs(test.args)

		err := cmd.Execute()

		stdout := buf.String()
		assert.Equal(t, test.expErr, err)
		assert.Contains(t, stdout, test.expOutput)
	}
}

func TestCliRemoveCommandWithOneKeySuccess(t *testing.T) {
	setupRemoveCliMock("some-ns", []string{"some-key"}, nil)

	stdout, stderr, err := runRemoveCli()

	assert.Nil(t, err)
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
	removeMocks.sdlIface.AssertExpectations(t)
}

func TestCliRemoveCommandWithMultipleKeysSuccess(t *testing.T) {
	setupRemoveCliMock("some-ns", []string{"some-key-1", "some-key-1", "some-key-3"}, nil)

	stdout, stderr, err := runRemoveCli()

	assert.Nil(t, err)
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
	removeMocks.sdlIface.AssertExpectations(t)
}

func TestCliRemoveCommandFailure(t *testing.T) {
	expErr := fmt.Errorf("some-error")
	setupRemoveCliMock("some-ns", []string{"some-key"}, expErr)

	_, stderr, err := runRemoveCli()

	assert.Equal(t, expErr, err)
	assert.Contains(t, stderr, expErr.Error())
	removeMocks.sdlIface.AssertExpectations(t)
}
