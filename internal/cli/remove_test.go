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
	sdlIface  *mocks.MockSdlApi
	ns        string
	kps       []string
	keys      []string
	retList   error
	retRemove error
}

func setupRemoveCliMock(ns string, keyPattern, keys []string, retList, retRemove error) {
	removeMocks = new(RemoveMocks)
	removeMocks.ns = ns
	removeMocks.kps = keyPattern
	removeMocks.keys = keys
	removeMocks.retList = retList
	removeMocks.retRemove = retRemove
}

func newMockSdlRemoveApi() cli.ISyncStorage {
	removeMocks.sdlIface = new(mocks.MockSdlApi)
	if len(removeMocks.kps) == 0 {
		removeMocks.kps = append(removeMocks.kps, "*")
	}
	for _, kp := range removeMocks.kps {
		removeMocks.sdlIface.On("ListKeys", removeMocks.ns, kp).Return(removeMocks.keys, removeMocks.retList)
	}
	removeMocks.sdlIface.On("Remove", removeMocks.ns, removeMocks.keys).Return(removeMocks.retRemove).Maybe()
	return removeMocks.sdlIface
}

func runRemoveCli() (string, string, error) {
	bufStdout := new(bytes.Buffer)
	bufStderr := new(bytes.Buffer)
	cmd := cli.NewRemoveCmdForTest(newMockSdlRemoveApi)
	cmd.SetOut(bufStdout)
	cmd.SetErr(bufStderr)
	args := []string{removeMocks.ns}
	args = append(args, removeMocks.kps...)
	cmd.SetArgs(args)
	err := cmd.Execute()

	return bufStdout.String(), bufStderr.String(), err
}

func TestCliRemoveCanShowHelp(t *testing.T) {
	var expOkErr error
	expHelp := "remove <namespace> [<key|pattern>... <keyN|patternN>] [flags]"
	expFlagErr := fmt.Errorf("unknown flag: --some-unknown-flag")
	expArgCntLtErr := fmt.Errorf("requires at least 1 arg(s), only received 0")
	tests := []struct {
		args      []string
		expErr    error
		expOutput string
	}{
		{args: []string{"-h"}, expErr: expOkErr, expOutput: expHelp},
		{args: []string{"--help"}, expErr: expOkErr, expOutput: expHelp},
		{args: []string{"--some-unknown-flag"}, expErr: expFlagErr, expOutput: expHelp},
		{args: nil, expErr: expArgCntLtErr, expOutput: expHelp},
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
	setupRemoveCliMock("some-ns", []string{"some-key"}, []string{"some-key"}, nil, nil)

	stdout, stderr, err := runRemoveCli()

	assert.Nil(t, err)
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
	removeMocks.sdlIface.AssertExpectations(t)
}

func TestCliRemoveCommandWithOneKeyPatternSuccess(t *testing.T) {
	setupRemoveCliMock("some-ns", []string{"some-key*"}, []string{"some-key-1", "some-key-1", "some-key-3"}, nil, nil)

	stdout, stderr, err := runRemoveCli()

	assert.Nil(t, err)
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
	removeMocks.sdlIface.AssertExpectations(t)
}

func TestCliRemoveCommandWithMultipleKeyPatternsSuccess(t *testing.T) {
	setupRemoveCliMock("some-ns", []string{"some-key*", "other-key*"}, []string{"other-key2"}, nil, nil)

	stdout, stderr, err := runRemoveCli()

	assert.Nil(t, err)
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
	removeMocks.sdlIface.AssertExpectations(t)
}

func TestCliRemoveCommandWithOutKeyOrPatternSuccess(t *testing.T) {
	setupRemoveCliMock("some-ns", []string{}, []string{"some-key-1", "some-key-1", "some-key-3"}, nil, nil)

	stdout, stderr, err := runRemoveCli()

	assert.Nil(t, err)
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
	removeMocks.sdlIface.AssertExpectations(t)
}

func TestCliRemoveCommandErrorInSdlApiListKeysFailure(t *testing.T) {
	expErr := fmt.Errorf("some-error")
	setupRemoveCliMock("some-ns", []string{"*"}, []string{"some-key"}, expErr, nil)

	_, stderr, err := runRemoveCli()

	assert.Equal(t, expErr, err)
	assert.Contains(t, stderr, expErr.Error())
	removeMocks.sdlIface.AssertExpectations(t)
}

func TestCliRemoveCommandErrorInSdlApiRemoveFailure(t *testing.T) {
	expErr := fmt.Errorf("some-error")
	setupRemoveCliMock("some-ns", []string{"*"}, []string{"some-key"}, nil, expErr)

	_, stderr, err := runRemoveCli()

	assert.Equal(t, expErr, err)
	assert.Contains(t, stderr, expErr.Error())
	removeMocks.sdlIface.AssertExpectations(t)
}
