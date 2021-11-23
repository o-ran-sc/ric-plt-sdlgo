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
	"github.com/stretchr/testify/assert"
	"testing"
)

var getMocks *GetMocks

type GetMocks struct {
	sdlIface *mocks.MockSdlApi
	ns       string
	keys     []string
	result   map[string]interface{}
	ret      error
}

func setupGetMock(ns string, keys []string, result map[string]interface{}, ret error) {
	getMocks = new(GetMocks)
	getMocks.ns = ns
	getMocks.keys = keys
	getMocks.result = result
	getMocks.ret = ret
}

func newSdlGetApiMock() cli.ISyncStorage {
	getMocks.sdlIface = new(mocks.MockSdlApi)
	getMocks.sdlIface.On("Get", getMocks.ns, getMocks.keys).Return(getMocks.result, getMocks.ret)
	return getMocks.sdlIface
}

func runGetCmdCli() (string, string, error) {
	bufOut := new(bytes.Buffer)
	bufErr := new(bytes.Buffer)

	cmd := cli.NewGetCmdForTest(newSdlGetApiMock)
	cmd.SetOut(bufOut)
	cmd.SetErr(bufErr)
	args := []string{getMocks.ns}
	args = append(args, getMocks.keys...)
	cmd.SetArgs(args)
	err := cmd.Execute()

	return bufOut.String(), bufErr.String(), err
}

func TestGetCmdShowHelp(t *testing.T) {
	var expOkErr error
	expHelp := "Display one or many resources.\n\nPrints namespaces, keys or keys data in the given namespace."
	expHelpUsage := "Usage:\n  get <namespace> <key> [<key2> <key3>... <keyN>] [flags]"
	expArgsErr := errors.New("accepts command or arguments, received 0")
	expNokErr := errors.New("unknown flag: --ff")
	tests := []struct {
		args   []string
		expErr error
		expOut string
	}{
		{args: []string{"-h"}, expErr: expOkErr, expOut: expHelp},
		{args: []string{"--help"}, expErr: expOkErr, expOut: expHelp},
		{args: []string{}, expErr: expArgsErr, expOut: expHelpUsage},
		{args: []string{"--ff"}, expErr: expNokErr, expOut: expHelpUsage},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewGetCmdForTest(newSdlGetApiMock)
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs(test.args)
		err := cmd.Execute()
		result := buf.String()

		assert.Equal(t, test.expErr, err)
		assert.Contains(t, result, test.expOut)
	}
}

func TestGetCmdSuccess(t *testing.T) {

	tests := []struct {
		ns     string
		keys   []string
		result map[string]interface{}
		expOut string
		expErr error
	}{
		{ns: "testns", keys: []string{"key1"}, result: map[string]interface{}{}, expOut: "", expErr: nil},
		{ns: "testns", keys: []string{"key1"}, result: map[string]interface{}{"key1": "1"}, expOut: "key1:1\n", expErr: nil},
		{ns: "testns", keys: []string{"key1 key2"}, result: map[string]interface{}{"key1": "1", "key2": "2"}, expOut: "key1:1\nkey2:2\n", expErr: nil},
		{ns: "testns", keys: []string{"key1 key3 key2"}, result: map[string]interface{}{"key1": "1", "key3": "3", "key2": "2"}, expOut: "key1:1\nkey2:2\nkey3:3\n", expErr: nil},
		{ns: "testns", keys: []string{"key1 keyDoesNotExist"}, result: map[string]interface{}{"key1": "1"}, expOut: "key1:1\n", expErr: nil},
	}
	for _, test := range tests {
		setupGetMock(test.ns, test.keys, test.result, test.expErr)
		stdout, stderr, err := runGetCmdCli()

		assert.Nil(t, err)
		assert.Equal(t, test.expOut, stdout)
		assert.Equal(t, "", stderr)
		getMocks.sdlIface.AssertExpectations(t)
	}
}

func TestGetCmdFails(t *testing.T) {
	expErr := errors.New("Boom!")

	setupGetMock("testns", []string{"key1"}, map[string]interface{}{}, expErr)
	_, stderr, err := runGetCmdCli()

	assert.Equal(t, expErr, err)
	assert.Contains(t, stderr, expErr.Error())
	getMocks.sdlIface.AssertExpectations(t)
}
