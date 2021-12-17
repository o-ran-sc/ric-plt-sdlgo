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

var mKeys *keysMock

type keysMock struct {
	sdlIface *mocks.MockSdlApi
	ns       string
	pattern  string
	keys     []string
	err      error
}

func setupKeysCliMock(ns, pattern string, keys []string, err error) {
	mKeys = new(keysMock)
	mKeys.ns = ns
	mKeys.pattern = pattern
	mKeys.keys = keys
	mKeys.err = err
}

func newMockSdlApi() cli.ISyncStorage {
	mKeys.sdlIface = new(mocks.MockSdlApi)
	mKeys.sdlIface.On("ListKeys", mKeys.ns, mKeys.pattern).Return(mKeys.keys, mKeys.err)
	return mKeys.sdlIface
}

func TestKeysCmdShowHelp(t *testing.T) {
	var expOkErr error
	expNokErrZeroArgs := errors.New("accepts between 1 and 2 arg(s), received 0")
	expNokErrThreeArgs := errors.New("accepts between 1 and 2 arg(s), received 3")
	expHelp := "Usage:\n  keys <namespace> [pattern|default '*'] [flags]"
	tests := []struct {
		args   []string
		expOut string
		expErr error
	}{
		{args: []string{"-h"}, expErr: expOkErr, expOut: expHelp},
		{args: []string{"--help"}, expErr: expOkErr, expOut: expHelp},
		{args: []string{}, expErr: expNokErrZeroArgs, expOut: expHelp},
		{args: []string{"ns", "key", "extra-arg"}, expErr: expNokErrThreeArgs, expOut: expHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewKeysCmdForTest(newMockSdlApi)
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs(test.args)
		err := cmd.Execute()
		result := buf.String()

		assert.Equal(t, test.expErr, err)
		assert.Contains(t, result, test.expOut)
	}
}

func TestKeysCmdSuccess(t *testing.T) {
	var expOkErr error
	argsNsPattern := []string{"testns", "*"}
	argsNs := []string{"testns"}
	tests := []struct {
		args        []string
		outListKeys []string
		expOut      string
		expErr      error
	}{
		{args: argsNs, outListKeys: []string{"key13"}, expOut: "key13", expErr: expOkErr},
		{args: argsNsPattern, outListKeys: []string{"key1"}, expOut: "key1", expErr: expOkErr},
		{args: argsNsPattern, outListKeys: []string{"key1", "key2"}, expOut: "key1\nkey2", expErr: expOkErr},
		{args: argsNsPattern, outListKeys: []string{"key2", "key3", "key1"}, expOut: "key1\nkey2\nkey3", expErr: expOkErr},
		{args: argsNsPattern, outListKeys: []string{}, expOut: "", expErr: expOkErr},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		if len(test.args) > 1 {
			setupKeysCliMock(test.args[0], test.args[1], test.outListKeys, test.expErr)
		} else {
			setupKeysCliMock(test.args[0], "*", test.outListKeys, test.expErr)
		}
		cmd := cli.NewKeysCmdForTest(newMockSdlApi)
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs(test.args)
		err := cmd.Execute()
		result := buf.String()

		assert.Equal(t, test.expErr, err)
		assert.Contains(t, result, test.expOut)
	}
}

func TestKeysCmdFails(t *testing.T) {
	argsNs := []string{"testns"}
	expNokErr := errors.New("Boom!")
	expNokOut := "Usage:\n  keys <namespace> [pattern|default '*'] [flags]"

	buf := new(bytes.Buffer)
	setupKeysCliMock("testns", "*", []string{"very wrong"}, expNokErr)
	cmd := cli.NewKeysCmdForTest(newMockSdlApi)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs(argsNs)
	err := cmd.Execute()
	result := buf.String()

	assert.Equal(t, expNokErr, err)
	assert.Contains(t, result, expNokOut)
}

func TestKeysCmdInvalidNamespaceArgument(t *testing.T) {
	expNokErrNsAsterisk1 := errors.New("Invalid character (*) in given * namespace argument.")
	expNokErrNsAsterisk2 := errors.New("Invalid character (*) in given foo* namespace argument.")
	expHelp := "Usage:\n  keys <namespace> [pattern|default '*'] [flags]"
	tests := []struct {
		args   []string
		expOut string
		expErr error
	}{
		{args: []string{"*"}, expErr: expNokErrNsAsterisk1, expOut: expHelp},
		{args: []string{"foo*"}, expErr: expNokErrNsAsterisk2, expOut: expHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewKeysCmdForTest(newMockSdlApi)
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs(test.args)
		err := cmd.Execute()
		result := buf.String()

		assert.Equal(t, test.expErr, err)
		assert.Contains(t, result, test.expOut)
	}
}
