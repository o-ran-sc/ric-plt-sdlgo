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

var mNs *nsMock

type nsMock struct {
	dbIface    *mocks.MockDB
	dbKeys     []string
	dbState    sdlgoredis.DbState
	dbKeysErr  error
	dbStateErr error
}

func setupNamespacesCliMock(keys []string, addr string, keysErr, stateErr error) {
	mNs = new(nsMock)
	mNs.dbKeys = keys
	mNs.dbState.PrimaryDbState.Fields.Role = "Primary"
	mNs.dbState.PrimaryDbState.Fields.Ip = addr
	if addr != "" {
		mNs.dbState.PrimaryDbState.Fields.Port = "6379"
	}
	mNs.dbKeysErr = keysErr
	mNs.dbStateErr = stateErr
}

func newNsMockDatabase() *cli.Database {
	db := &cli.Database{}
	mNs.dbIface = new(mocks.MockDB)
	mNs.dbIface.On("Keys", "*").Return(mNs.dbKeys, mNs.dbKeysErr)
	mNs.dbIface.On("State").Return(&mNs.dbState, mNs.dbStateErr).Maybe()
	db.Instances = append(db.Instances, mNs.dbIface)
	return db
}

func TestNamespacesCmdShowHelp(t *testing.T) {
	var expOkErr error
	expNokErrTooManyArgs := errors.New("accepts 0 arg(s), received 1")
	expHelp := "Usage:\n  namespaces [flags]"
	tests := []struct {
		args   []string
		expOut string
		expErr error
	}{
		{args: []string{"-h"}, expErr: expOkErr, expOut: expHelp},
		{args: []string{"--help"}, expErr: expOkErr, expOut: expHelp},
		{args: []string{"extra-arg"}, expErr: expNokErrTooManyArgs, expOut: expHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs(test.args)
		err := cmd.Execute()
		result := buf.String()

		assert.Equal(t, test.expErr, err)
		assert.Contains(t, result, test.expOut)
	}
}

func TestNamespacesCmdSuccess(t *testing.T) {
	expOut := "ns1\n" + "ns2\n" + "ns3\n"
	buf := new(bytes.Buffer)
	setupNamespacesCliMock([]string{
		"{ns1},key1",
		"{ns3},key1",
		"{ns2},key1",
		"{ns1},key2",
	}, "1.2.3.4", nil, nil)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()
	result := buf.String()

	assert.Nil(t, err)
	assert.Equal(t, expOut, result)
	mNs.dbIface.AssertExpectations(t)
}

func TestNamespacesCmdWithWideFlagSuccess(t *testing.T) {
	expOut :=
		"ADDRESS        NAMESPACE   KEYS   \n" +
			"1.2.3.4:6379   ns1         2      \n" +
			"1.2.3.4:6379   ns2         1      \n" +
			"1.2.3.4:6379   ns3         1      \n"
	buf := new(bytes.Buffer)
	setupNamespacesCliMock([]string{
		"{ns1},key1",
		"{ns3},key1",
		"{ns2},key1",
		"{ns1},key2",
	}, "1.2.3.4", nil, nil)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--wide"})

	err := cmd.Execute()
	result := buf.String()

	assert.Nil(t, err)
	assert.Equal(t, expOut, result)
	mNs.dbIface.AssertExpectations(t)
}

func TestNamespacesCmdNoKeysInDbSuccess(t *testing.T) {
	expOut := ""
	buf := new(bytes.Buffer)
	setupNamespacesCliMock([]string{}, "1.2.3.4", nil, nil)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()
	result := buf.String()

	assert.Nil(t, err)
	assert.Equal(t, expOut, result)
	mNs.dbIface.AssertExpectations(t)
}

func TestNamespacesCmdWithWideFlagNoKeysInDbSuccess(t *testing.T) {
	expOut := "ADDRESS   NAMESPACE   KEYS   \n"
	buf := new(bytes.Buffer)
	setupNamespacesCliMock([]string{}, "1.2.3.4", nil, nil)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"-w"})

	err := cmd.Execute()
	result := buf.String()

	assert.Nil(t, err)
	assert.Equal(t, expOut, result)
	mNs.dbIface.AssertExpectations(t)
}

func TestNamespacesCmdWithWideFlagStandaloneRedisAddressMissingSuccess(t *testing.T) {
	expOut :=
		"ADDRESS   NAMESPACE   KEYS   \n" +
			"          ns1         2      \n" +
			"          ns2         1      \n" +
			"          ns3         1      \n"
	buf := new(bytes.Buffer)
	setupNamespacesCliMock([]string{
		"{ns1},key1",
		"{ns3},key1",
		"{ns2},key1",
		"{ns1},key2",
	}, "", nil, nil)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--wide"})

	err := cmd.Execute()
	result := buf.String()

	assert.Nil(t, err)
	assert.Equal(t, expOut, result)
	mNs.dbIface.AssertExpectations(t)
}

func TestNamespacesCmdDbKeysFailure(t *testing.T) {
	expNokErr := errors.New("Some error")
	expOut := "Error: Some error"

	buf := new(bytes.Buffer)
	setupNamespacesCliMock(nil, "1.2.3.4", expNokErr, nil)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()
	result := buf.String()

	assert.Equal(t, expNokErr, err)
	assert.Contains(t, result, expOut)
	mNs.dbIface.AssertExpectations(t)
}

func TestNamespacesCmdDbStateFailure(t *testing.T) {
	expNokErr := errors.New("Some error")
	expOut := "Error: Some error"

	buf := new(bytes.Buffer)
	setupNamespacesCliMock(nil, "1.2.3.4", nil, expNokErr)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()
	result := buf.String()

	assert.Equal(t, expNokErr, err)
	assert.Contains(t, result, expOut)
	mNs.dbIface.AssertExpectations(t)
}

func TestNamespacesCmdNsStartMarkerFailure(t *testing.T) {
	expNokErr := errors.New("Namespace parsing error, no '{' in key string 'ns2},key1'")
	expOut := "Error: Namespace parsing error, no '{' in key string 'ns2},key1'"

	buf := new(bytes.Buffer)
	setupNamespacesCliMock([]string{
		"{ns1},key1",
		"ns2},key1",
		"{ns1},key2",
	}, "1.2.3.4", nil, nil)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()
	result := buf.String()

	assert.Equal(t, expNokErr, err)
	assert.Contains(t, result, expOut)
	mNs.dbIface.AssertExpectations(t)
}

func TestNamespacesCmdNsEndMarkerFailure(t *testing.T) {
	expNokErr := errors.New("Namespace parsing error, no '}' in key string '{ns2,key1'")
	expOut := "Error: Namespace parsing error, no '}' in key string '{ns2,key1'"

	buf := new(bytes.Buffer)
	setupNamespacesCliMock([]string{
		"{ns1},key1",
		"{ns2,key1",
		"{ns1},key2",
	}, "1.2.3.4", nil, nil)
	cmd := cli.NewNamespacesCmdForTest(newNsMockDatabase)
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	err := cmd.Execute()
	result := buf.String()

	assert.Equal(t, expNokErr, err)
	assert.Contains(t, result, expOut)
	mNs.dbIface.AssertExpectations(t)
}
