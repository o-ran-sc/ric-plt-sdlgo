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
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/cli"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

var sMocks *setMocks

type setMocks struct {
	sdlIface *mocks.MockSdlApi
	ns string
	key string
	value string
	ret error
}

func setupSetCliMock(ns, key, value string, ret error) {
	sMocks = new(setMocks)
	sMocks.ns = ns
	sMocks.key = key
	sMocks.value = value
	sMocks.ret = ret
}

func newMockSdlApi() cli.ISyncStorage {
	sMocks.sdlIface = new(mocks.MockSdlApi)
	sMocks.sdlIface.On("Set", sMocks.ns, []interface {}{sMocks.key, sMocks.value}).Return(sMocks.ret)
	return sMocks.sdlIface
}

func runSetCli() (string, error) {
	buf := new(bytes.Buffer)
	cmd := cli.NewSetCmdForTest(newMockSdlApi)
	cmd.SetOut(buf)

	err := cmd.Execute()

	return buf.String(), err
}

func TestCliSetSuccess(t *testing.T) {
	setupSetCliMock("some-ns", "some-key", "some-value", nil)
	_, err := runSetCli()

	assert.Nil(t, err)
}
