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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetCmdShowHelp(t *testing.T) {
	var expOkErr error
	expHelp := "Display one or many resources.\n\nPrints important information about the specified resources."
	expExamples := "Examples:\n  # List keys in the given namespace."
	expNokErr := errors.New("unknown flag: --ff")
	tests := []struct {
		args   []string
		expErr error
		expOut string
	}{
		{args: []string{"-h"}, expErr: expOkErr, expOut: expHelp},
		{args: []string{"--help"}, expErr: expOkErr, expOut: expHelp},
		{args: []string{}, expErr: expOkErr, expOut: expHelp},
		{args: []string{"--ff"}, expErr: expNokErr, expOut: expExamples},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewGetCmdForTest()
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs(test.args)
		err := cmd.Execute()
		result := buf.String()

		assert.Equal(t, test.expErr, err)
		assert.Contains(t, result, test.expOut)
	}
}
