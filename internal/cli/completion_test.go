/*
   Copyright (c) 2022 Nokia.

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

func TestCompletionHelp(t *testing.T) {
	var expOkErr error
	expHelp := "To load bash completions:\nsource <(sdlcli completion bash)"
	expNokErr := errors.New("invalid argument \"zsh\" for \"completion\"")
	expNokHelp := "Usage:\n  completion [bash]"
	tests := []struct {
		args      string
		expErr    error
		expOutput string
	}{
		{args: "-h", expErr: expOkErr, expOutput: expHelp},
		{args: "--help", expErr: expOkErr, expOutput: expHelp},
		{args: "zsh", expErr: expNokErr, expOutput: expNokHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewCompletionCmdForTest(buf)
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs([]string{test.args})

		err := cmd.Execute()
		result := buf.String()
		assert.Equal(t, test.expErr, err)
		assert.Contains(t, result, test.expOutput)
	}
}

func TestCompletionBash(t *testing.T) {
	buf := new(bytes.Buffer)
	cmd := cli.NewCompletionCmdForTest(buf)
	cmd.SetArgs([]string{"bash"})

	err := cmd.Execute()
	result := buf.String()
	assert.Nil(t, err)
	assert.Contains(t, result, "# bash completion for completion")
}
