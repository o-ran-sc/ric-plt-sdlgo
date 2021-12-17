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
	"errors"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/cli"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTypesKeysArgsValidate(t *testing.T) {
	tests := []struct {
		ns      string
		pattern string
		expErr  error
	}{
		{ns: "", pattern: "", expErr: nil},
		{ns: "a", pattern: "", expErr: nil},
		{ns: "aa", pattern: "b", expErr: nil},
		{ns: "", pattern: "b", expErr: nil},
	}
	for _, test := range tests {
		assert.Equal(t, test.expErr, cli.NewKeysArgs(test.ns, test.pattern).Validate())
	}
}

func TestTypesKeysArgsValidateNsAsteriskError(t *testing.T) {
	tests := []struct {
		ns      string
		pattern string
		expErr  error
	}{
		{ns: "*", pattern: "", expErr: errors.New("Invalid character (*) in given * namespace argument.")},
		{ns: "\\*", pattern: "", expErr: errors.New("Invalid character (*) in given \\* namespace argument.")},
		{ns: "a*", pattern: "", expErr: errors.New("Invalid character (*) in given a* namespace argument.")},
	}
	for _, test := range tests {
		assert.Equal(t, test.expErr, cli.NewKeysArgs(test.ns, test.pattern).Validate())
	}
}
