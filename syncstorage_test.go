/*
   Copyright (c) 2021 Nokia.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   This source code is part of the near-RT RIC (RAN Intelligent Controller)
   platform project (RICP).
*/
package sdlgo_test

import (
	"errors"
	"testing"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
	"github.com/stretchr/testify/assert"
)

func setupSDL() (*mockDB, *sdlgo.SyncStorage) {
	dbMock := new(mockDB)
	sdl := sdlgo.NewSyncStorageForTest(dbMock)
	return dbMock, sdl
}

func TestListKeys(t *testing.T) {
	dbMock, sdl := setupSDL()

	tests := []struct {
		keysPatternMockDB     string
		keysReturnValueMockDB []string
		keysPattern           string
		expected              []string
	}{
		{"{ns1},*", []string{"{ns1},key1", "{ns1},key2"}, "*", []string{"key1", "key2"}},
		{"{ns1},ke*", []string{"{ns1},key1", "{ns1},key2"}, "ke*", []string{"key1", "key2"}},
		{"{ns1},ke?2", []string{"{ns1},key2"}, "ke?2", []string{"key2"}},
	}

	for _, test := range tests {
		dbMock.On("Keys", test.keysPatternMockDB).Return(test.keysReturnValueMockDB, nil)

		keys, err := sdl.ListKeys("ns1", test.keysPattern)
		assert.Nil(t, err)
		assert.Equal(t, test.expected, keys)
		dbMock.AssertExpectations(t)
	}
}

func TestListKeysEmpty(t *testing.T) {
	dbMock, sdl := setupSDL()

	dbMock.On("Keys", "{ns1},").Return([]string{}, nil)

	keys, err := sdl.ListKeys("ns1", "")
	assert.Nil(t, err)
	assert.Nil(t, keys)
	dbMock.AssertExpectations(t)
}

func TestListKeysError(t *testing.T) {
	dbMock, sdl := setupSDL()

	errorStringMockDB := string("(empty list or set)")
	dbMock.On("Keys", "{ns1},").Return([]string{}, errors.New(errorStringMockDB))

	keys, err := sdl.ListKeys("ns1", "")
	assert.NotNil(t, err)
	assert.EqualError(t, err, errorStringMockDB)
	assert.Nil(t, keys)
	dbMock.AssertExpectations(t)
}
