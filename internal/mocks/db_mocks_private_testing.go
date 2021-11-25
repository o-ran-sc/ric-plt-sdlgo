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

package mocks

import (
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/stretchr/testify/mock"
)

type MockDB struct {
	mock.Mock
}

func (m *MockDB) Info() (*sdlgoredis.DbInfo, error) {
	a := m.Called()
	return a.Get(0).(*sdlgoredis.DbInfo), a.Error(1)
}

func (m *MockDB) State() (*sdlgoredis.DbState, error) {
	a := m.Called()
	return a.Get(0).(*sdlgoredis.DbState), a.Error(1)
}

func (m *MockDB) Keys(pattern string) ([]string, error) {
	a := m.Called(pattern)
	return a.Get(0).([]string), a.Error(1)
}

func (m *MockDB) Statistics() (*sdlgoredis.DbStatistics, error) {
	a := m.Called()
	return a.Get(0).(*sdlgoredis.DbStatistics), a.Error(1)
}

type MockSdlApi struct {
	mock.Mock
}

func (m *MockSdlApi) ListKeys(ns string, pattern string) ([]string, error) {
	a := m.Called(ns, pattern)
	return a.Get(0).([]string), a.Error(1)
}

func (m *MockSdlApi) Set(ns string, pairs ...interface{}) error {
	a := m.Called(ns, pairs)
	return a.Error(0)
}

func (m *MockSdlApi) Remove(ns string, keys []string) error {
	a := m.Called(ns, keys)
	return a.Error(0)
}

func (m *MockSdlApi) Get(ns string, keys []string) (map[string]interface{}, error) {
	a := m.Called(ns, keys)
	return a.Get(0).(map[string]interface{}), a.Error(1)
}
