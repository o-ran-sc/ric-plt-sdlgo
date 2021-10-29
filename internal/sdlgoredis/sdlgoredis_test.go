/*
   Copyright (c) 2019 AT&T Intellectual Property.
   Copyright (c) 2018-2019 Nokia.

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

package sdlgoredis_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type clientMock struct {
	mock.Mock
}

type pubSubMock struct {
	mock.Mock
}

type MockOS struct {
	mock.Mock
}

func (m *pubSubMock) Channel() <-chan *redis.Message {
	return m.Called().Get(0).(chan *redis.Message)
}

func (m *pubSubMock) Subscribe(channels ...string) error {
	return m.Called().Error(0)
}

func (m *pubSubMock) Unsubscribe(channels ...string) error {
	return m.Called().Error(0)
}

func (m *pubSubMock) Close() error {
	return m.Called().Error(0)
}

func (m *clientMock) Command() *redis.CommandsInfoCmd {
	return m.Called().Get(0).(*redis.CommandsInfoCmd)
}

func (m *clientMock) Close() error {
	return m.Called().Error(0)
}

func (m *clientMock) Subscribe(channels ...string) *redis.PubSub {
	return m.Called(channels).Get(0).(*redis.PubSub)
}

func (m *clientMock) MSet(pairs ...interface{}) *redis.StatusCmd {
	return m.Called(pairs).Get(0).(*redis.StatusCmd)
}

func (m *clientMock) Do(args ...interface{}) *redis.Cmd {
	return m.Called(args).Get(0).(*redis.Cmd)
}

func (m *clientMock) MGet(keys ...string) *redis.SliceCmd {
	return m.Called(keys).Get(0).(*redis.SliceCmd)
}

func (m *clientMock) Del(keys ...string) *redis.IntCmd {
	return m.Called(keys).Get(0).(*redis.IntCmd)
}

func (m *clientMock) Keys(pattern string) *redis.StringSliceCmd {
	return m.Called(pattern).Get(0).(*redis.StringSliceCmd)
}

func (m *clientMock) SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return m.Called(key, value, expiration).Get(0).(*redis.BoolCmd)
}

func (m *clientMock) SAdd(key string, members ...interface{}) *redis.IntCmd {
	return m.Called(key, members).Get(0).(*redis.IntCmd)
}

func (m *clientMock) SRem(key string, members ...interface{}) *redis.IntCmd {
	return m.Called(key, members).Get(0).(*redis.IntCmd)
}

func (m *clientMock) SMembers(key string) *redis.StringSliceCmd {
	return m.Called(key).Get(0).(*redis.StringSliceCmd)
}

func (m *clientMock) SIsMember(key string, member interface{}) *redis.BoolCmd {
	return m.Called(key, member).Get(0).(*redis.BoolCmd)
}

func (m *clientMock) SCard(key string) *redis.IntCmd {
	return m.Called(key).Get(0).(*redis.IntCmd)
}

func (m *clientMock) PTTL(key string) *redis.DurationCmd {
	return m.Called(key).Get(0).(*redis.DurationCmd)
}

func (m *clientMock) Eval(script string, keys []string, args ...interface{}) *redis.Cmd {
	return m.Called(script, keys).Get(0).(*redis.Cmd)
}

func (m *clientMock) EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return m.Called(sha1, keys, args).Get(0).(*redis.Cmd)
}

func (m *clientMock) ScriptExists(scripts ...string) *redis.BoolSliceCmd {
	return m.Called(scripts).Get(0).(*redis.BoolSliceCmd)
}

func (m *clientMock) ScriptLoad(script string) *redis.StringCmd {
	return m.Called(script).Get(0).(*redis.StringCmd)
}

func (m *clientMock) Info(section ...string) *redis.StringCmd {
	return m.Called(section).Get(0).(*redis.StringCmd)
}

func setSubscribeNotifications() (*pubSubMock, sdlgoredis.SubscribeFn) {
	mock := new(pubSubMock)
	return mock, func(client sdlgoredis.RedisClient, channels ...string) sdlgoredis.Subscriber {
		return mock
	}
}

func (m *MockOS) Getenv(key string, defValue string) string {
	a := m.Called(key, defValue)
	return a.String(0)
}

func setup(commandsExists bool) (*pubSubMock, *clientMock, *sdlgoredis.DB) {
	mock := new(clientMock)
	pubSubMock, subscribeNotifications := setSubscribeNotifications()
	db := sdlgoredis.CreateDB(mock, subscribeNotifications)

	dummyCommandInfo := redis.CommandInfo{
		Name: "dummy",
	}
	cmdResult := make(map[string]*redis.CommandInfo, 0)

	if commandsExists {
		cmdResult = map[string]*redis.CommandInfo{
			"setie":    &dummyCommandInfo,
			"delie":    &dummyCommandInfo,
			"setiepub": &dummyCommandInfo,
			"setnxpub": &dummyCommandInfo,
			"msetmpub": &dummyCommandInfo,
			"delmpub":  &dummyCommandInfo,
		}
	} else {
		cmdResult = map[string]*redis.CommandInfo{
			"dummy": &dummyCommandInfo,
		}
	}

	mock.On("Command").Return(redis.NewCommandsInfoCmdResult(cmdResult, nil))
	db.CheckCommands()
	return pubSubMock, mock, db
}

func setupEnv(host, port, msname, sntport, clsaddrlist string) ([]*clientMock, []*sdlgoredis.DB) {
	var clmocks []*clientMock

	dummyCommandInfo := redis.CommandInfo{
		Name: "dummy",
	}
	cmdResult := make(map[string]*redis.CommandInfo, 0)

	cmdResult = map[string]*redis.CommandInfo{
		"dummy": &dummyCommandInfo,
	}

	osmock := new(MockOS)
	osmock.On("Getenv", "DBAAS_SERVICE_HOST", "localhost").Return(host)
	osmock.On("Getenv", "DBAAS_SERVICE_PORT", "6379").Return(port)
	osmock.On("Getenv", "DBAAS_MASTER_NAME", "").Return(msname)
	osmock.On("Getenv", "DBAAS_SERVICE_SENTINEL_PORT", "").Return(sntport)
	osmock.On("Getenv", "DBAAS_CLUSTER_ADDR_LIST", "").Return(clsaddrlist)

	clients := sdlgoredis.ReadConfigAndCreateDbClients(
		osmock,
		func(addr, port, clusterName string, isHa bool) sdlgoredis.RedisClient {
			clm := new(clientMock)
			clm.On("Command").Return(redis.NewCommandsInfoCmdResult(cmdResult, nil))
			clmocks = append(clmocks, clm)
			return clm
		},
	)

	return clmocks, clients
}

func TestCloseDbSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	r.On("Close").Return(nil)
	err := db.CloseDB()
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestCloseDbFailure(t *testing.T) {
	_, r, db := setup(true)
	r.On("Close").Return(errors.New("Some error"))
	err := db.CloseDB()
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestMSetSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKeysAndValues := []interface{}{"key1", "value1", "key2", 2}
	r.On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := db.MSet("key1", "value1", "key2", 2)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestMSetFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKeysAndValues := []interface{}{"key1", "value1", "key2", 2}
	r.On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", errors.New("Some error")))
	err := db.MSet("key1", "value1", "key2", 2)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestMSetMPubSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"MSETMPUB", 2, 2, "key1", "val1", "key2", "val2",
		"chan1", "event1", "chan2", "event2"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("", nil))
	assert.Nil(t, db.MSetMPub([]string{"chan1", "event1", "chan2", "event2"},
		"key1", "val1", "key2", "val2"))
	r.AssertExpectations(t)
}

func TestMsetMPubFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"MSETMPUB", 2, 2, "key1", "val1", "key2", "val2",
		"chan1", "event1", "chan2", "event2"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("", errors.New("Some error")))
	assert.NotNil(t, db.MSetMPub([]string{"chan1", "event1", "chan2", "event2"},
		"key1", "val1", "key2", "val2"))
	r.AssertExpectations(t)
}

func TestMSetMPubCommandMissing(t *testing.T) {
	_, r, db := setup(false)
	expectedMessage := []interface{}{"MSETMPUB", 2, 2, "key1", "val1", "key2", "val2",
		"chan1", "event1", "chan2", "event2"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	assert.NotNil(t, db.MSetMPub([]string{"chan1", "event1", "chan2", "event2"},
		"key1", "val1", "key2", "val2"))
	r.AssertExpectations(t)

}

func TestMGetSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKeys := []string{"key1", "key2", "key3"}
	expectedResult := []interface{}{"val1", 2, nil}
	r.On("MGet", expectedKeys).Return(redis.NewSliceResult(expectedResult, nil))
	result, err := db.MGet([]string{"key1", "key2", "key3"})
	assert.Equal(t, result, expectedResult)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestMGetFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKeys := []string{"key1", "key2", "key3"}
	expectedResult := []interface{}{nil}
	r.On("MGet", expectedKeys).Return(redis.NewSliceResult(expectedResult,
		errors.New("Some error")))
	result, err := db.MGet([]string{"key1", "key2", "key3"})
	assert.Equal(t, result, expectedResult)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestDelMPubSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELMPUB", 2, 2, "key1", "key2", "chan1", "event1",
		"chan2", "event2"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("", nil))
	assert.Nil(t, db.DelMPub([]string{"chan1", "event1", "chan2", "event2"},
		[]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestDelMPubFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELMPUB", 2, 2, "key1", "key2", "chan1", "event1",
		"chan2", "event2"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("", errors.New("Some error")))
	assert.NotNil(t, db.DelMPub([]string{"chan1", "event1", "chan2", "event2"},
		[]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestDelMPubCommandMissing(t *testing.T) {
	_, r, db := setup(false)
	expectedMessage := []interface{}{"DELMPUB", 2, 2, "key1", "key2", "chan1", "event1",
		"chan2", "event2"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	assert.NotNil(t, db.DelMPub([]string{"chan1", "event1", "chan2", "event2"},
		[]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestDelSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKeys := []string{"key1", "key2"}
	r.On("Del", expectedKeys).Return(redis.NewIntResult(2, nil))
	assert.Nil(t, db.Del([]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestDelFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKeys := []string{"key1", "key2"}
	r.On("Del", expectedKeys).Return(redis.NewIntResult(2, errors.New("Some error")))
	assert.NotNil(t, db.Del([]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestKeysSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedPattern := "pattern*"
	expectedResult := []string{"pattern1", "pattern2"}
	r.On("Keys", expectedPattern).Return(redis.NewStringSliceResult(expectedResult, nil))
	result, err := db.Keys("pattern*")
	assert.Equal(t, result, expectedResult)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestKeysFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedPattern := "pattern*"
	expectedResult := []string{}
	r.On("Keys", expectedPattern).Return(redis.NewStringSliceResult(expectedResult,
		errors.New("Some error")))
	_, err := db.Keys("pattern*")
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEKeyExists(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETIE", "key", "newdata", "olddata"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("OK", nil))
	result, err := db.SetIE("key", "olddata", "newdata")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEKeyDoesntExists(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETIE", "key", "newdata", "olddata"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, nil))
	result, err := db.SetIE("key", "olddata", "newdata")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETIE", "key", "newdata", "olddata"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, errors.New("Some error")))
	result, err := db.SetIE("key", "olddata", "newdata")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetIECommandMissing(t *testing.T) {
	_, r, db := setup(false)
	expectedMessage := []interface{}{"SETIE", "key", "newdata", "olddata"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.SetIE("key", "olddata", "newdata")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEPubKeyExists(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETIEMPUB", "key", "newdata", "olddata", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("OK", nil))
	result, err := db.SetIEPub([]string{"channel", "message"}, "key", "olddata", "newdata")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEPubKeyDoesntExists(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETIEMPUB", "key", "newdata", "olddata", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, nil))
	result, err := db.SetIEPub([]string{"channel", "message"}, "key", "olddata", "newdata")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEPubFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETIEMPUB", "key", "newdata", "olddata", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, errors.New("Some error")))
	result, err := db.SetIEPub([]string{"channel", "message"}, "key", "olddata", "newdata")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEPubCommandMissing(t *testing.T) {
	_, r, db := setup(false)
	expectedMessage := []interface{}{"SETIEMPUB", "key", "newdata", "olddata", "channel", "message"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.SetIEPub([]string{"channel", "message"}, "key", "olddata", "newdata")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXPubKeyDoesntExist(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETNXMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("OK", nil))
	result, err := db.SetNXPub([]string{"channel", "message"}, "key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXPubKeyExists(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETNXMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, nil))
	result, err := db.SetNXPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXPubFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"SETNXMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, errors.New("Some error")))
	result, err := db.SetNXPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXPubCommandMissing(t *testing.T) {
	_, r, db := setup(false)
	expectedMessage := []interface{}{"SETNXMPUB", "key", "data", "channel", "message"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.SetNXPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SetNX", expectedKey, expectedData, time.Duration(0)).Return(redis.NewBoolResult(true, nil))
	result, err := db.SetNX("key", "data", 0)
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXUnsuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SetNX", expectedKey, expectedData, time.Duration(0)).Return(redis.NewBoolResult(false, nil))
	result, err := db.SetNX("key", "data", 0)
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SetNX", expectedKey, expectedData, time.Duration(0)).
		Return(redis.NewBoolResult(false, errors.New("Some error")))
	result, err := db.SetNX("key", "data", 0)
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubKeyDoesntExist(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(0), nil))
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubKeyExists(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(1), nil))
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubKeyExistsIntTypeRedisValue(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(1, nil))
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(0), errors.New("Some error")))
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubCommandMissing(t *testing.T) {
	_, r, db := setup(false)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEKeyDoesntExist(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(0), nil))
	result, err := db.DelIE("key", "data")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEKeyExists(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(1), nil))
	result, err := db.DelIE("key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEKeyExistsIntTypeRedisValue(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(1, nil))
	result, err := db.DelIE("key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(0), errors.New("Some error")))
	result, err := db.DelIE("key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestDelIECommandMissing(t *testing.T) {
	_, r, db := setup(false)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.DelIE("key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSAddSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := []interface{}{"data", 2}
	r.On("SAdd", expectedKey, expectedData).Return(redis.NewIntResult(2, nil))
	assert.Nil(t, db.SAdd("key", "data", 2))
	r.AssertExpectations(t)
}

func TestSAddFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := []interface{}{"data", 2}
	r.On("SAdd", expectedKey, expectedData).Return(redis.NewIntResult(2, errors.New("Some error")))
	assert.NotNil(t, db.SAdd("key", "data", 2))
	r.AssertExpectations(t)
}

func TestSRemSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := []interface{}{"data", 2}
	r.On("SRem", expectedKey, expectedData).Return(redis.NewIntResult(2, nil))
	assert.Nil(t, db.SRem("key", "data", 2))
	r.AssertExpectations(t)
}

func TestSRemFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := []interface{}{"data", 2}
	r.On("SRem", expectedKey, expectedData).Return(redis.NewIntResult(2, errors.New("Some error")))
	assert.NotNil(t, db.SRem("key", "data", 2))
	r.AssertExpectations(t)
}

func TestSMembersSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedResult := []string{"member1", "member2"}
	r.On("SMembers", expectedKey).Return(redis.NewStringSliceResult(expectedResult, nil))
	result, err := db.SMembers("key")
	assert.Equal(t, result, expectedResult)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSMembersFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedResult := []string{"member1", "member2"}
	r.On("SMembers", expectedKey).Return(redis.NewStringSliceResult(expectedResult,
		errors.New("Some error")))
	result, err := db.SMembers("key")
	assert.Equal(t, result, expectedResult)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSIsMemberIsMember(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SIsMember", expectedKey, expectedData).Return(redis.NewBoolResult(true, nil))
	result, err := db.SIsMember("key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSIsMemberIsNotMember(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SIsMember", expectedKey, expectedData).Return(redis.NewBoolResult(false, nil))
	result, err := db.SIsMember("key", "data")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSIsMemberFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SIsMember", expectedKey, expectedData).
		Return(redis.NewBoolResult(false, errors.New("Some error")))
	result, err := db.SIsMember("key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSCardSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	r.On("SCard", expectedKey).Return(redis.NewIntResult(1, nil))
	result, err := db.SCard("key")
	assert.Equal(t, int64(1), result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSCardFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	r.On("SCard", expectedKey).Return(redis.NewIntResult(1, errors.New("Some error")))
	result, err := db.SCard("key")
	assert.Equal(t, int64(1), result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSubscribeChannelDBSubscribeRXUnsubscribe(t *testing.T) {
	ps, r, db := setup(true)
	ch := make(chan *redis.Message)
	msg := redis.Message{
		Channel: "{prefix}channel",
		Pattern: "pattern",
		Payload: "event",
	}
	ps.On("Channel").Return(ch)
	ps.On("Unsubscribe").Return(nil)
	ps.On("Close").Return(nil)
	count := 0
	receivedChannel := ""
	db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel = channel
	}, "{prefix}", "---", "{prefix}channel")
	ch <- &msg
	db.UnsubscribeChannelDB("{prefix}channel")
	time.Sleep(1 * time.Second)
	assert.Equal(t, 1, count)
	assert.Equal(t, "channel", receivedChannel)
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestSubscribeChannelDBSubscribeTwoUnsubscribeOne(t *testing.T) {
	ps, r, db := setup(true)
	ch := make(chan *redis.Message)
	msg1 := redis.Message{
		Channel: "{prefix}channel1",
		Pattern: "pattern",
		Payload: "event",
	}
	msg2 := redis.Message{
		Channel: "{prefix}channel2",
		Pattern: "pattern",
		Payload: "event",
	}
	ps.On("Channel").Return(ch)
	ps.On("Subscribe").Return(nil)
	ps.On("Unsubscribe").Return(nil)
	ps.On("Unsubscribe").Return(nil)
	ps.On("Close").Return(nil)
	count := 0
	receivedChannel1 := ""
	db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel1 = channel
	}, "{prefix}", "---", "{prefix}channel1")
	ch <- &msg1
	receivedChannel2 := ""
	db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel2 = channel
	}, "{prefix}", "---", "{prefix}channel2")

	time.Sleep(1 * time.Second)
	db.UnsubscribeChannelDB("{prefix}channel1")
	ch <- &msg2
	db.UnsubscribeChannelDB("{prefix}channel2")
	time.Sleep(1 * time.Second)
	assert.Equal(t, 2, count)
	assert.Equal(t, "channel1", receivedChannel1)
	assert.Equal(t, "channel2", receivedChannel2)
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestSubscribeChannelReDBSubscribeAfterUnsubscribe(t *testing.T) {
	ps, r, db := setup(true)
	ch := make(chan *redis.Message)
	msg := redis.Message{
		Channel: "{prefix}channel",
		Pattern: "pattern",
		Payload: "event",
	}
	ps.On("Channel").Return(ch)
	ps.On("Unsubscribe").Return(nil)
	ps.On("Close").Return(nil)
	count := 0
	receivedChannel := ""

	db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel = channel
	}, "{prefix}", "---", "{prefix}channel")
	ch <- &msg
	db.UnsubscribeChannelDB("{prefix}channel")
	time.Sleep(1 * time.Second)

	db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel = channel
	}, "{prefix}", "---", "{prefix}channel")
	ch <- &msg
	db.UnsubscribeChannelDB("{prefix}channel")

	time.Sleep(1 * time.Second)
	assert.Equal(t, 2, count)
	assert.Equal(t, "channel", receivedChannel)
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestPTTLSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedResult := time.Duration(1)
	r.On("PTTL", expectedKey).Return(redis.NewDurationResult(expectedResult,
		nil))
	result, err := db.PTTL("key")
	assert.Equal(t, result, expectedResult)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestPTTLFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedResult := time.Duration(1)
	r.On("PTTL", expectedKey).Return(redis.NewDurationResult(expectedResult,
		errors.New("Some error")))
	result, err := db.PTTL("key")
	assert.Equal(t, result, expectedResult)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestPExpireIESuccessfully(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	expectedDuration := strconv.FormatInt(int64(10000), 10)

	r.On("EvalSha", mock.Anything, []string{expectedKey}, []interface{}{expectedData, expectedDuration}).
		Return(redis.NewCmdResult(int64(1), nil))

	err := db.PExpireIE("key", "data", 10*time.Second)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestPExpireIEFailure(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	expectedDuration := strconv.FormatInt(int64(10000), 10)

	r.On("EvalSha", mock.Anything, []string{expectedKey}, []interface{}{expectedData, expectedDuration}).
		Return(redis.NewCmdResult(int64(1), errors.New("Some error")))

	err := db.PExpireIE("key", "data", 10*time.Second)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestPExpireIELockNotHeld(t *testing.T) {
	_, r, db := setup(true)
	expectedKey := "key"
	expectedData := "data"
	expectedDuration := strconv.FormatInt(int64(10000), 10)

	r.On("EvalSha", mock.Anything, []string{expectedKey}, []interface{}{expectedData, expectedDuration}).
		Return(redis.NewCmdResult(int64(0), nil))

	err := db.PExpireIE("key", "data", 10*time.Second)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestClientStandaloneRedisLegacyEnv(t *testing.T) {
	rcls, dbs := setupEnv(
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6376", "", "", "",
	)
	assert.Equal(t, 1, len(rcls))
	assert.Equal(t, 1, len(dbs))

	expectedKeysAndValues := []interface{}{"key1", "value1"}
	rcls[0].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := dbs[0].MSet("key1", "value1")
	assert.Nil(t, err)
	rcls[0].AssertExpectations(t)
}

func TestClientSentinelRedisLegacyEnv(t *testing.T) {
	rcls, dbs := setupEnv(
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6376", "dbaasmaster", "26376", "",
	)
	assert.Equal(t, 1, len(rcls))
	assert.Equal(t, 1, len(dbs))

	expectedKeysAndValues := []interface{}{"key1", "value1"}
	rcls[0].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := dbs[0].MSet("key1", "value1")
	assert.Nil(t, err)
	rcls[0].AssertExpectations(t)
}

func TestClientTwoStandaloneRedisEnvs(t *testing.T) {
	rcls, dbs := setupEnv(
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6376", "", "",
		"service-ricplt-dbaas-tcp-cluster-0.ricplt,service-ricplt-dbaas-tcp-cluster-1.ricplt",
	)
	assert.Equal(t, 2, len(rcls))
	assert.Equal(t, 2, len(dbs))

	expectedKeysAndValues := []interface{}{"key1", "value1"}
	rcls[0].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := dbs[0].MSet("key1", "value1")
	assert.Nil(t, err)
	rcls[0].AssertExpectations(t)

	expectedKeysAndValues = []interface{}{"key2", "value2"}
	rcls[1].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err = dbs[1].MSet("key2", "value2")
	assert.Nil(t, err)
	rcls[0].AssertExpectations(t)
	rcls[1].AssertExpectations(t)
}

func TestClientTwoSentinelRedisEnvs(t *testing.T) {
	rcls, dbs := setupEnv(
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6376", "dbaasmaster", "26376",
		"service-ricplt-dbaas-tcp-cluster-0.ricplt,service-ricplt-dbaas-tcp-cluster-1.ricplt",
	)
	assert.Equal(t, 2, len(rcls))
	assert.Equal(t, 2, len(dbs))

	expectedKeysAndValues := []interface{}{"key1", "value1"}
	rcls[0].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := dbs[0].MSet("key1", "value1")
	assert.Nil(t, err)
	rcls[0].AssertExpectations(t)

	expectedKeysAndValues = []interface{}{"key2", "value2"}
	rcls[1].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err = dbs[1].MSet("key2", "value2")
	assert.Nil(t, err)
	rcls[0].AssertExpectations(t)
	rcls[1].AssertExpectations(t)
}

func TestInfoOfMasterRedisWithTwoSlavesSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:2\r\n" +
		"min_slaves_good_slaves:2\r\n" +
		"slave0:ip=1.2.3.4,port=6379,state=online,offset=100200300,lag=0\r\n" +
		"slave1:ip=5.6.7.8,port=6379,state=online,offset=100200300,lag=0\r\n"
	expInfo := &internal.DbInfo{
		MasterRole:      true,
		ConfReplicasCnt: 2,
		Replicas: []internal.DbInfoReplica{
			internal.DbInfoReplica{
				Addr:   "1.2.3.4:6379",
				Online: true,
			},
			internal.DbInfoReplica{
				Addr:   "5.6.7.8:6379",
				Online: true,
			},
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoOfMasterRedisWithOneSlaveOnlineAndOtherSlaveNotOnlineSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:2\r\n" +
		"min_slaves_good_slaves:2\r\n" +
		"slave0:ip=1.2.3.4,port=6379,state=online,offset=100200300,lag=0\r\n" +
		"slave1:ip=5.6.7.8,port=6379,state=wait_bgsave,offset=100200300,lag=0\r\n"
	expInfo := &internal.DbInfo{
		MasterRole:      true,
		ConfReplicasCnt: 2,
		Replicas: []internal.DbInfoReplica{
			internal.DbInfoReplica{
				Addr:   "1.2.3.4:6379",
				Online: true,
			},
			internal.DbInfoReplica{
				Addr:   "5.6.7.8:6379",
				Online: false,
			},
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoOfStandaloneMasterRedisSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:0\r\n"
	expInfo := &internal.DbInfo{
		MasterRole: true,
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoWithGibberishContentSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	redisInfo := "!#¤%&?+?´-\r\n"
	expInfo := &internal.DbInfo{
		MasterRole: false,
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoWithEmptyContentSuccessfully(t *testing.T) {
	_, r, db := setup(true)
	var redisInfo string
	expInfo := &internal.DbInfo{
		MasterRole: false,
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}
