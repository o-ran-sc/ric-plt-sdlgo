/*
   Copyright (c) 2019 AT&T Intellectual Property.
   Copyright (c) 2018-2022 Nokia.

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
	"context"
	"errors"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strconv"
	"testing"
	"time"
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

func (m *pubSubMock) Channel(opts ...redis.ChannelOption) <-chan *redis.Message {
	return m.Called().Get(0).(chan *redis.Message)
}

func (m *pubSubMock) Subscribe(ctx context.Context, channels ...string) error {
	return m.Called().Error(0)
}

func (m *pubSubMock) Unsubscribe(ctx context.Context, channels ...string) error {
	return m.Called().Error(0)
}

func (m *pubSubMock) Close() error {
	return m.Called().Error(0)
}

func (m *clientMock) Command(ctx context.Context) *redis.CommandsInfoCmd {
	return m.Called().Get(0).(*redis.CommandsInfoCmd)
}

func (m *clientMock) Close() error {
	return m.Called().Error(0)
}

func (m *clientMock) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return m.Called(channels).Get(0).(*redis.PubSub)
}

func (m *clientMock) MSet(ctx context.Context, pairs ...interface{}) *redis.StatusCmd {
	return m.Called(pairs).Get(0).(*redis.StatusCmd)
}

func (m *clientMock) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	return m.Called(args).Get(0).(*redis.Cmd)
}

func (m *clientMock) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return m.Called(keys).Get(0).(*redis.SliceCmd)
}

func (m *clientMock) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return m.Called(keys).Get(0).(*redis.IntCmd)
}

func (m *clientMock) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return m.Called(pattern).Get(0).(*redis.StringSliceCmd)
}

func (m *clientMock) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return m.Called(key, value, expiration).Get(0).(*redis.BoolCmd)
}

func (m *clientMock) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return m.Called(key, members).Get(0).(*redis.IntCmd)
}

func (m *clientMock) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return m.Called(key, members).Get(0).(*redis.IntCmd)
}

func (m *clientMock) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return m.Called(key).Get(0).(*redis.StringSliceCmd)
}

func (m *clientMock) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	return m.Called(key, member).Get(0).(*redis.BoolCmd)
}

func (m *clientMock) SCard(ctx context.Context, key string) *redis.IntCmd {
	return m.Called(key).Get(0).(*redis.IntCmd)
}

func (m *clientMock) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	return m.Called(key).Get(0).(*redis.DurationCmd)
}

func (m *clientMock) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return m.Called(script, keys).Get(0).(*redis.Cmd)
}

func (m *clientMock) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return m.Called(sha1, keys, args).Get(0).(*redis.Cmd)
}

func (m *clientMock) ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd {
	return m.Called(scripts).Get(0).(*redis.BoolSliceCmd)
}

func (m *clientMock) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return m.Called(script).Get(0).(*redis.StringCmd)
}

func (m *clientMock) Info(ctx context.Context, section ...string) *redis.StringCmd {
	return m.Called(section).Get(0).(*redis.StringCmd)
}

type MockRedisSentinel struct {
	mock.Mock
}

func (m *MockRedisSentinel) Master(ctx context.Context, name string) *redis.StringStringMapCmd {
	a := m.Called(name)
	return a.Get(0).(*redis.StringStringMapCmd)
}

func (m *MockRedisSentinel) Slaves(ctx context.Context, name string) *redis.SliceCmd {
	a := m.Called(name)
	return a.Get(0).(*redis.SliceCmd)
}

func (m *MockRedisSentinel) Sentinels(ctx context.Context, name string) *redis.SliceCmd {
	a := m.Called(name)
	return a.Get(0).(*redis.SliceCmd)
}

func setSubscribeNotifications() (*pubSubMock, sdlgoredis.SubscribeFn) {
	mock := new(pubSubMock)
	return mock, func(ctx context.Context, client sdlgoredis.RedisClient, channels ...string) sdlgoredis.Subscriber {
		return mock
	}
}

func (m *MockOS) Getenv(key string, defValue string) string {
	a := m.Called(key, defValue)
	return a.String(0)
}

type setupEv struct {
	pubSubMock []*pubSubMock
	rClient    []*clientMock
	rSentinel  []*MockRedisSentinel
	db         []*sdlgoredis.DB
}

func setupHaEnv(commandsExists bool) (*pubSubMock, *clientMock, *sdlgoredis.DB) {
	psm, cm, _, db := setupHaEnvWithSentinels(commandsExists, "3")
	return psm, cm, db
}

func setupHaEnvWithSentinels(commandsExists bool, nodeCnt string) (*pubSubMock, *clientMock, []*MockRedisSentinel, *sdlgoredis.DB) {
	setupVals := setupEnv(
		commandsExists,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt",
		"6379",
		"dbaasmaster-cluster-0",
		"26379",
		"",
		nodeCnt,
	)
	return setupVals.pubSubMock[0], setupVals.rClient[0], setupVals.rSentinel, setupVals.db[0]
}

func setupSingleEnv(commandsExists bool, nodeCnt string) (*pubSubMock, *clientMock, *sdlgoredis.DB) {
	setupVals := setupEnv(
		commandsExists,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt",
		"6379", "", "", "", nodeCnt,
	)
	return setupVals.pubSubMock[0], setupVals.rClient[0], setupVals.db[0]
}

func setupEnv(commandsExists bool, host, port, msname, sntport, clsaddrlist, nodeCnt string) setupEv {
	var ret setupEv

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

	osmock := new(MockOS)
	osmock.On("Getenv", "DBAAS_SERVICE_HOST", "localhost").Return(host)
	osmock.On("Getenv", "DBAAS_SERVICE_PORT", "6379").Return(port)
	osmock.On("Getenv", "DBAAS_MASTER_NAME", "").Return(msname)
	osmock.On("Getenv", "DBAAS_SERVICE_SENTINEL_PORT", "").Return(sntport)
	osmock.On("Getenv", "DBAAS_CLUSTER_ADDR_LIST", "").Return(clsaddrlist)
	osmock.On("Getenv", "DBAAS_NODE_COUNT", "1").Return(nodeCnt)

	pubSubMock, subscribeNotifications := setSubscribeNotifications()
	smock := new(MockRedisSentinel)
	ret.rSentinel = append(ret.rSentinel, smock)
	clients := sdlgoredis.ReadConfigAndCreateDbClients(
		osmock,
		func(addr, port, clusterName string, isHa bool) sdlgoredis.RedisClient {
			clm := new(clientMock)
			clm.On("Command").Return(redis.NewCommandsInfoCmdResult(cmdResult, nil))
			ret.rClient = append(ret.rClient, clm)
			ret.pubSubMock = append(ret.pubSubMock, pubSubMock)
			return clm
		},
		subscribeNotifications,
		func(addr, sentinelPort, masterName, nodeCnt string) *sdlgoredis.Sentinel {
			s := &sdlgoredis.Sentinel{
				IredisSentinelClient: smock,
				MasterName:           masterName,
				NodeCnt:              nodeCnt,
			}
			return s
		},
	)
	ret.db = clients
	return ret
}

func newMockRedisMasterCallResp(role, ip, port, flag string) map[string]string {
	resp := map[string]string{}
	if role != "" {
		resp["role-reported"] = role
		resp["ip"] = ip
		resp["port"] = port
		resp["flags"] = flag
	}
	return resp
}

type mockRedisSlaves struct {
	resp []interface{}
}

func newMockRedisSlavesCall() *mockRedisSlaves {
	return new(mockRedisSlaves)
}

func (mrr *mockRedisSlaves) add(role, ip, port, link, flag string) {
	mrr.resp = append(mrr.resp,
		[]interface{}{
			"role-reported", role,
			"ip", ip,
			"port", port,
			"master-link-status", link,
			"flags", flag,
		},
	)
}

type mockRedisSentinels struct {
	resp []interface{}
}

func newMockRedisSentinelsCall() *mockRedisSentinels {
	return new(mockRedisSentinels)
}

func (mrs *mockRedisSentinels) add(ip, port, flag string) {
	mrs.resp = append(mrs.resp,
		[]interface{}{
			"ip", ip,
			"port", port,
			"flags", flag,
		},
	)
}

type ExpDbState struct {
	s sdlgoredis.DbState
}

func newExpDbState(nodeCnt int, err error) *ExpDbState {
	state := new(ExpDbState)
	state.s.ConfigNodeCnt = nodeCnt
	state.s.Err = err
	return state
}

func (edbs *ExpDbState) addPrimary(role, ip, port, flag string, err error) {
	edbs.s.PrimaryDbState.Err = err
	edbs.s.PrimaryDbState.Fields = sdlgoredis.PrimaryDbStateFields{
		Role:  role,
		Ip:    ip,
		Port:  port,
		Flags: flag,
	}
}

func (edbs *ExpDbState) addReplica(role, ip, port, link, flag string, err error) {
	if edbs.s.ReplicasDbState == nil {
		edbs.s.ReplicasDbState = new(sdlgoredis.ReplicasDbState)
		edbs.s.ReplicasDbState.States = make([]*sdlgoredis.ReplicaDbState, 0)
	}
	edbs.s.ReplicasDbState.Err = err
	if ip != "" || port != "" || link != "" || flag != "" {
		edbs.s.ReplicasDbState.States = append(edbs.s.ReplicasDbState.States,
			&sdlgoredis.ReplicaDbState{
				Fields: sdlgoredis.ReplicaDbStateFields{
					Role:              role,
					Ip:                ip,
					Port:              port,
					PrimaryLinkStatus: link,
					Flags:             flag,
				},
			},
		)
	}
}

func (edbs *ExpDbState) addSentinel(ip, port, flag string, err error) {
	if edbs.s.SentinelsDbState == nil {
		edbs.s.SentinelsDbState = new(sdlgoredis.SentinelsDbState)
		edbs.s.SentinelsDbState.States = make([]*sdlgoredis.SentinelDbState, 0)
	}
	edbs.s.SentinelsDbState.Err = err
	if ip != "" || port != "" || flag != "" {
		edbs.s.SentinelsDbState.States = append(edbs.s.SentinelsDbState.States,
			&sdlgoredis.SentinelDbState{
				Fields: sdlgoredis.SentinelDbStateFields{
					Ip:    ip,
					Port:  port,
					Flags: flag,
				},
			},
		)
	}
}

func TestCloseDbSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	r.On("Close").Return(nil)
	err := db.CloseDB()
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestCloseDbFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	r.On("Close").Return(errors.New("Some error"))
	err := db.CloseDB()
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestMSetSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKeysAndValues := []interface{}{"key1", "value1", "key2", 2}
	r.On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := db.MSet("key1", "value1", "key2", 2)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestMSetFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKeysAndValues := []interface{}{"key1", "value1", "key2", 2}
	r.On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", errors.New("Some error")))
	err := db.MSet("key1", "value1", "key2", 2)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestMSetMPubSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"MSETMPUB", 2, 2, "key1", "val1", "key2", "val2",
		"chan1", "event1", "chan2", "event2"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("", nil))
	assert.Nil(t, db.MSetMPub([]string{"chan1", "event1", "chan2", "event2"},
		"key1", "val1", "key2", "val2"))
	r.AssertExpectations(t)
}

func TestMsetMPubFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"MSETMPUB", 2, 2, "key1", "val1", "key2", "val2",
		"chan1", "event1", "chan2", "event2"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("", errors.New("Some error")))
	assert.NotNil(t, db.MSetMPub([]string{"chan1", "event1", "chan2", "event2"},
		"key1", "val1", "key2", "val2"))
	r.AssertExpectations(t)
}

func TestMSetMPubCommandMissing(t *testing.T) {
	_, r, db := setupHaEnv(false)
	expectedMessage := []interface{}{"MSETMPUB", 2, 2, "key1", "val1", "key2", "val2",
		"chan1", "event1", "chan2", "event2"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	assert.NotNil(t, db.MSetMPub([]string{"chan1", "event1", "chan2", "event2"},
		"key1", "val1", "key2", "val2"))
	r.AssertExpectations(t)

}

func TestMGetSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKeys := []string{"key1", "key2", "key3"}
	expectedResult := []interface{}{"val1", 2, nil}
	r.On("MGet", expectedKeys).Return(redis.NewSliceResult(expectedResult, nil))
	result, err := db.MGet([]string{"key1", "key2", "key3"})
	assert.Equal(t, result, expectedResult)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestMGetFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
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
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELMPUB", 2, 2, "key1", "key2", "chan1", "event1",
		"chan2", "event2"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("", nil))
	assert.Nil(t, db.DelMPub([]string{"chan1", "event1", "chan2", "event2"},
		[]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestDelMPubFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELMPUB", 2, 2, "key1", "key2", "chan1", "event1",
		"chan2", "event2"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("", errors.New("Some error")))
	assert.NotNil(t, db.DelMPub([]string{"chan1", "event1", "chan2", "event2"},
		[]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestDelMPubCommandMissing(t *testing.T) {
	_, r, db := setupHaEnv(false)
	expectedMessage := []interface{}{"DELMPUB", 2, 2, "key1", "key2", "chan1", "event1",
		"chan2", "event2"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	assert.NotNil(t, db.DelMPub([]string{"chan1", "event1", "chan2", "event2"},
		[]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestDelSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKeys := []string{"key1", "key2"}
	r.On("Del", expectedKeys).Return(redis.NewIntResult(2, nil))
	assert.Nil(t, db.Del([]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestDelFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKeys := []string{"key1", "key2"}
	r.On("Del", expectedKeys).Return(redis.NewIntResult(2, errors.New("Some error")))
	assert.NotNil(t, db.Del([]string{"key1", "key2"}))
	r.AssertExpectations(t)
}

func TestKeysSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedPattern := "pattern*"
	expectedResult := []string{"pattern1", "pattern2"}
	r.On("Keys", expectedPattern).Return(redis.NewStringSliceResult(expectedResult, nil))
	result, err := db.Keys("pattern*")
	assert.Equal(t, result, expectedResult)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestKeysFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedPattern := "pattern*"
	expectedResult := []string{}
	r.On("Keys", expectedPattern).Return(redis.NewStringSliceResult(expectedResult,
		errors.New("Some error")))
	_, err := db.Keys("pattern*")
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEKeyExists(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETIE", "key", "newdata", "olddata"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("OK", nil))
	result, err := db.SetIE("key", "olddata", "newdata")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEKeyDoesntExists(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETIE", "key", "newdata", "olddata"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, nil))
	result, err := db.SetIE("key", "olddata", "newdata")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETIE", "key", "newdata", "olddata"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, errors.New("Some error")))
	result, err := db.SetIE("key", "olddata", "newdata")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetIECommandMissing(t *testing.T) {
	_, r, db := setupHaEnv(false)
	expectedMessage := []interface{}{"SETIE", "key", "newdata", "olddata"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.SetIE("key", "olddata", "newdata")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEPubKeyExists(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETIEMPUB", "key", "newdata", "olddata", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("OK", nil))
	result, err := db.SetIEPub([]string{"channel", "message"}, "key", "olddata", "newdata")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEPubKeyDoesntExists(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETIEMPUB", "key", "newdata", "olddata", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, nil))
	result, err := db.SetIEPub([]string{"channel", "message"}, "key", "olddata", "newdata")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEPubFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETIEMPUB", "key", "newdata", "olddata", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, errors.New("Some error")))
	result, err := db.SetIEPub([]string{"channel", "message"}, "key", "olddata", "newdata")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetIEPubCommandMissing(t *testing.T) {
	_, r, db := setupHaEnv(false)
	expectedMessage := []interface{}{"SETIEMPUB", "key", "newdata", "olddata", "channel", "message"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.SetIEPub([]string{"channel", "message"}, "key", "olddata", "newdata")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXPubKeyDoesntExist(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETNXMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult("OK", nil))
	result, err := db.SetNXPub([]string{"channel", "message"}, "key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXPubKeyExists(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETNXMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, nil))
	result, err := db.SetNXPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXPubFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"SETNXMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(nil, errors.New("Some error")))
	result, err := db.SetNXPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXPubCommandMissing(t *testing.T) {
	_, r, db := setupHaEnv(false)
	expectedMessage := []interface{}{"SETNXMPUB", "key", "data", "channel", "message"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.SetNXPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SetNX", expectedKey, expectedData, time.Duration(0)).Return(redis.NewBoolResult(true, nil))
	result, err := db.SetNX("key", "data", 0)
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXUnsuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SetNX", expectedKey, expectedData, time.Duration(0)).Return(redis.NewBoolResult(false, nil))
	result, err := db.SetNX("key", "data", 0)
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSetNXFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
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
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(0), nil))
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubKeyExists(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(1), nil))
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubKeyExistsIntTypeRedisValue(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(1, nil))
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(0), errors.New("Some error")))
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEPubCommandMissing(t *testing.T) {
	_, r, db := setupHaEnv(false)
	expectedMessage := []interface{}{"DELIEMPUB", "key", "data", "channel", "message"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.DelIEPub([]string{"channel", "message"}, "key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEKeyDoesntExist(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(0), nil))
	result, err := db.DelIE("key", "data")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEKeyExists(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(1), nil))
	result, err := db.DelIE("key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEKeyExistsIntTypeRedisValue(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(1, nil))
	result, err := db.DelIE("key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestDelIEFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.On("Do", expectedMessage).Return(redis.NewCmdResult(int64(0), errors.New("Some error")))
	result, err := db.DelIE("key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestDelIECommandMissing(t *testing.T) {
	_, r, db := setupHaEnv(false)
	expectedMessage := []interface{}{"DELIE", "key", "data"}
	r.AssertNotCalled(t, "Do", expectedMessage)
	result, err := db.DelIE("key", "data")
	assert.False(t, result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSAddSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedData := []interface{}{"data", 2}
	r.On("SAdd", expectedKey, expectedData).Return(redis.NewIntResult(2, nil))
	assert.Nil(t, db.SAdd("key", "data", 2))
	r.AssertExpectations(t)
}

func TestSAddFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedData := []interface{}{"data", 2}
	r.On("SAdd", expectedKey, expectedData).Return(redis.NewIntResult(2, errors.New("Some error")))
	assert.NotNil(t, db.SAdd("key", "data", 2))
	r.AssertExpectations(t)
}

func TestSRemSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedData := []interface{}{"data", 2}
	r.On("SRem", expectedKey, expectedData).Return(redis.NewIntResult(2, nil))
	assert.Nil(t, db.SRem("key", "data", 2))
	r.AssertExpectations(t)
}

func TestSRemFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedData := []interface{}{"data", 2}
	r.On("SRem", expectedKey, expectedData).Return(redis.NewIntResult(2, errors.New("Some error")))
	assert.NotNil(t, db.SRem("key", "data", 2))
	r.AssertExpectations(t)
}

func TestSMembersSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedResult := []string{"member1", "member2"}
	r.On("SMembers", expectedKey).Return(redis.NewStringSliceResult(expectedResult, nil))
	result, err := db.SMembers("key")
	assert.Equal(t, result, expectedResult)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSMembersFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
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
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SIsMember", expectedKey, expectedData).Return(redis.NewBoolResult(true, nil))
	result, err := db.SIsMember("key", "data")
	assert.True(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSIsMemberIsNotMember(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	expectedData := "data"
	r.On("SIsMember", expectedKey, expectedData).Return(redis.NewBoolResult(false, nil))
	result, err := db.SIsMember("key", "data")
	assert.False(t, result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSIsMemberFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
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
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	r.On("SCard", expectedKey).Return(redis.NewIntResult(1, nil))
	result, err := db.SCard("key")
	assert.Equal(t, int64(1), result)
	assert.Nil(t, err)
	r.AssertExpectations(t)
}

func TestSCardFailure(t *testing.T) {
	_, r, db := setupHaEnv(true)
	expectedKey := "key"
	r.On("SCard", expectedKey).Return(redis.NewIntResult(1, errors.New("Some error")))
	result, err := db.SCard("key")
	assert.Equal(t, int64(1), result)
	assert.NotNil(t, err)
	r.AssertExpectations(t)
}

func TestSubscribeChannelDBSubscribeRXUnsubscribe(t *testing.T) {
	ps, r, db := setupHaEnv(true)
	ch := make(chan *redis.Message)
	msg := redis.Message{
		Channel: "{prefix},channel",
		Pattern: "pattern",
		Payload: "event",
	}
	ps.On("Channel").Return(ch)
	ps.On("Subscribe").Return(nil)
	ps.On("Unsubscribe").Return(nil)
	ps.On("Close").Return(nil)
	count := 0
	receivedChannel := ""
	err := db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel = channel
	}, "{prefix},channel")
	assert.Nil(t, err)
	ch <- &msg
	err = db.UnsubscribeChannelDB("{prefix},channel")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	assert.Equal(t, 1, count)
	assert.Equal(t, "channel", receivedChannel)
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestSubscribeChannelDBFailure(t *testing.T) {
	mockedErr := errors.New("Some DB Backend Subscribe Error")
	ps, r, db := setupHaEnv(true)
	ch := make(chan *redis.Message)
	ps.On("Channel").Return(ch)
	ps.On("Subscribe").Return(mockedErr)
	err := db.SubscribeChannelDB(func(channel string, payload ...string) {
	}, "{prefix},channel")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), mockedErr.Error())
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestUnsubscribeChannelDBFailure(t *testing.T) {
	mockedErr := errors.New("Some DB Backend Unsubscribe Error")
	ps, r, db := setupHaEnv(true)
	ch := make(chan *redis.Message)
	ps.On("Channel").Return(ch)
	ps.On("Subscribe").Return(nil)
	ps.On("Unsubscribe").Return(mockedErr)
	err := db.SubscribeChannelDB(func(channel string, payload ...string) {
	}, "{prefix},channel")
	assert.Nil(t, err)
	err = db.UnsubscribeChannelDB("{prefix},channel")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), mockedErr.Error())
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestSubscribeChannelDBSubscribeTwoUnsubscribeOne(t *testing.T) {
	ps, r, db := setupHaEnv(true)
	ch := make(chan *redis.Message)
	msg1 := redis.Message{
		Channel: "{prefix},channel1",
		Pattern: "pattern",
		Payload: "event",
	}
	msg2 := redis.Message{
		Channel: "{prefix},channel2",
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
	err := db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel1 = channel
	}, "{prefix},channel1")
	assert.Nil(t, err)
	ch <- &msg1
	receivedChannel2 := ""
	err = db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel2 = channel
	}, "{prefix},channel2")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	err = db.UnsubscribeChannelDB("{prefix},channel1")
	assert.Nil(t, err)
	ch <- &msg2
	err = db.UnsubscribeChannelDB("{prefix},channel2")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	assert.Equal(t, 2, count)
	assert.Equal(t, "channel1", receivedChannel1)
	assert.Equal(t, "channel2", receivedChannel2)
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestSubscribeChannelDBTwoSubscribesWithUnequalPrefixAndUnsubscribes(t *testing.T) {
	ps, r, db := setupHaEnv(true)
	ch := make(chan *redis.Message)
	msg1 := redis.Message{
		Channel: "{prefix1},channel",
		Pattern: "pattern",
		Payload: "event",
	}
	msg2 := redis.Message{
		Channel: "{prefix2},channel",
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
	err := db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel1 = channel
	}, "{prefix1},channel")
	assert.Nil(t, err)
	ch <- &msg1
	receivedChannel2 := ""
	err = db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel2 = channel
	}, "{prefix2},channel")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	err = db.UnsubscribeChannelDB("{prefix1},channel")
	assert.Nil(t, err)
	ch <- &msg2
	err = db.UnsubscribeChannelDB("{prefix2},channel")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	assert.Equal(t, 2, count)
	assert.Equal(t, "channel", receivedChannel1)
	assert.Equal(t, "channel", receivedChannel2)
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestSubscribeChannelReDBSubscribeAfterUnsubscribe(t *testing.T) {
	ps, r, db := setupHaEnv(true)
	ch := make(chan *redis.Message)
	msg := redis.Message{
		Channel: "{prefix},channel",
		Pattern: "pattern",
		Payload: "event",
	}
	ps.On("Channel").Return(ch)
	ps.On("Subscribe").Return(nil)
	ps.On("Unsubscribe").Return(nil)
	ps.On("Close").Return(nil)
	count := 0
	receivedChannel := ""

	err := db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel = channel
	}, "{prefix},channel")
	assert.Nil(t, err)
	ch <- &msg
	err = db.UnsubscribeChannelDB("{prefix},channel")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)

	err = db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel = channel
	}, "{prefix}", "---", "{prefix},channel")
	assert.Nil(t, err)
	ch <- &msg
	err = db.UnsubscribeChannelDB("{prefix},channel")
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)
	assert.Equal(t, 2, count)
	assert.Equal(t, "channel", receivedChannel)
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestSubscribeChannelDBSubscribeReceivedEventIgnoredIfChannelNameIsUnknown(t *testing.T) {
	ps, r, db := setupHaEnv(true)
	ch := make(chan *redis.Message)
	msg := redis.Message{
		Channel: "missingNsPrefixchannel",
		Pattern: "pattern",
		Payload: "event",
	}
	ps.On("Channel").Return(ch)
	ps.On("Subscribe").Return(nil)
	ps.On("Unsubscribe").Return(nil)
	ps.On("Close").Return(nil)
	count := 0
	receivedChannel := ""
	err := db.SubscribeChannelDB(func(channel string, payload ...string) {
		count++
		receivedChannel = channel
	}, "{prefix},channel")
	assert.Nil(t, err)
	ch <- &msg
	err = db.UnsubscribeChannelDB("{prefix},channel")
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	assert.Equal(t, 0, count)
	assert.Equal(t, "", receivedChannel)
	r.AssertExpectations(t)
	ps.AssertExpectations(t)
}

func TestPTTLSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
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
	_, r, db := setupHaEnv(true)
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
	_, r, db := setupHaEnv(true)
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
	_, r, db := setupHaEnv(true)
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
	_, r, db := setupHaEnv(true)
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
	setupVals := setupEnv(
		true,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6379", "", "", "", "",
	)
	assert.Equal(t, 1, len(setupVals.rClient))
	assert.Equal(t, 1, len(setupVals.db))

	expectedKeysAndValues := []interface{}{"key1", "value1"}
	setupVals.rClient[0].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := setupVals.db[0].MSet("key1", "value1")
	assert.Nil(t, err)
	setupVals.rClient[0].AssertExpectations(t)
}

func TestClientSentinelRedisLegacyEnv(t *testing.T) {
	setupVals := setupEnv(
		true,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6379", "dbaasmaster-cluster-0", "26379", "", "3",
	)
	assert.Equal(t, 1, len(setupVals.rClient))
	assert.Equal(t, 1, len(setupVals.db))

	expectedKeysAndValues := []interface{}{"key1", "value1"}
	setupVals.rClient[0].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := setupVals.db[0].MSet("key1", "value1")
	assert.Nil(t, err)
	setupVals.rClient[0].AssertExpectations(t)
}

func TestClientTwoStandaloneRedisEnvs(t *testing.T) {
	setupVals := setupEnv(
		true,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6379,6380", "", "",
		"service-ricplt-dbaas-tcp-cluster-0.ricplt,service-ricplt-dbaas-tcp-cluster-1.ricplt", "",
	)
	assert.Equal(t, 2, len(setupVals.rClient))
	assert.Equal(t, 2, len(setupVals.db))

	expectedKeysAndValues := []interface{}{"key1", "value1"}
	setupVals.rClient[0].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := setupVals.db[0].MSet("key1", "value1")
	assert.Nil(t, err)
	setupVals.rClient[0].AssertExpectations(t)

	expectedKeysAndValues = []interface{}{"key2", "value2"}
	setupVals.rClient[1].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err = setupVals.db[1].MSet("key2", "value2")
	assert.Nil(t, err)
	setupVals.rClient[0].AssertExpectations(t)
	setupVals.rClient[1].AssertExpectations(t)
}

func TestClientTwoSentinelRedisEnvs(t *testing.T) {
	setupVals := setupEnv(
		true,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6379,6380", "dbaasmaster-cluster-0,dbaasmaster-cluster-1",
		"26379,26380", "service-ricplt-dbaas-tcp-cluster-0.ricplt,service-ricplt-dbaas-tcp-cluster-1.ricplt", "3",
	)
	assert.Equal(t, 2, len(setupVals.rClient))
	assert.Equal(t, 2, len(setupVals.db))

	expectedKeysAndValues := []interface{}{"key1", "value1"}
	setupVals.rClient[0].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err := setupVals.db[0].MSet("key1", "value1")
	assert.Nil(t, err)
	setupVals.rClient[0].AssertExpectations(t)

	expectedKeysAndValues = []interface{}{"key2", "value2"}
	setupVals.rClient[1].On("MSet", expectedKeysAndValues).Return(redis.NewStatusResult("OK", nil))
	err = setupVals.db[1].MSet("key2", "value2")
	assert.Nil(t, err)
	setupVals.rClient[0].AssertExpectations(t)
	setupVals.rClient[1].AssertExpectations(t)
}

func TestCompleteConfigIfLessPortsThanAddresses(t *testing.T) {
	setupVals := setupEnv(
		true,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6379", "dbaasmaster-cluster-0,dbaasmaster-cluster-1",
		"", "service-ricplt-dbaas-tcp-cluster-0.ricplt,service-ricplt-dbaas-tcp-cluster-1.ricplt", "3",
	)
	assert.Equal(t, 2, len(setupVals.rClient))
	assert.Equal(t, 2, len(setupVals.db))
}

func TestCompleteConfigIfLessSentinelPortsThanAddresses(t *testing.T) {
	setupVals := setupEnv(
		true,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6379,6380", "dbaasmaster-cluster-0,dbaasmaster-cluster-1",
		"26379", "service-ricplt-dbaas-tcp-cluster-0.ricplt,service-ricplt-dbaas-tcp-cluster-1.ricplt", "3",
	)
	assert.Equal(t, 2, len(setupVals.rClient))
	assert.Equal(t, 2, len(setupVals.db))
}

func TestCompleteConfigIfLessSentinelNamesThanAddresses(t *testing.T) {
	setupVals := setupEnv(
		true,
		"service-ricplt-dbaas-tcp-cluster-0.ricplt", "6379,6380", "dbaasmaster-cluster-0",
		"26379,26380", "service-ricplt-dbaas-tcp-cluster-0.ricplt,service-ricplt-dbaas-tcp-cluster-1.ricplt", "3",
	)
	assert.Equal(t, 2, len(setupVals.rClient))
	assert.Equal(t, 2, len(setupVals.db))
}

func TestInfoOfPrimaryRedisWithTwoReplicasSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:2\r\n" +
		"min_slaves_good_slaves:2\r\n" +
		"slave0:ip=1.2.3.4,port=6379,state=online,offset=100200300,lag=0\r\n" +
		"slave1:ip=5.6.7.8,port=6379,state=online,offset=100200300,lag=0\r\n"
	expInfo := &sdlgoredis.DbInfo{
		Fields: sdlgoredis.DbInfoFields{
			PrimaryRole:         true,
			ConnectedReplicaCnt: 2,
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoOfPrimaryRedisWithOneReplicaOnlineAndOtherReplicaNotOnlineSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:1\r\n" +
		"min_slaves_good_slaves:2\r\n" +
		"slave0:ip=1.2.3.4,port=6379,state=online,offset=100200300,lag=0\r\n" +
		"slave1:ip=5.6.7.8,port=6379,state=wait_bgsave,offset=100200300,lag=0\r\n"
	expInfo := &sdlgoredis.DbInfo{
		Fields: sdlgoredis.DbInfoFields{
			PrimaryRole:         true,
			ConnectedReplicaCnt: 1,
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoOfStandalonePrimaryRedisSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:0\r\n" +
		"min_slaves_good_slaves:0\r\n"
	expInfo := &sdlgoredis.DbInfo{
		Fields: sdlgoredis.DbInfoFields{
			PrimaryRole:         true,
			ConnectedReplicaCnt: 0,
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoOfStandalonePrimaryRedisFailureWhenIntConversionFails(t *testing.T) {
	_, r, db := setupHaEnv(true)
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:not-int\r\n" +
		"min_slaves_good_slaves:0\r\n"
	expInfo := &sdlgoredis.DbInfo{
		Fields: sdlgoredis.DbInfoFields{
			PrimaryRole:         true,
			ConnectedReplicaCnt: 0,
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoWithGibberishContentSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	redisInfo := "!#¤%&?+?´-\r\n"
	expInfo := &sdlgoredis.DbInfo{}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoWithEmptyContentSuccessfully(t *testing.T) {
	_, r, db := setupHaEnv(true)
	var redisInfo string
	expInfo := &sdlgoredis.DbInfo{
		Fields: sdlgoredis.DbInfoFields{
			PrimaryRole: false,
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestInfoWithSomeStatisticsOfStandalonePrimaryRedis(t *testing.T) {
	_, r, db := setupHaEnv(true)
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:0\r\n" +
		"min_slaves_good_slaves:0\r\n" +
		"# Server\r\n" +
		"uptime_in_days:23\r\n" +
		"# Clients\r\n" +
		"connected_clients:2\r\n" +
		"# Memory\r\n" +
		"used_memory:2093808\r\n" +
		"used_memory_human:2.00M\r\n" +
		"mem_fragmentation_ratio:6.44\r\n" +
		"# Stats\r\n" +
		"total_connections_received:278\r\n" +
		"# CPU\r\n" +
		"used_cpu_sys:1775.254919\r\n" +
		"# Commandstats\r\n" +
		"cmdstat_role:calls=1,usec=3,usec_per_call=3.00\r\n" +
		"# Keyspace\r\n" +
		"db0:keys=4,expires=0,avg_ttl=0"
	expInfo := &sdlgoredis.DbInfo{
		Fields: sdlgoredis.DbInfoFields{
			PrimaryRole:         true,
			ConnectedReplicaCnt: 0,
			Server: sdlgoredis.ServerInfoFields{
				UptimeInDays: 23,
			},
			Clients: sdlgoredis.ClientsInfoFields{
				ConnectedClients: 2,
			},
			Memory: sdlgoredis.MeroryInfoFields{
				UsedMemory:            2093808,
				UsedMemoryHuman:       "2.00M",
				MemFragmentationRatio: 6.44,
			},
			Stats: sdlgoredis.StatsInfoFields{
				TotalConnectionsReceived: 278,
			},
			Cpu: sdlgoredis.CpuInfoFields{
				UsedCpuSys: 1775.254919,
			},
			Commandstats: sdlgoredis.CommandstatsInfoFields{
				CmdstatRole: sdlgoredis.CommandstatsValues{
					Calls:       1,
					Usec:        3,
					UsecPerCall: 3.00,
				},
			},
			Keyspace: sdlgoredis.KeyspaceInfoFields{
				Db: sdlgoredis.KeyspaceValues{
					Keys: 4,
				},
			},
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	info, err := db.Info()
	assert.Nil(t, err)
	assert.Equal(t, expInfo, info)
	r.AssertExpectations(t)
}

func TestStateWithPrimaryAndTwoReplicaRedisSuccessfully(t *testing.T) {
	_, r, s, db := setupHaEnvWithSentinels(true, "3")

	redisPrimaryState := newMockRedisMasterCallResp("master", "10.20.30.30", "6379", "master")
	redisReplicasState := newMockRedisSlavesCall()
	redisReplicasState.add("slave", "10.20.30.40", "6379", "up", "slave")
	redisReplicasState.add("slave", "10.20.30.50", "30000", "up", "slave")
	redisSentinelsState := newMockRedisSentinelsCall()
	redisSentinelsState.add("10.20.30.40", "26379", "sentinel")
	redisSentinelsState.add("10.20.30.50", "30001", "sentinel")

	expState := newExpDbState(3, nil)
	expState.addPrimary("master", "10.20.30.30", "6379", "master", nil)
	expState.addReplica("slave", "10.20.30.40", "6379", "up", "slave", nil)
	expState.addReplica("slave", "10.20.30.50", "30000", "up", "slave", nil)
	expState.addSentinel("10.20.30.40", "26379", "sentinel", nil)
	expState.addSentinel("10.20.30.50", "30001", "sentinel", nil)

	s[0].On("Master", "dbaasmaster-cluster-0").Return(redis.NewStringStringMapResult(redisPrimaryState, nil))
	s[0].On("Slaves", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisReplicasState.resp, nil))
	s[0].On("Sentinels", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisSentinelsState.resp, nil))
	ret, err := db.State()
	assert.Nil(t, err)
	assert.Equal(t, expState.s, *ret)
	r.AssertExpectations(t)
}

func TestStateWithTwoSdlClustersContainingPrimaryAndTwoReplicaRedisSuccessfully(t *testing.T) {
	setupVals := setupEnv(
		true,
		"", "6379,6380", "dbaasmaster-cluster-0,dbaasmaster-cluster-1",
		"26379,26380", "service-ricplt-dbaas-tcp-cluster-0.ricplt,service-ricplt-dbaas-tcp-cluster-1.ricplt", "3",
	)
	r := setupVals.rClient
	s := setupVals.rSentinel
	db := setupVals.db

	FstRedisPrimaryState := newMockRedisMasterCallResp("master", "10.20.30.30", "6379", "master")
	FstRedisReplicasState := newMockRedisSlavesCall()
	FstRedisReplicasState.add("slave", "10.20.30.40", "6379", "up", "slave")
	FstRedisReplicasState.add("slave", "10.20.30.50", "6379", "up", "slave")
	FstRedisSentinelsState := newMockRedisSentinelsCall()
	FstRedisSentinelsState.add("10.20.30.40", "26379", "sentinel")
	FstRedisSentinelsState.add("10.20.30.50", "26379", "sentinel")

	SndRedisPrimaryState := newMockRedisMasterCallResp("master", "10.20.30.60", "6380", "master")
	SndRedisReplicasState := newMockRedisSlavesCall()
	SndRedisReplicasState.add("slave", "10.20.30.70", "6380", "up", "slave")
	SndRedisReplicasState.add("slave", "10.20.30.80", "6380", "up", "slave")
	SndRedisSentinelsState := newMockRedisSentinelsCall()
	SndRedisSentinelsState.add("10.20.30.70", "26380", "sentinel")
	SndRedisSentinelsState.add("10.20.30.80", "26380", "sentinel")

	FstExpState := newExpDbState(3, nil)
	FstExpState.addPrimary("master", "10.20.30.30", "6379", "master", nil)
	FstExpState.addReplica("slave", "10.20.30.40", "6379", "up", "slave", nil)
	FstExpState.addReplica("slave", "10.20.30.50", "6379", "up", "slave", nil)
	FstExpState.addSentinel("10.20.30.40", "26379", "sentinel", nil)
	FstExpState.addSentinel("10.20.30.50", "26379", "sentinel", nil)

	SndExpState := newExpDbState(3, nil)
	SndExpState.addPrimary("master", "10.20.30.60", "6380", "master", nil)
	SndExpState.addReplica("slave", "10.20.30.70", "6380", "up", "slave", nil)
	SndExpState.addReplica("slave", "10.20.30.80", "6380", "up", "slave", nil)
	SndExpState.addSentinel("10.20.30.70", "26380", "sentinel", nil)
	SndExpState.addSentinel("10.20.30.80", "26380", "sentinel", nil)

	s[0].On("Master", "dbaasmaster-cluster-0").Return(redis.NewStringStringMapResult(FstRedisPrimaryState, nil))
	s[0].On("Slaves", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(FstRedisReplicasState.resp, nil))
	s[0].On("Sentinels", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(FstRedisSentinelsState.resp, nil))

	s[0].On("Master", "dbaasmaster-cluster-1").Return(redis.NewStringStringMapResult(SndRedisPrimaryState, nil))
	s[0].On("Slaves", "dbaasmaster-cluster-1").Return(redis.NewSliceResult(SndRedisReplicasState.resp, nil))
	s[0].On("Sentinels", "dbaasmaster-cluster-1").Return(redis.NewSliceResult(SndRedisSentinelsState.resp, nil))

	ret, err := db[0].State()
	assert.Nil(t, err)
	assert.Equal(t, FstExpState.s, *ret)

	ret, err = db[1].State()
	assert.Nil(t, err)
	assert.Equal(t, SndExpState.s, *ret)
	r[0].AssertExpectations(t)
}

func TestStateWithPrimaryAndTwoReplicaRedisFailureInPrimaryRedisCall(t *testing.T) {
	expErr := errors.New("Some error")
	_, r, s, db := setupHaEnvWithSentinels(true, "3")

	redisPrimaryState := newMockRedisMasterCallResp("master", "10.20.30.30", "6379", "master")
	redisReplicasState := newMockRedisSlavesCall()
	redisReplicasState.add("slave", "10.20.30.40", "6379", "up", "slave")
	redisReplicasState.add("slave", "10.20.30.50", "30000", "up", "slave")
	redisSentinelsState := newMockRedisSentinelsCall()
	redisSentinelsState.add("10.20.30.40", "26379", "sentinel")
	redisSentinelsState.add("10.20.30.50", "30001", "sentinel")

	expState := newExpDbState(3, nil)
	expState.addPrimary("", "", "", "", expErr)
	expState.addReplica("slave", "10.20.30.40", "6379", "up", "slave", nil)
	expState.addReplica("slave", "10.20.30.50", "30000", "up", "slave", nil)
	expState.addSentinel("10.20.30.40", "26379", "sentinel", nil)
	expState.addSentinel("10.20.30.50", "30001", "sentinel", nil)

	s[0].On("Master", "dbaasmaster-cluster-0").Return(redis.NewStringStringMapResult(redisPrimaryState, expErr))
	s[0].On("Slaves", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisReplicasState.resp, nil))
	s[0].On("Sentinels", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisSentinelsState.resp, nil))
	ret, err := db.State()
	assert.NotNil(t, err)
	assert.Equal(t, expState.s, *ret)
	r.AssertExpectations(t)
}

func TestStateWithPrimaryAndTwoReplicaRedisFailureInReplicasRedisCall(t *testing.T) {
	expErr := errors.New("Some error")
	_, r, s, db := setupHaEnvWithSentinels(true, "3")

	redisPrimaryState := newMockRedisMasterCallResp("master", "10.20.30.30", "6379", "master")
	redisReplicasState := newMockRedisSlavesCall()
	redisReplicasState.add("slave", "10.20.30.40", "6379", "up", "slave")
	redisReplicasState.add("slave", "10.20.30.50", "30000", "up", "slave")
	redisSentinelsState := newMockRedisSentinelsCall()
	redisSentinelsState.add("10.20.30.40", "26379", "sentinel")
	redisSentinelsState.add("10.20.30.50", "30001", "sentinel")

	expState := newExpDbState(3, nil)
	expState.addPrimary("master", "10.20.30.30", "6379", "master", nil)
	expState.addReplica("", "", "", "", "", expErr)
	expState.addReplica("", "", "", "", "", expErr)
	expState.addSentinel("10.20.30.40", "26379", "sentinel", nil)
	expState.addSentinel("10.20.30.50", "30001", "sentinel", nil)

	s[0].On("Master", "dbaasmaster-cluster-0").Return(redis.NewStringStringMapResult(redisPrimaryState, nil))
	s[0].On("Slaves", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisReplicasState.resp, errors.New("Some error")))
	s[0].On("Sentinels", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisSentinelsState.resp, nil))
	ret, err := db.State()
	assert.NotNil(t, err)
	assert.Equal(t, expState.s, *ret)
	r.AssertExpectations(t)
}

func TestStateWithPrimaryAndOneReplicaRedisFailureInSentinelsRedisCall(t *testing.T) {
	expErr := errors.New("Some error")
	_, r, s, db := setupHaEnvWithSentinels(true, "2")

	redisPrimaryState := newMockRedisMasterCallResp("master", "10.20.30.30", "6379", "master")
	redisReplicasState := newMockRedisSlavesCall()
	redisReplicasState.add("slave", "10.20.30.40", "6379", "up", "slave")
	redisSentinelsState := newMockRedisSentinelsCall()
	redisSentinelsState.add("10.20.30.40", "26379", "sentinel")

	expState := newExpDbState(2, nil)
	expState.addPrimary("master", "10.20.30.30", "6379", "master", nil)
	expState.addReplica("slave", "10.20.30.40", "6379", "up", "slave", nil)
	expState.addSentinel("", "", "", expErr)

	s[0].On("Master", "dbaasmaster-cluster-0").Return(redis.NewStringStringMapResult(redisPrimaryState, nil))
	s[0].On("Slaves", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisReplicasState.resp, nil))
	s[0].On("Sentinels", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisSentinelsState.resp, expErr))
	ret, err := db.State()
	assert.NotNil(t, err)
	assert.Equal(t, expState.s, *ret)
	r.AssertExpectations(t)
}

func TestStateWithPrimaryAndTwoReplicaRedisFailureWhenIntConversionFails(t *testing.T) {
	expErr := errors.New("Sentinel DBAAS_NODE_COUNT configuration value 'no-int' conversion to integer failed")
	_, r, s, db := setupHaEnvWithSentinels(true, "no-int")

	redisPrimaryState := newMockRedisMasterCallResp("master", "10.20.30.30", "6379", "master")
	redisReplicasState := newMockRedisSlavesCall()
	redisReplicasState.add("slave", "10.20.30.40", "6379", "up", "slave")
	redisReplicasState.add("slave", "10.20.30.50", "30000", "up", "slave")
	redisSentinelsState := newMockRedisSentinelsCall()
	redisSentinelsState.add("10.20.30.40", "26379", "sentinel")
	redisSentinelsState.add("10.20.30.50", "30001", "sentinel")

	expState := newExpDbState(0, expErr)
	expState.addPrimary("master", "10.20.30.30", "6379", "master", nil)
	expState.addReplica("slave", "10.20.30.40", "6379", "up", "slave", nil)
	expState.addReplica("slave", "10.20.30.50", "30000", "up", "slave", nil)
	expState.addSentinel("10.20.30.40", "26379", "sentinel", nil)
	expState.addSentinel("10.20.30.50", "30001", "sentinel", nil)

	s[0].On("Master", "dbaasmaster-cluster-0").Return(redis.NewStringStringMapResult(redisPrimaryState, nil))
	s[0].On("Slaves", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisReplicasState.resp, nil))
	s[0].On("Sentinels", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisSentinelsState.resp, nil))
	ret, err := db.State()
	assert.Equal(t, expErr, err)
	assert.Equal(t, expState.s, *ret)
	r.AssertExpectations(t)
}

// Test case to test ignoring of a sentinel entry with zero port. Implementation has been
// done because we miss the fix for the Redis Bug #9240.
func TestStateWithPrimaryAndTwoReplicaFirstSentinelStateIgnoredBecauseZeroPortBugRedisSuccessfully(t *testing.T) {
	_, r, s, db := setupHaEnvWithSentinels(true, "3")

	redisPrimaryState := newMockRedisMasterCallResp("master", "10.20.30.30", "6379", "master")
	redisReplicasState := newMockRedisSlavesCall()
	redisReplicasState.add("slave", "10.20.30.40", "6379", "up", "slave")
	redisReplicasState.add("slave", "10.20.30.50", "30000", "up", "slave")
	redisSentinelsState := newMockRedisSentinelsCall()
	redisSentinelsState.add("10.20.30.40", "0", "s_down,sentinel,disconnected")
	redisSentinelsState.add("10.20.30.50", "26379", "sentinel")

	expState := newExpDbState(3, nil)
	expState.addPrimary("master", "10.20.30.30", "6379", "master", nil)
	expState.addReplica("slave", "10.20.30.40", "6379", "up", "slave", nil)
	expState.addReplica("slave", "10.20.30.50", "30000", "up", "slave", nil)
	expState.addSentinel("10.20.30.50", "26379", "sentinel", nil)

	s[0].On("Master", "dbaasmaster-cluster-0").Return(redis.NewStringStringMapResult(redisPrimaryState, nil))
	s[0].On("Slaves", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisReplicasState.resp, nil))
	s[0].On("Sentinels", "dbaasmaster-cluster-0").Return(redis.NewSliceResult(redisSentinelsState.resp, nil))
	ret, err := db.State()
	assert.Nil(t, err)
	assert.Equal(t, expState.s, *ret)
	r.AssertExpectations(t)
}

func TestStateWithSinglePrimaryRedisSuccessfully(t *testing.T) {
	_, r, db := setupSingleEnv(true, "1")
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:0\r\n" +
		"min_slaves_good_slaves:0\r\n"

	expState := &sdlgoredis.DbState{
		ConfigNodeCnt: 1,
		PrimaryDbState: sdlgoredis.PrimaryDbState{
			Fields: sdlgoredis.PrimaryDbStateFields{
				Role:  "master",
				Ip:    "service-ricplt-dbaas-tcp-cluster-0.ricplt",
				Port:  "6379",
				Flags: "master",
			},
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	ret, err := db.State()
	assert.Nil(t, err)
	assert.Equal(t, expState, ret)
	r.AssertExpectations(t)
}

func TestStateWithSinglePrimaryRedisFailureWhenIntConversionFails(t *testing.T) {
	expErr := errors.New("DBAAS_NODE_COUNT configuration value 'no-int' conversion to integer failed")
	_, r, db := setupSingleEnv(true, "no-int")
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:0\r\n" +
		"min_slaves_good_slaves:0\r\n"

	expState := &sdlgoredis.DbState{
		Err:           expErr,
		ConfigNodeCnt: 0,
		PrimaryDbState: sdlgoredis.PrimaryDbState{
			Fields: sdlgoredis.PrimaryDbStateFields{
				Role:  "master",
				Ip:    "service-ricplt-dbaas-tcp-cluster-0.ricplt",
				Port:  "6379",
				Flags: "master",
			},
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	ret, err := db.State()
	assert.Equal(t, expErr, err)
	assert.Equal(t, expState, ret)
	r.AssertExpectations(t)
}

func TestStateWithSinglePrimaryRedisFailureInInfoCall(t *testing.T) {
	expErr := errors.New("Some error")
	_, r, db := setupSingleEnv(true, "1")
	redisInfo := ""
	expState := &sdlgoredis.DbState{
		PrimaryDbState: sdlgoredis.PrimaryDbState{
			Err: expErr,
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, errors.New("Some error")))
	ret, err := db.State()
	assert.NotNil(t, err)
	assert.Equal(t, expState, ret)
	r.AssertExpectations(t)
}

func TestStatisticsWithSinglePrimaryRedisSuccessfully(t *testing.T) {
	_, r, db := setupSingleEnv(true, "1")
	redisInfo := "# Replication\r\n" +
		"role:master\r\n" +
		"connected_slaves:0\r\n" +
		"min_slaves_good_slaves:0\r\n" +
		"# Server\r\n" +
		"uptime_in_days:12\r\n"

	expStatistics := &sdlgoredis.DbStatistics{
		Stats: []*sdlgoredis.DbStatisticsInfo{
			{
				IPAddr: "service-ricplt-dbaas-tcp-cluster-0.ricplt",
				Port:   "6379",
				Info: &sdlgoredis.DbInfo{
					Fields: sdlgoredis.DbInfoFields{
						PrimaryRole: true,
						Server: sdlgoredis.ServerInfoFields{
							UptimeInDays: 12,
						},
					},
				},
			},
		},
	}

	r.On("Info", []string{"all"}).Return(redis.NewStringResult(redisInfo, nil))
	ret, err := db.Statistics()
	assert.Nil(t, err)
	assert.Equal(t, expStatistics, ret)
	r.AssertExpectations(t)
}
