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

package sdlgo_test

import (
	"errors"
	"testing"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockDB struct {
	mock.Mock
}

func (m *mockDB) SubscribeChannelDB(cb sdlgoredis.ChannelNotificationCb, channelPrefix, eventSeparator string, channels ...string) {
	m.Called(cb, channelPrefix, eventSeparator, channels)
}

func (m *mockDB) UnsubscribeChannelDB(channels ...string) {
	m.Called(channels)
}

func (m *mockDB) MSet(pairs ...interface{}) error {
	a := m.Called(pairs)
	return a.Error(0)
}

func (m *mockDB) MSetPub(ns, message string, pairs ...interface{}) error {
	a := m.Called(ns, message, pairs)
	return a.Error(0)
}

func (m *mockDB) MGet(keys []string) ([]interface{}, error) {
	a := m.Called(keys)
	return a.Get(0).([]interface{}), a.Error(1)
}

func (m *mockDB) CloseDB() error {
	a := m.Called()
	return a.Error(0)
}

func (m *mockDB) Del(keys []string) error {
	a := m.Called(keys)
	return a.Error(0)
}

func (m *mockDB) DelPub(channel, message string, keys []string) error {
	a := m.Called(channel, message, keys)
	return a.Error(0)
}

func (m *mockDB) Keys(pattern string) ([]string, error) {
	a := m.Called(pattern)
	return a.Get(0).([]string), a.Error(1)
}

func (m *mockDB) SetIE(key string, oldData, newData interface{}) (bool, error) {
	a := m.Called(key, oldData, newData)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) SetIEPub(channel, message, key string, oldData, newData interface{}) (bool, error) {
	a := m.Called(channel, message, key, oldData, newData)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) SetNX(key string, data interface{}) (bool, error) {
	a := m.Called(key, data)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) SetNXPub(channel, message, key string, data interface{}) (bool, error) {
	a := m.Called(channel, message, key, data)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) DelIE(key string, data interface{}) (bool, error) {
	a := m.Called(key, data)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) DelIEPub(channel, message, key string, data interface{}) (bool, error) {
	a := m.Called(channel, message, key, data)
	return a.Bool(0), a.Error(1)
}

func setup() (*mockDB, *sdlgo.SdlInstance) {
	m := new(mockDB)
	i := sdlgo.NewSdlInstance("namespace", m)
	return m, i
}

func TestSubscribeChannel(t *testing.T) {
	m, i := setup()

	expectedCB := func(channel string, events ...string) {}
	expectedChannels := []string{"{namespace},channel1", "{namespace},channel2"}

	m.On("SubscribeChannelDB", mock.AnythingOfType("sdlgoredis.ChannelNotificationCb"), "{namespace},", "___", expectedChannels).Return()
	i.SubscribeChannel(expectedCB, "channel1", "channel2")
	m.AssertExpectations(t)
}

func TestUnsubscribeChannel(t *testing.T) {
	m, i := setup()

	expectedChannels := []string{"{namespace},channel1", "{namespace},channel2"}

	m.On("UnsubscribeChannelDB", expectedChannels).Return()
	i.UnsubscribeChannel("channel1", "channel2")
	m.AssertExpectations(t)
}
func TestGetOneKey(t *testing.T) {
	m, i := setup()

	mgetExpected := []string{"{namespace},key"}
	mReturn := []interface{}{"somevalue"}
	mReturnExpected := make(map[string]interface{})
	mReturnExpected["key"] = "somevalue"

	m.On("MGet", mgetExpected).Return(mReturn, nil)
	retVal, err := i.Get([]string{"key"})
	assert.Nil(t, err)
	assert.Equal(t, mReturnExpected, retVal)
	m.AssertExpectations(t)
}

func TestGetSeveralKeys(t *testing.T) {
	m, i := setup()

	mgetExpected := []string{"{namespace},key1", "{namespace},key2", "{namespace},key3"}
	mReturn := []interface{}{"somevalue1", 2, "someothervalue"}
	mReturnExpected := make(map[string]interface{})
	mReturnExpected["key1"] = "somevalue1"
	mReturnExpected["key2"] = 2
	mReturnExpected["key3"] = "someothervalue"

	m.On("MGet", mgetExpected).Return(mReturn, nil)
	retVal, err := i.Get([]string{"key1", "key2", "key3"})
	assert.Nil(t, err)
	assert.Equal(t, mReturnExpected, retVal)
	m.AssertExpectations(t)
}

func TestGetSeveralKeysSomeFail(t *testing.T) {
	m, i := setup()

	mgetExpected := []string{"{namespace},key1", "{namespace},key2", "{namespace},key3"}
	mReturn := []interface{}{"somevalue1", nil, "someothervalue"}
	mReturnExpected := make(map[string]interface{})
	mReturnExpected["key1"] = "somevalue1"
	mReturnExpected["key2"] = nil
	mReturnExpected["key3"] = "someothervalue"

	m.On("MGet", mgetExpected).Return(mReturn, nil)
	retVal, err := i.Get([]string{"key1", "key2", "key3"})
	assert.Nil(t, err)
	assert.Equal(t, mReturnExpected, retVal)
	m.AssertExpectations(t)
}

func TestGetKeyReturnError(t *testing.T) {
	m, i := setup()

	mgetExpected := []string{"{namespace},key"}
	mReturn := []interface{}{nil}
	mReturnExpected := make(map[string]interface{})

	m.On("MGet", mgetExpected).Return(mReturn, errors.New("Some error"))
	retVal, err := i.Get([]string{"key"})
	assert.NotNil(t, err)
	assert.Equal(t, mReturnExpected, retVal)
	m.AssertExpectations(t)
}

func TestGetEmptyList(t *testing.T) {
	m, i := setup()

	mgetExpected := []string{}

	retval, err := i.Get([]string{})
	assert.Nil(t, err)
	assert.Len(t, retval, 0)
	m.AssertNotCalled(t, "MGet", mgetExpected)
}

func TestWriteOneKey(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", "data1"}

	m.On("MSet", msetExpected).Return(nil)
	err := i.Set("key1", "data1")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteSeveralKeysSlice(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", "data1", "{namespace},key2", 22}

	m.On("MSet", msetExpected).Return(nil)
	err := i.Set([]interface{}{"key1", "data1", "key2", 22})
	assert.Nil(t, err)
	m.AssertExpectations(t)

}

func TestWriteSeveralKeysArray(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", "data1", "{namespace},key2", "data2"}

	m.On("MSet", msetExpected).Return(nil)
	err := i.Set([4]string{"key1", "data1", "key2", "data2"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteFail(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", "data1"}

	m.On("MSet", msetExpected).Return(errors.New("Some error"))
	err := i.Set("key1", "data1")
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestWriteEmptyList(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{}
	err := i.Set()
	assert.Nil(t, err)
	m.AssertNotCalled(t, "MSet", msetExpected)
}

func TestWriteAndPublishOneKeyOneChannel(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedMessage := "event"
	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}

	m.On("MSetPub", expectedChannel, expectedMessage, expectedKeyVal).Return(nil)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event"}, "key1", "data1")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}
func TestWriteAndPublishOneKeyOneChannelTwoEvents(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedMessage := "event1___event2"
	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}

	m.On("MSetPub", expectedChannel, expectedMessage, expectedKeyVal).Return(nil)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event1", "channel", "event2"}, "key1", "data1")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishIncorrectChannelAndEvent(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedMessage := "event1___event2"
	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}
	m.AssertNotCalled(t, "MSetPub", expectedChannel, expectedMessage, expectedKeyVal)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event1", "channel"}, "key1", "data1")
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishNoData(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedMessage := "event"
	expectedKeyVal := []interface{}{"key"}

	m.AssertNotCalled(t, "MSetPub", expectedChannel, expectedMessage, expectedKeyVal)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event"}, []interface{}{"key"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishNoChannelEvent(t *testing.T) {
	m, i := setup()

	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}

	m.On("MSet", expectedKeyVal).Return(nil)
	m.AssertNotCalled(t, "MSetPub", "", "", expectedKeyVal)
	err := i.SetAndPublish([]string{}, "key1", "data1")
	assert.Nil(t, err)
	m.AssertExpectations(t)

}

func TestRemoveAndPublishSuccessfully(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKeys := []string{"{namespace},key1", "{namespace},key2"}

	m.On("DelPub", expectedChannel, expectedEvent, expectedKeys).Return(nil)
	err := i.RemoveAndPublish([]string{"channel", "event"}, []string{"key1", "key2"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}
func TestRemoveAndPublishFail(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKeys := []string{"{namespace},key1", "{namespace},key2"}

	m.On("DelPub", expectedChannel, expectedEvent, expectedKeys).Return(errors.New("Some error"))
	err := i.RemoveAndPublish([]string{"channel", "event"}, []string{"key1", "key2"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAndPublishNoChannels(t *testing.T) {
	m, i := setup()

	expectedKeys := []string{"{namespace},key1", "{namespace},key2"}

	m.On("Del", expectedKeys).Return(nil)
	err := i.RemoveAndPublish([]string{}, []string{"key1", "key2"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAndPublishIncorrectChannel(t *testing.T) {
	m, i := setup()

	notExpectedChannel := "{namespace},channel"
	notExpectedEvent := "event"
	notExpectedKeys := []string{"{namespace},key"}

	m.AssertNotCalled(t, "DelPub", notExpectedChannel, notExpectedEvent, notExpectedKeys)
	m.AssertNotCalled(t, "Del", notExpectedKeys)
	err := i.RemoveAndPublish([]string{"channel", "event", "channel2"}, []string{})
	assert.Nil(t, err)
	m.AssertExpectations(t)

}
func TestRemoveAndPublishNoKeys(t *testing.T) {
	m, i := setup()

	notExpectedChannel := "{namespace},channel"
	notExpectedEvent := "event"
	notExpectedKeys := []string{"{namespace},key"}

	m.AssertNotCalled(t, "DelPub", notExpectedChannel, notExpectedEvent, notExpectedKeys)
	m.AssertNotCalled(t, "Del", notExpectedKeys)
	err := i.RemoveAndPublish([]string{"channel", "event"}, []string{})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}
func TestRemoveAndPublishNoChannelsError(t *testing.T) {
	m, i := setup()

	expectedKeys := []string{"{namespace},key1", "{namespace},key2"}

	m.On("Del", expectedKeys).Return(errors.New("Some error"))
	err := i.RemoveAndPublish([]string{}, []string{"key1", "key2"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}
func TestRemoveSuccessfully(t *testing.T) {
	m, i := setup()

	msetExpected := []string{"{namespace},key1", "{namespace},key2"}
	m.On("Del", msetExpected).Return(nil)

	err := i.Remove([]string{"key1", "key2"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveFail(t *testing.T) {
	m, i := setup()

	msetExpected := []string{"{namespace},key"}
	m.On("Del", msetExpected).Return(errors.New("Some error"))

	err := i.Remove([]string{"key"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveEmptyList(t *testing.T) {
	m, i := setup()

	err := i.Remove([]string{})
	assert.Nil(t, err)
	m.AssertNotCalled(t, "Del", []string{})
}

func TestGetAllSuccessfully(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mReturnExpected := []string{"{namespace},key1", "{namespace},key2"}
	expectedReturn := []string{"key1", "key2"}
	m.On("Keys", mKeysExpected).Return(mReturnExpected, nil)
	retVal, err := i.GetAll()
	assert.Nil(t, err)
	assert.Equal(t, expectedReturn, retVal)
	m.AssertExpectations(t)
}

func TestGetAllFail(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mReturnExpected := []string{}
	m.On("Keys", mKeysExpected).Return(mReturnExpected, errors.New("some error"))
	retVal, err := i.GetAll()
	assert.NotNil(t, err)
	assert.Nil(t, retVal)
	assert.Equal(t, len(retVal), 0)
	m.AssertExpectations(t)
}

func TestGetAllReturnEmpty(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	var mReturnExpected []string = nil
	m.On("Keys", mKeysExpected).Return(mReturnExpected, nil)
	retVal, err := i.GetAll()
	assert.Nil(t, err)
	assert.Nil(t, retVal)
	assert.Equal(t, len(retVal), 0)
	m.AssertExpectations(t)

}

func TestRemoveAllSuccessfully(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mKeysReturn := []string{"{namespace},key1", "{namespace},key2"}
	mDelExpected := mKeysReturn
	m.On("Keys", mKeysExpected).Return(mKeysReturn, nil)
	m.On("Del", mDelExpected).Return(nil)
	err := i.RemoveAll()
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAllNoKeysFound(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	var mKeysReturn []string = nil
	m.On("Keys", mKeysExpected).Return(mKeysReturn, nil)
	m.AssertNumberOfCalls(t, "Del", 0)
	err := i.RemoveAll()
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAllKeysReturnError(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	var mKeysReturn []string = nil
	m.On("Keys", mKeysExpected).Return(mKeysReturn, errors.New("Some error"))
	m.AssertNumberOfCalls(t, "Del", 0)
	err := i.RemoveAll()
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAllDelReturnError(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mKeysReturn := []string{"{namespace},key1", "{namespace},key2"}
	mDelExpected := mKeysReturn
	m.On("Keys", mKeysExpected).Return(mKeysReturn, nil)
	m.On("Del", mDelExpected).Return(errors.New("Some Error"))
	err := i.RemoveAll()
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestSetIfSuccessfullyOkStatus(t *testing.T) {
	m, i := setup()

	mSetIEExpectedKey := string("{namespace},key1")
	mSetIEExpectedOldData := interface{}("olddata")
	mSetIEExpectedNewData := interface{}("newdata")
	m.On("SetIE", mSetIEExpectedKey, mSetIEExpectedOldData, mSetIEExpectedNewData).Return(true, nil)
	status, err := i.SetIf("key1", "olddata", "newdata")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfSuccessfullyNOKStatus(t *testing.T) {
	m, i := setup()

	mSetIEExpectedKey := string("{namespace},key1")
	mSetIEExpectedOldData := interface{}("olddata")
	mSetIEExpectedNewData := interface{}("newdata")
	m.On("SetIE", mSetIEExpectedKey, mSetIEExpectedOldData, mSetIEExpectedNewData).Return(false, nil)
	status, err := i.SetIf("key1", "olddata", "newdata")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfFailure(t *testing.T) {
	m, i := setup()

	mSetIEExpectedKey := string("{namespace},key1")
	mSetIEExpectedOldData := interface{}("olddata")
	mSetIEExpectedNewData := interface{}("newdata")
	m.On("SetIE", mSetIEExpectedKey, mSetIEExpectedOldData, mSetIEExpectedNewData).Return(false, errors.New("Some error"))
	status, err := i.SetIf("key1", "olddata", "newdata")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfAndPublishSuccessfully(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKey := "{namespace},key"
	expectedOldData := interface{}("olddata")
	expectedNewData := interface{}("newdata")
	m.On("SetIEPub", expectedChannel, expectedEvent, expectedKey, expectedOldData, expectedNewData).Return(true, nil)
	status, err := i.SetIfAndPublish([]string{"channel", "event"}, "key", "olddata", "newdata")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfAndPublishIncorrectChannelAndEvent(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKey := "{namespace},key"
	expectedOldData := interface{}("olddata")
	expectedNewData := interface{}("newdata")
	m.AssertNotCalled(t, "SetIEPub", expectedChannel, expectedEvent, expectedKey, expectedOldData, expectedNewData)
	m.AssertNotCalled(t, "SetIE", expectedKey, expectedOldData, expectedNewData)
	status, err := i.SetIfAndPublish([]string{"channel", "event1", "channel"}, "key", "olddata", "newdata")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}
func TestSetIfAndPublishNOKStatus(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKey := "{namespace},key"
	expectedOldData := interface{}("olddata")
	expectedNewData := interface{}("newdata")
	m.On("SetIEPub", expectedChannel, expectedEvent, expectedKey, expectedOldData, expectedNewData).Return(false, nil)
	status, err := i.SetIfAndPublish([]string{"channel", "event"}, "key", "olddata", "newdata")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfAndPublishNoChannels(t *testing.T) {
	m, i := setup()

	expectedKey := "{namespace},key"
	expectedOldData := interface{}("olddata")
	expectedNewData := interface{}("newdata")
	m.On("SetIE", expectedKey, expectedOldData, expectedNewData).Return(true, nil)
	status, err := i.SetIfAndPublish([]string{}, "key", "olddata", "newdata")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishSuccessfully(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNXPub", expectedChannel, expectedEvent, expectedKey, expectedData).Return(true, nil)
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event"}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishSeveralEvents(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event1___event2"
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNXPub", expectedChannel, expectedEvent, expectedKey, expectedData).Return(true, nil)
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event1", "channel", "event2"}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishNoChannels(t *testing.T) {
	m, i := setup()

	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNX", expectedKey, expectedData).Return(true, nil)
	status, err := i.SetIfNotExistsAndPublish([]string{}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishFail(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNXPub", expectedChannel, expectedEvent, expectedKey, expectedData).Return(false, nil)
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event"}, "key", "data")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishIncorrectChannels(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.AssertNotCalled(t, "SetNXPub", expectedChannel, expectedEvent, expectedKey, expectedData)
	m.AssertNotCalled(t, "SetNX", expectedKey, expectedData)
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event", "channel2"}, "key", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishError(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNXPub", expectedChannel, expectedEvent, expectedKey, expectedData).Return(false, errors.New("Some error"))
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event"}, "key", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsSuccessfullyOkStatus(t *testing.T) {
	m, i := setup()

	mSetNXExpectedKey := string("{namespace},key1")
	mSetNXExpectedData := interface{}("data")
	m.On("SetNX", mSetNXExpectedKey, mSetNXExpectedData).Return(true, nil)
	status, err := i.SetIfNotExists("key1", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsSuccessfullyNOKStatus(t *testing.T) {
	m, i := setup()

	mSetNXExpectedKey := string("{namespace},key1")
	mSetNXExpectedData := interface{}("data")
	m.On("SetNX", mSetNXExpectedKey, mSetNXExpectedData).Return(false, nil)
	status, err := i.SetIfNotExists("key1", "data")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsFailure(t *testing.T) {
	m, i := setup()

	mSetNXExpectedKey := string("{namespace},key1")
	mSetNXExpectedData := interface{}("data")
	m.On("SetNX", mSetNXExpectedKey, mSetNXExpectedData).Return(false, errors.New("Some error"))
	status, err := i.SetIfNotExists("key1", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishSuccessfully(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event1___event2"
	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.On("DelIEPub", expectedChannel, expectedEvent, expectedKey, expectedValue).Return(true, nil)
	status, err := i.RemoveIfAndPublish([]string{"channel", "event1", "channel", "event2"}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishNok(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event1___event2"
	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.On("DelIEPub", expectedChannel, expectedEvent, expectedKey, expectedValue).Return(false, nil)
	status, err := i.RemoveIfAndPublish([]string{"channel", "event1", "channel", "event2"}, "key", "data")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishError(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event1___event2"
	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.On("DelIEPub", expectedChannel, expectedEvent, expectedKey, expectedValue).Return(false, errors.New("Some error"))
	status, err := i.RemoveIfAndPublish([]string{"channel", "event1", "channel", "event2"}, "key", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishIncorrectChannel(t *testing.T) {
	m, i := setup()

	expectedChannel := "{namespace},channel"
	expectedEvent := "event"
	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.AssertNotCalled(t, "DelIEPub", expectedChannel, expectedEvent, expectedKey, expectedValue)
	m.AssertNotCalled(t, "DelIE", expectedKey, expectedValue)
	status, err := i.RemoveIfAndPublish([]string{"channel", "event1", "channel"}, "key", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishNoChannels(t *testing.T) {
	m, i := setup()

	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.On("DelIE", expectedKey, expectedValue).Return(true, nil)
	status, err := i.RemoveIfAndPublish([]string{}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)

}
func TestRemoveIfSuccessfullyOkStatus(t *testing.T) {
	m, i := setup()

	mDelIEExpectedKey := string("{namespace},key1")
	mDelIEExpectedData := interface{}("data")
	m.On("DelIE", mDelIEExpectedKey, mDelIEExpectedData).Return(true, nil)
	status, err := i.RemoveIf("key1", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfSuccessfullyNOKStatus(t *testing.T) {
	m, i := setup()

	mDelIEExpectedKey := string("{namespace},key1")
	mDelIEExpectedData := interface{}("data")
	m.On("DelIE", mDelIEExpectedKey, mDelIEExpectedData).Return(false, nil)
	status, err := i.RemoveIf("key1", "data")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfFailure(t *testing.T) {
	m, i := setup()

	mDelIEExpectedKey := string("{namespace},key1")
	mDelIEExpectedData := interface{}("data")
	m.On("DelIE", mDelIEExpectedKey, mDelIEExpectedData).Return(true, errors.New("Some error"))
	status, err := i.RemoveIf("key1", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}
