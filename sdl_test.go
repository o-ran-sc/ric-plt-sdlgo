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

package sdlgo_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockDB struct {
	mock.Mock
}

func (m *mockDB) SubscribeChannelDB(cb func(string, ...string), channels ...string) error {
	a := m.Called(cb, channels)
	return a.Error(0)
}

func (m *mockDB) UnsubscribeChannelDB(channels ...string) error {
	a := m.Called(channels)
	return a.Error(0)
}

func (m *mockDB) MSet(pairs ...interface{}) error {
	a := m.Called(pairs)
	return a.Error(0)
}

func (m *mockDB) MSetMPub(channelsAndEvents []string, pairs ...interface{}) error {
	a := m.Called(channelsAndEvents, pairs)
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

func (m *mockDB) DelMPub(channelsAndEvents []string, keys []string) error {
	a := m.Called(channelsAndEvents, keys)
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

func (m *mockDB) SetIEPub(channelsAndEvents []string, key string, oldData, newData interface{}) (bool, error) {
	a := m.Called(channelsAndEvents, key, oldData, newData)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) SetNX(key string, data interface{}, expiration time.Duration) (bool, error) {
	a := m.Called(key, data, expiration)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) SetNXPub(channelsAndEvents []string, key string, data interface{}) (bool, error) {
	a := m.Called(channelsAndEvents, key, data)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) DelIE(key string, data interface{}) (bool, error) {
	a := m.Called(key, data)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) DelIEPub(channelsAndEvents []string, key string, data interface{}) (bool, error) {
	a := m.Called(channelsAndEvents, key, data)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) SAdd(key string, data ...interface{}) error {
	a := m.Called(key, data)
	return a.Error(0)
}

func (m *mockDB) SRem(key string, data ...interface{}) error {
	a := m.Called(key, data)
	return a.Error(0)
}
func (m *mockDB) SMembers(key string) ([]string, error) {
	a := m.Called(key)
	return a.Get(0).([]string), a.Error(1)
}

func (m *mockDB) SIsMember(key string, data interface{}) (bool, error) {
	a := m.Called(key, data)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) SCard(key string) (int64, error) {
	a := m.Called(key)
	return a.Get(0).(int64), a.Error(1)
}

func (m *mockDB) PTTL(key string) (time.Duration, error) {
	a := m.Called(key)
	return a.Get(0).(time.Duration), a.Error(1)
}

func (m *mockDB) PExpireIE(key string, data interface{}, expiration time.Duration) error {
	a := m.Called(key, data, expiration)
	return a.Error(0)
}

func setup() (*mockDB, *sdlgo.SdlInstance) {
	m := new(mockDB)
	i := sdlgo.NewSdlInstanceForTest("namespace", m)
	return m, i
}

func verifySliceInOrder(a, b []string) bool {
	for i, v := range a {
		found := false
		if i%2 == 0 {
			for j, x := range b {
				if j%2 == 0 && x == v && a[i+1] == b[j+1] {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true

}

func TestClose(t *testing.T) {
	m, i := setup()

	m.On("CloseDB").Return(nil)
	err := i.Close()
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestCloseReturnError(t *testing.T) {
	m, i := setup()

	m.On("CloseDB").Return(errors.New("Some error"))
	err := i.Close()
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestSubscribeChannel(t *testing.T) {
	m, i := setup()

	expectedCB := func(channel string, events ...string) {}
	expectedChannels := []string{"{namespace},channel1", "{namespace},channel2"}

	m.On("SubscribeChannelDB", mock.AnythingOfType("func(string, ...string)"), expectedChannels).Return(nil)
	err := i.SubscribeChannel(expectedCB, "channel1", "channel2")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestSubscribeChannelError(t *testing.T) {
	mockedErr := errors.New("Some DB Backend Subscribe Error")
	m, i := setup()

	expectedCB := func(channel string, events ...string) {}
	expectedChannels := []string{"{namespace},channel1", "{namespace},channel2"}

	m.On("SubscribeChannelDB", mock.AnythingOfType("func(string, ...string)"), expectedChannels).Return(mockedErr)
	err := i.SubscribeChannel(expectedCB, "channel1", "channel2")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), mockedErr.Error())
	m.AssertExpectations(t)
}

func TestUnsubscribeChannel(t *testing.T) {
	m, i := setup()

	expectedChannels := []string{"{namespace},channel1", "{namespace},channel2"}

	m.On("UnsubscribeChannelDB", expectedChannels).Return(nil)
	err := i.UnsubscribeChannel("channel1", "channel2")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestUnsubscribeChannelError(t *testing.T) {
	mockedErr := errors.New("Some DB Backend Unsubscribe Error")
	m, i := setup()

	expectedChannels := []string{"{namespace},channel1", "{namespace},channel2"}

	m.On("UnsubscribeChannelDB", expectedChannels).Return(mockedErr)
	err := i.UnsubscribeChannel("channel1", "channel2")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), mockedErr.Error())
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

func TestWriteByteSliceAsValue(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", []byte{1, 2, 3, 4, 5}}

	m.On("MSet", msetExpected).Return(nil)
	err := i.Set("key1", []byte{1, 2, 3, 4, 5})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteByteSliceAsValueMixed(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", []byte{1, 2, 3, 4, 5}, "{namespace},key2", "value2"}

	m.On("MSet", msetExpected).Return(nil)
	err := i.Set("key1", []byte{1, 2, 3, 4, 5}, []string{"key2", "value2"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteByteArrayAsValue(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", [5]byte{1, 2, 3, 4, 5}}

	m.On("MSet", msetExpected).Return(nil)
	err := i.Set("key1", [5]byte{1, 2, 3, 4, 5})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteMapAsInput(t *testing.T) {
	m, i := setup()

	setExpected := []interface{}{"{namespace},key1", "string123",
		"{namespace},key22", 12,
		"{namespace},key333", []byte{1, 2, 3, 4, 5}}
	inputMap := map[string]interface{}{
		"key1":   "string123",
		"key22":  12,
		"key333": []byte{1, 2, 3, 4, 5},
	}

	m.On("MSet", mock.MatchedBy(func(input []interface{}) bool {
		for _, v := range input {
			found := false
			for _, x := range setExpected {
				found = reflect.DeepEqual(x, v)
				if found == true {
					break
				}
			}
			if found == false {
				return false
			}
		}
		return true
	})).Return(nil)

	err := i.Set(inputMap)
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteMixed(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", "data1", "{namespace},key2", "data2", "{namespace},key3", "data3"}

	m.On("MSet", msetExpected).Return(nil)
	err := i.Set("key1", "data1", []string{"key2", "data2"}, [2]string{"key3", "data3"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteIncorrectMixed(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", "data1", "{namespace},key2", "data2", "{namespace},key3", "data3"}

	m.AssertNotCalled(t, "MSet", msetExpected)
	err := i.Set("key1", []string{"key2", "data2"}, [2]string{"key3", "data3"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}
func TestWriteIncorrectPairs(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{}

	m.AssertNotCalled(t, "MSet", msetExpected)
	err := i.Set("key")
	assert.NotNil(t, err)
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

func TestWriteSeveralKeysIncorrectSlice(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{"{namespace},key1", "data1", "{namespace},key2", 22}

	m.AssertNotCalled(t, "MSet", msetExpected)
	err := i.Set([]interface{}{"key1", "data1", "key2"})
	assert.NotNil(t, err)
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

func TestWriteSeveralKeysIncorrectArray(t *testing.T) {
	m, i := setup()

	msetExpected := []interface{}{}

	m.AssertNotCalled(t, "MSet", msetExpected)
	err := i.Set([3]string{"key1", "data1", "key2"})
	assert.NotNil(t, err)
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

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}

	m.On("MSetMPub", expectedChannelAndEvent, expectedKeyVal).Return(nil)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event"}, "key1", "data1")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishSeveralChannelsAndEvents(t *testing.T) {
	m, i := setup()

	expectedChannelsAndEvents := []string{"{namespace},channel1", "event1___event2",
		"{namespace},channel2", "event3___event4"}
	expectedKeyVal := []interface{}{"{namespace},key", "data"}

	verifyChannelAndEvent := func(input []string) bool {
		return verifySliceInOrder(input, expectedChannelsAndEvents)
	}
	m.On("MSetMPub", mock.MatchedBy(verifyChannelAndEvent), expectedKeyVal).Return(nil)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel1", "event1", "channel2", "event3", "channel1", "event2", "channel2", "event4"},
		"key", "data")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishOneKeyOneChannelTwoEvents(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvents := []string{"{namespace},channel", "event1___event2"}
	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}

	m.On("MSetMPub", expectedChannelAndEvents, expectedKeyVal).Return(nil)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event1", "channel", "event2"}, "key1", "data1")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishIncorrectChannelAndEvent(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{}
	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}
	m.AssertNotCalled(t, "MSetMPub", expectedChannelAndEvent, expectedKeyVal)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event1", "channel"}, "key1", "data1")
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishNotAllowedCharactersInEvents(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{}
	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}
	m.AssertNotCalled(t, "MSetMPub", expectedChannelAndEvent, expectedKeyVal)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event1___event2"}, "key1", "data1")
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishNoData(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{}
	expectedKeyVal := []interface{}{"key"}

	m.AssertNotCalled(t, "MSetMPub", expectedChannelAndEvent, expectedKeyVal)
	m.AssertNotCalled(t, "MSet", expectedKeyVal)
	err := i.SetAndPublish([]string{"channel", "event"}, []interface{}{"key"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestWriteAndPublishNoChannelEvent(t *testing.T) {
	m, i := setup()

	expectedKeyVal := []interface{}{"{namespace},key1", "data1"}

	m.On("MSet", expectedKeyVal).Return(nil)
	m.AssertNotCalled(t, "MSetMPub", "", "", expectedKeyVal)
	err := i.SetAndPublish([]string{}, "key1", "data1")
	assert.Nil(t, err)
	m.AssertExpectations(t)

}

func TestRemoveAndPublishSuccessfully(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKeys := []string{"{namespace},key1", "{namespace},key2"}

	m.On("DelMPub", expectedChannelAndEvent, expectedKeys).Return(nil)
	err := i.RemoveAndPublish([]string{"channel", "event"}, []string{"key1", "key2"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAndPublishSeveralChannelsAndEventsSuccessfully(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel1", "event1___event2",
		"{namespace},channel2", "event3___event4"}
	expectedKeys := []string{"{namespace},key1", "{namespace},key2"}

	verifyChannelAndEvent := func(input []string) bool {
		return verifySliceInOrder(input, expectedChannelAndEvent)
	}
	m.On("DelMPub", mock.MatchedBy(verifyChannelAndEvent), expectedKeys).Return(nil)
	err := i.RemoveAndPublish([]string{"channel1", "event1", "channel2", "event3",
		"channel1", "event2", "channel2", "event4"},
		[]string{"key1", "key2"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}
func TestRemoveAndPublishFail(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKeys := []string{"{namespace},key1", "{namespace},key2"}

	m.On("DelMPub", expectedChannelAndEvent, expectedKeys).Return(errors.New("Some error"))
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

	notExpectedChannelAndEvent := []string{}
	notExpectedKeys := []string{"{namespace},key"}

	m.AssertNotCalled(t, "DelMPub", notExpectedChannelAndEvent, notExpectedKeys)
	m.AssertNotCalled(t, "Del", notExpectedKeys)
	err := i.RemoveAndPublish([]string{"channel", "event", "channel2"}, []string{"key1", "key2"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)

}
func TestRemoveAndPublishNoKeys(t *testing.T) {
	m, i := setup()

	notExpectedChannelAndEvent := []string{}
	notExpectedKeys := []string{"{namespace},key"}

	m.AssertNotCalled(t, "DelMPub", notExpectedChannelAndEvent, notExpectedKeys)
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

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKey := "{namespace},key"
	expectedOldData := interface{}("olddata")
	expectedNewData := interface{}("newdata")
	m.On("SetIEPub", expectedChannelAndEvent, expectedKey, expectedOldData, expectedNewData).Return(true, nil)
	status, err := i.SetIfAndPublish([]string{"channel", "event"}, "key", "olddata", "newdata")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfAndPublishIncorrectChannelAndEvent(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKey := "{namespace},key"
	expectedOldData := interface{}("olddata")
	expectedNewData := interface{}("newdata")
	m.AssertNotCalled(t, "SetIEPub", expectedChannelAndEvent, expectedKey, expectedOldData, expectedNewData)
	m.AssertNotCalled(t, "SetIE", expectedKey, expectedOldData, expectedNewData)
	status, err := i.SetIfAndPublish([]string{"channel", "event1", "channel"}, "key", "olddata", "newdata")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}
func TestSetIfAndPublishNOKStatus(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKey := "{namespace},key"
	expectedOldData := interface{}("olddata")
	expectedNewData := interface{}("newdata")
	m.On("SetIEPub", expectedChannelAndEvent, expectedKey, expectedOldData, expectedNewData).Return(false, nil)
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

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNXPub", expectedChannelAndEvent, expectedKey, expectedData).Return(true, nil)
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event"}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishSeveralEvents(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event1___event2"}
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNXPub", expectedChannelAndEvent, expectedKey, expectedData).Return(true, nil)
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event1", "channel", "event2"}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishNoChannels(t *testing.T) {
	m, i := setup()

	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNX", expectedKey, expectedData, time.Duration(0)).Return(true, nil)
	status, err := i.SetIfNotExistsAndPublish([]string{}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishFail(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNXPub", expectedChannelAndEvent, expectedKey, expectedData).Return(false, nil)
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event"}, "key", "data")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishIncorrectChannels(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.AssertNotCalled(t, "SetNXPub", expectedChannelAndEvent, expectedKey, expectedData)
	m.AssertNotCalled(t, "SetNX", expectedKey, expectedData, 0)
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event", "channel2"}, "key", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsAndPublishError(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKey := "{namespace},key"
	expectedData := interface{}("data")

	m.On("SetNXPub", expectedChannelAndEvent, expectedKey, expectedData).Return(false, errors.New("Some error"))
	status, err := i.SetIfNotExistsAndPublish([]string{"channel", "event"}, "key", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsSuccessfullyOkStatus(t *testing.T) {
	m, i := setup()

	mSetNXExpectedKey := string("{namespace},key1")
	mSetNXExpectedData := interface{}("data")
	m.On("SetNX", mSetNXExpectedKey, mSetNXExpectedData, time.Duration(0)).Return(true, nil)
	status, err := i.SetIfNotExists("key1", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsSuccessfullyNOKStatus(t *testing.T) {
	m, i := setup()

	mSetNXExpectedKey := string("{namespace},key1")
	mSetNXExpectedData := interface{}("data")
	m.On("SetNX", mSetNXExpectedKey, mSetNXExpectedData, time.Duration(0)).Return(false, nil)
	status, err := i.SetIfNotExists("key1", "data")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestSetIfNotExistsFailure(t *testing.T) {
	m, i := setup()

	mSetNXExpectedKey := string("{namespace},key1")
	mSetNXExpectedData := interface{}("data")
	m.On("SetNX", mSetNXExpectedKey, mSetNXExpectedData, time.Duration(0)).Return(false, errors.New("Some error"))
	status, err := i.SetIfNotExists("key1", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishSuccessfully(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event1___event2"}
	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.On("DelIEPub", expectedChannelAndEvent, expectedKey, expectedValue).Return(true, nil)
	status, err := i.RemoveIfAndPublish([]string{"channel", "event1", "channel", "event2"}, "key", "data")
	assert.Nil(t, err)
	assert.True(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishNok(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event1___event2"}
	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.On("DelIEPub", expectedChannelAndEvent, expectedKey, expectedValue).Return(false, nil)
	status, err := i.RemoveIfAndPublish([]string{"channel", "event1", "channel", "event2"}, "key", "data")
	assert.Nil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishError(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event1___event2"}
	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.On("DelIEPub", expectedChannelAndEvent, expectedKey, expectedValue).Return(false, errors.New("Some error"))
	status, err := i.RemoveIfAndPublish([]string{"channel", "event1", "channel", "event2"}, "key", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
	m.AssertExpectations(t)
}

func TestRemoveIfAndPublishIncorrectChannel(t *testing.T) {
	m, i := setup()

	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	expectedKey := "{namespace},key"
	expectedValue := interface{}("data")

	m.AssertNotCalled(t, "DelIEPub", expectedChannelAndEvent, expectedKey, expectedValue)
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

func TestRemoveAllAndPublishSuccessfully(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mKeysReturn := []string{"{namespace},key1", "{namespace},key2"}
	mDelExpected := mKeysReturn
	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	m.On("Keys", mKeysExpected).Return(mKeysReturn, nil)
	m.On("DelMPub", expectedChannelAndEvent, mDelExpected).Return(nil)
	err := i.RemoveAllAndPublish([]string{"channel", "event"})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAllAndPublishKeysReturnError(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mKeysReturn := []string{"{namespace},key1", "{namespace},key2"}
	mDelExpected := mKeysReturn
	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	m.On("Keys", mKeysExpected).Return(mKeysReturn, errors.New("Some error"))
	m.AssertNotCalled(t, "DelMPub", expectedChannelAndEvent, mDelExpected)
	err := i.RemoveAllAndPublish([]string{"channel", "event"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAllAndPublishKeysDelReturnsError(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mKeysReturn := []string{"{namespace},key1", "{namespace},key2"}
	mDelExpected := mKeysReturn
	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	m.On("Keys", mKeysExpected).Return(mKeysReturn, nil)
	m.On("DelMPub", expectedChannelAndEvent, mDelExpected).Return(errors.New("Some error"))
	err := i.RemoveAllAndPublish([]string{"channel", "event"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAllAndPublishKeysEventsWithIllegalCharacters(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mKeysReturn := []string{"{namespace},key1", "{namespace},key2"}
	mDelExpected := mKeysReturn
	expectedChannelAndEvent := []string{"{namespace},channel", "event"}
	m.On("Keys", mKeysExpected).Return(mKeysReturn, nil)
	m.AssertNotCalled(t, "DelMPub", expectedChannelAndEvent, mDelExpected)
	err := i.RemoveAllAndPublish([]string{"channel", "event___anotherEvent"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)

}

func TestRemoveAllAndPublishNoChannels(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mKeysReturn := []string{"{namespace},key1", "{namespace},key2"}
	mDelExpected := mKeysReturn
	m.On("Keys", mKeysExpected).Return(mKeysReturn, nil)
	m.On("Del", mDelExpected).Return(nil)
	m.AssertNotCalled(t, "DelMPub", "", "", mDelExpected)
	err := i.RemoveAllAndPublish([]string{})
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveAllAndPublishIncorrectChannel(t *testing.T) {
	m, i := setup()

	mKeysExpected := string("{namespace},*")
	mKeysReturn := []string{"{namespace},key1", "{namespace},key2"}
	mDelExpected := mKeysReturn
	m.On("Keys", mKeysExpected).Return(mKeysReturn, nil)
	m.AssertNotCalled(t, "DelMPub", "", "", mDelExpected)
	err := i.RemoveAllAndPublish([]string{"channel", "event", "channel2"})
	assert.NotNil(t, err)
	m.AssertExpectations(t)

}

func TestAddMemberSuccessfully(t *testing.T) {
	m, i := setup()

	groupExpected := string("{namespace},group")
	membersExpected := []interface{}{"member1", "member2"}

	m.On("SAdd", groupExpected, membersExpected).Return(nil)

	err := i.AddMember("group", "member1", "member2")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestAddMemberFail(t *testing.T) {
	m, i := setup()

	groupExpected := string("{namespace},group")
	membersExpected := []interface{}{"member1", "member2"}

	m.On("SAdd", groupExpected, membersExpected).Return(errors.New("Some error"))

	err := i.AddMember("group", "member1", "member2")
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}
func TestRemoveMemberSuccessfully(t *testing.T) {
	m, i := setup()

	groupExpected := string("{namespace},group")
	membersExpected := []interface{}{"member1", "member2"}

	m.On("SRem", groupExpected, membersExpected).Return(nil)

	err := i.RemoveMember("group", "member1", "member2")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveMemberFail(t *testing.T) {
	m, i := setup()

	groupExpected := string("{namespace},group")
	membersExpected := []interface{}{"member1", "member2"}

	m.On("SRem", groupExpected, membersExpected).Return(errors.New("Some error"))

	err := i.RemoveMember("group", "member1", "member2")
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestRemoveGroupSuccessfully(t *testing.T) {
	m, i := setup()

	groupExpected := []string{"{namespace},group"}

	m.On("Del", groupExpected).Return(nil)

	err := i.RemoveGroup("group")
	assert.Nil(t, err)
	m.AssertExpectations(t)
}
func TestRemoveGroupFail(t *testing.T) {
	m, i := setup()

	groupExpected := []string{"{namespace},group"}

	m.On("Del", groupExpected).Return(errors.New("Some error"))

	err := i.RemoveGroup("group")
	assert.NotNil(t, err)
	m.AssertExpectations(t)
}

func TestGetMembersSuccessfully(t *testing.T) {
	m, i := setup()

	groupExpected := "{namespace},group"
	returnExpected := []string{"member1", "member2"}

	m.On("SMembers", groupExpected).Return(returnExpected, nil)

	result, err := i.GetMembers("group")
	assert.Nil(t, err)
	assert.Equal(t, result, returnExpected)
	m.AssertExpectations(t)
}
func TestGetMembersFail(t *testing.T) {
	m, i := setup()

	groupExpected := "{namespace},group"
	returnExpected := []string{"member1", "member2"}

	m.On("SMembers", groupExpected).Return(returnExpected, errors.New("Some error"))

	result, err := i.GetMembers("group")
	assert.NotNil(t, err)
	assert.Equal(t, []string{}, result)
	m.AssertExpectations(t)
}

func TestIsMemberSuccessfullyIsMember(t *testing.T) {
	m, i := setup()

	groupExpected := "{namespace},group"
	memberExpected := "member"

	m.On("SIsMember", groupExpected, memberExpected).Return(true, nil)

	result, err := i.IsMember("group", "member")
	assert.Nil(t, err)
	assert.True(t, result)
	m.AssertExpectations(t)
}
func TestIsMemberSuccessfullyIsNotMember(t *testing.T) {
	m, i := setup()

	groupExpected := "{namespace},group"
	memberExpected := "member"

	m.On("SIsMember", groupExpected, memberExpected).Return(false, nil)

	result, err := i.IsMember("group", "member")
	assert.Nil(t, err)
	assert.False(t, result)
	m.AssertExpectations(t)
}
func TestIsMemberFailure(t *testing.T) {
	m, i := setup()

	groupExpected := "{namespace},group"
	memberExpected := "member"

	m.On("SIsMember", groupExpected, memberExpected).Return(true, errors.New("Some error"))

	result, err := i.IsMember("group", "member")
	assert.NotNil(t, err)
	assert.False(t, result)
	m.AssertExpectations(t)
}

func TestGroupSizeSuccessfully(t *testing.T) {
	m, i := setup()

	var expectedSize int64
	expectedSize = 2
	groupExpected := "{namespace},group"

	m.On("SCard", groupExpected).Return(expectedSize, nil)

	result, err := i.GroupSize("group")
	assert.Nil(t, err)
	assert.Equal(t, expectedSize, result)
	m.AssertExpectations(t)
}
func TestGroupSizeFail(t *testing.T) {
	m, i := setup()

	var expectedSize int64
	expectedSize = 2
	groupExpected := "{namespace},group"

	m.On("SCard", groupExpected).Return(expectedSize, errors.New("Some error"))

	result, err := i.GroupSize("group")
	assert.NotNil(t, err)
	assert.Equal(t, int64(0), result)
	m.AssertExpectations(t)
}

func TestLockResourceSuccessfully(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, nil)

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{})
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	m.AssertExpectations(t)
}

func TestLockResourceFailure(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, errors.New("Some error"))

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{})
	assert.NotNil(t, err)
	assert.Nil(t, lock)
	m.AssertExpectations(t)
}

func TestLockResourceTrySeveralTimesSuccessfully(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(false, nil).Once()
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, nil).Once()

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{
		RetryCount: 2,
	})
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	m.AssertExpectations(t)
}

func TestLockResourceTrySeveralTimesFailure(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(false, nil).Once()
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, errors.New("Some error")).Once()

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{
		RetryCount: 2,
	})
	assert.NotNil(t, err)
	assert.Nil(t, lock)
	m.AssertExpectations(t)
}

func TestLockResourceTrySeveralTimesUnableToGetResource(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(false, nil).Once()
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(false, nil).Once()

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{
		RetryCount: 1,
	})
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Lock not obtained")
	assert.Nil(t, lock)
	m.AssertExpectations(t)
}

func TestReleaseResourceSuccessfully(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, nil).Once()
	m.On("DelIE", resourceExpected, mock.Anything).Return(true, nil).Once()

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{
		RetryCount: 1,
	})
	err2 := lock.ReleaseResource()
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.Nil(t, err2)
	m.AssertExpectations(t)
}

func TestReleaseResourceFailure(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, nil).Once()
	m.On("DelIE", resourceExpected, mock.Anything).Return(true, errors.New("Some error")).Once()

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{
		RetryCount: 1,
	})
	err2 := lock.ReleaseResource()
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.NotNil(t, err2)
	m.AssertExpectations(t)
}

func TestReleaseResourceLockNotHeld(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, nil).Once()
	m.On("DelIE", resourceExpected, mock.Anything).Return(false, nil).Once()

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{
		RetryCount: 1,
	})
	err2 := lock.ReleaseResource()
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.NotNil(t, err2)
	assert.EqualError(t, err2, "Lock not held")
	m.AssertExpectations(t)
}

func TestRefreshResourceSuccessfully(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, nil).Once()
	m.On("PExpireIE", resourceExpected, mock.Anything, time.Duration(1)).Return(nil).Once()

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{
		RetryCount: 1,
	})
	err2 := lock.RefreshResource(time.Duration(1))
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.Nil(t, err2)
	m.AssertExpectations(t)
}

func TestRefreshResourceFailure(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("SetNX", resourceExpected, mock.Anything, time.Duration(1)).Return(true, nil).Once()
	m.On("PExpireIE", resourceExpected, mock.Anything, time.Duration(1)).Return(errors.New("Some error")).Once()

	lock, err := i.LockResource("resource", time.Duration(1), &sdlgo.Options{
		RetryCount: 1,
	})
	err2 := lock.RefreshResource(time.Duration(1))
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.NotNil(t, err2)
	m.AssertExpectations(t)
}

func TestCheckResourceSuccessfully(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("PTTL", resourceExpected).Return(time.Duration(1), nil)
	result, err := i.CheckResource("resource")
	assert.Nil(t, err)
	assert.Equal(t, result, time.Duration(1))
	m.AssertExpectations(t)
}

func TestCheckResourceFailure(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("PTTL", resourceExpected).Return(time.Duration(1), errors.New("Some error"))
	result, err := i.CheckResource("resource")
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Some error")
	assert.Equal(t, result, time.Duration(0))
	m.AssertExpectations(t)
}

func TestCheckResourceInvalidResource(t *testing.T) {
	m, i := setup()

	resourceExpected := "{namespace},resource"
	m.On("PTTL", resourceExpected).Return(time.Duration(-1), nil)
	result, err := i.CheckResource("resource")
	assert.NotNil(t, err)
	assert.EqualError(t, err, "invalid resource given, no expiration time attached")
	assert.Equal(t, result, time.Duration(0))
	m.AssertExpectations(t)
}
