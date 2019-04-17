package sdlgo_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gerrit.oran-osc.org/r/ric-plt/sdlgo"
)

type mockDB struct {
	mock.Mock
}

func (m *mockDB) MSet(pairs ...interface{}) error {
	a := m.Called(pairs)
	return a.Error(0)
}

func (m *mockDB) MGet(keys []string) ([]interface{}, error) {
	a := m.Called(keys)
	return a.Get(0).([]interface{}), a.Error(1)
}

func (m *mockDB) Close() error {
	a := m.Called()
	return a.Error(0)
}

func (m *mockDB) Del(keys []string) error {
	a := m.Called(keys)
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

func (m *mockDB) SetNX(key string, data interface{}) (bool, error) {
	a := m.Called(key, data)
	return a.Bool(0), a.Error(1)
}

func (m *mockDB) DelIE(key string, data interface{}) (bool, error) {
	a := m.Called(key, data)
	return a.Bool(0), a.Error(1)
}

func setup() (*mockDB, *sdlgo.SdlInstance) {
	m := new(mockDB)
	i := sdlgo.NewSdlInstance("namespace", m)
	return m, i
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
	m.On("SetIE", mSetIEExpectedKey, mSetIEExpectedOldData, mSetIEExpectedNewData).Return(true, errors.New("Some error"))
	status, err := i.SetIf("key1", "olddata", "newdata")
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
	m.On("SetNX", mSetNXExpectedKey, mSetNXExpectedData).Return(true, errors.New("Some error"))
	status, err := i.SetIfNotExists("key1", "data")
	assert.NotNil(t, err)
	assert.False(t, status)
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
