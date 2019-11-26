package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/simar7/gokv/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Foo string  `json:"foo"`
	Bar float64 `json:"bar"`
	Baz int     `json:"baz"`
}

func TestNewStore(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		mr, err := miniredis.Run()
		assert.NoError(t, err)
		defer mr.Close()

		s, err := NewStore(Options{
			Address: mr.Addr(),
		})
		assert.NoError(t, err)
		defer s.Close()

		assert.NoError(t, err)
		assert.Equal(t, 10000, s.p.MaxActive)
		assert.Equal(t, 80, s.p.MaxIdle)
	})

	t.Run("sad path, ping fails", func(t *testing.T) {
		s, err := NewStore(Options{
			Address: "path/to/nowhere:1234",
		})

		assert.Contains(t, err.Error(), "redis initialization failed: dial tcp: lookup path/to/nowhere")
		assert.Equal(t, Store{}, s)
	})
}

func TestStore_Get(t *testing.T) {
	testCases := []struct {
		name          string
		expectedValue interface{}
		expectedError error
		inStore       bool
		badMarshal    bool
	}{
		{
			name: "happy path, key found",
			expectedValue: testStruct{
				Foo: "foo",
				Bar: 42.0,
				Baz: 123,
			},
			inStore: true,
		},
		{
			name:          "happy path, key not found",
			expectedError: ErrKeyNotFound,
			expectedValue: testStruct{},
		},
		{
			name:          "sad path, marshal failure: stored key type differs from asked",
			expectedValue: testStruct{},
			expectedError: errors.New("invalid character 'u' in literal false (expecting 'a')"),
			inStore:       true,
			badMarshal:    true,
		},
	}

	for _, tc := range testCases {
		mr, err := miniredis.Run()
		assert.NoError(t, err, tc.name)
		defer mr.Close()

		if tc.inStore {
			if tc.badMarshal {
				_ = mr.Set("foo", "fubar")
			} else {
				b, _ := json.Marshal(testStruct{
					Foo: "foo",
					Bar: 42.0,
					Baz: 123,
				})
				_ = mr.Set("foo", string(b))
			}
		}

		s, err := NewStore(Options{
			Address: mr.Addr(),
		})
		assert.NoError(t, err, tc.name)
		defer s.Close()

		var actualValue testStruct
		found, err := s.Get(types.GetItemInput{
			Key:   "foo",
			Value: &actualValue,
		})

		switch {
		case tc.expectedError != nil:
			assert.Equal(t, tc.expectedError.Error(), err.Error(), tc.name)
		default:
			assert.NoError(t, err, tc.name)
			assert.True(t, found, tc.name)
		}

		assert.Equal(t, tc.expectedValue, actualValue, tc.name)
	}
}

func TestStore_Set(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	s, err := NewStore(Options{
		Address: mr.Addr(),
	})
	assert.NoError(t, err)
	defer s.Close()

	assert.NoError(t, s.Set(types.SetItemInput{
		Key: "foo",
		Value: testStruct{
			Foo: "foo",
			Bar: 42.0,
			Baz: 123,
		},
	}))

	// check if the key was actually set
	var actualValue testStruct
	found, err := s.Get(types.GetItemInput{Key: "foo", Value: &actualValue})
	assert.True(t, found)
	assert.NoError(t, err)
	assert.Equal(t, testStruct{Foo: "foo", Bar: 42.0, Baz: 123}, actualValue)
}

func TestStore_Delete(t *testing.T) {
	t.Run("happy path, key to delete exists in redis", func(t *testing.T) {
		mr, err := miniredis.Run()
		assert.NoError(t, err)
		defer mr.Close()

		b, _ := json.Marshal(testStruct{
			Foo: "foo",
			Bar: 42.0,
			Baz: 123,
		})
		_ = mr.Set("foo", string(b))

		s, err := NewStore(Options{
			Address: mr.Addr(),
		})
		assert.NoError(t, err)
		defer s.Close()

		assert.NoError(t, s.Delete(types.DeleteItemInput{Key: "foo"}))
	})

	t.Run("sad path, key to delete does not exist", func(t *testing.T) {
		mr, err := miniredis.Run()
		assert.NoError(t, err)
		defer mr.Close()

		s, err := NewStore(Options{
			Address: mr.Addr(),
		})
		assert.NoError(t, err)
		defer s.Close()

		assert.Equal(t, ErrKeyNotFound, s.Delete(types.DeleteItemInput{Key: "foo"}))
	})
}

func TestStore_BatchSet(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	s, err := NewStore(Options{
		Address: mr.Addr(),
	})
	assert.NoError(t, err)
	defer s.Close()

	assert.NoError(t, s.BatchSet(types.BatchSetItemInput{
		Keys:   []string{"key1", "key2", "key3"},
		Values: []string{"val1", "val2", "val3"},
	}))

	// check if the keys were actually set
	for i := 1; i <= 3; i++ {
		var actualValue string
		found, err := s.Get(types.GetItemInput{Key: fmt.Sprintf("key%d", i), Value: &actualValue})
		assert.True(t, found)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val%d", i), actualValue)
	}
}

func TestStore_Scan(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	s, err := NewStore(Options{
		Address: mr.Addr(),
	})
	assert.NoError(t, err)
	defer s.Close()

	expectedKeys := []string{"key1", "key2", "key3"}
	expectedValues := []string{"val1", "", "val3"}

	assert.NoError(t, s.BatchSet(types.BatchSetItemInput{
		Keys:   expectedKeys,
		Values: expectedValues,
	}))

	out, err := s.Scan(types.ScanInput{})
	assert.NoError(t, err)

	assert.Equal(t, []string{"key1", "key2", "key3"}, out.Keys)
	for i, v := range out.Values {
		assert.Equal(t, fmt.Sprintf(`"%s"`, expectedValues[i]), string(v))
	}
}
