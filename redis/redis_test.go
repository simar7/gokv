package redis

import (
	"encoding/json"
	"errors"
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

		s, err := NewStore(Options{
			Address: mr.Addr(),
		})

		assert.NoError(t, err)
		assert.Equal(t, 10000, s.p.MaxActive)
		assert.Equal(t, 80, s.p.MaxIdle)
	})

	t.Run("sad path, ping fails", func(t *testing.T) {
		s, err := NewStore(Options{
			Address: "path/to/nowhere:1234",
		})
		assert.Equal(t, "redis initialization failed: dial tcp: lookup path/to/nowhere: no such host", err.Error())
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
