package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
}

func TestCheckKeyAndValue(t *testing.T) {
	testCases := []struct {
		name          string
		inputKey      string
		inputValue    interface{}
		expectedError error
	}{
		{
			name:       "happy path",
			inputKey:   "foo",
			inputValue: testStruct{},
		},
		{
			name:          "missing key",
			inputValue:    testStruct{},
			expectedError: ErrEmptyKey,
		},
		{
			name:          "missing value",
			inputKey:      "foo",
			expectedError: ErrEmptyValue,
		},
	}

	for _, tc := range testCases {
		err := CheckKeyAndValue(tc.inputKey, tc.inputValue)
		switch {
		case tc.expectedError != nil:
			assert.Equal(t, tc.expectedError, err, tc.name)
		default:
			assert.NoError(t, err, tc.name)
		}
	}
}
