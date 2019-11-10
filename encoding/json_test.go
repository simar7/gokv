package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Foo string  `json:"foo"`
	Bar float64 `json:"bar"`
	Baz int     `json:"baz"`
}

func TestJSONCodec_Marshal(t *testing.T) {
	testCases := []struct {
		name           string
		input          interface{}
		expectedError  error
		expectedOutput []byte
	}{
		{
			name: "happy path",
			input: testStruct{
				Foo: "foostring",
				Bar: 42.0,
				Baz: 123,
			},
		},
		// TODO: Add failing cases
	}

	for _, tc := range testCases {
		jc := JSONCodec{}
		actualData, err := jc.Marshal(tc.input)
		switch {
		case tc.expectedError != nil:
			assert.Equal(t, tc.expectedError, err, tc.name)
			assert.Nil(t, actualData, tc.name)
		default:
			assert.NoError(t, err, tc.name)
			assert.NotNil(t, actualData, tc.name)
		}
	}
}

func TestJSONCodec_Unmarshal(t *testing.T) {
	jc := JSONCodec{}
	inputBytes := []byte{123, 34, 102, 111, 111, 34, 58, 34, 102, 111, 111, 115, 116, 114, 105, 110, 103, 34, 44, 34, 98, 97, 114, 34, 58, 52, 50, 44, 34, 98, 97, 122, 34, 58, 49, 50, 51, 125}

	actualOutput := testStruct{}
	expectedOuput := testStruct{
		Foo: "foostring",
		Bar: 42.0,
		Baz: 123,
	}
	err := jc.Unmarshal(inputBytes, &actualOutput)
	assert.NoError(t, err)
	assert.Equal(t, expectedOuput, actualOutput)
}
