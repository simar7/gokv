package encoding

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRAWCodec_Marshal(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		rc := RawCodec{}
		b, _ := json.Marshal(testStruct{
			Foo: "foo",
			Bar: 123,
			Baz: 456,
		})
		actual, err := rc.Marshal(b)
		assert.NoError(t, err)
		assert.Equal(t, b, actual)

	})

	t.Run("sad path, input is not []byte", func(t *testing.T) {
		rc := RawCodec{}
		actual, err := rc.Marshal("foo")
		assert.Equal(t, ErrFailedMarshal, err)
		assert.Nil(t, actual)
	})
}

func TestRAWCodec_Unmarshal(t *testing.T) {
	rc := RawCodec{}
	b, _ := json.Marshal(testStruct{
		Foo: "foo",
		Bar: 123,
		Baz: 456,
	})
	var actual []byte
	err := rc.Unmarshal(b, &actual)
	assert.NoError(t, err)
	assert.Equal(t, b, actual)
}
