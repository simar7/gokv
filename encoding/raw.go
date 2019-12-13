package encoding

import (
	"errors"
)

var (
	ErrFailedMarshal = errors.New("input to marshal not []byte")
)

// Raw codec doesn't do any encoding/decoding
// it passes through the data passed in and out
// it may be useful to store binary data where
// marshaling is not important. Only supports []byte.
type RawCodec struct {
}

func (rc RawCodec) Marshal(v interface{}) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, ErrFailedMarshal
	}
	return b, nil
}

func (rc RawCodec) Unmarshal(data []byte, v interface{}) error {
	*v.(*[]byte) = data
	return nil
}
