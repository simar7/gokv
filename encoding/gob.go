package encoding

import (
	"bytes"
	"encoding/gob"
)

type GobCodec struct{}

func (c GobCodec) Marshal(v interface{}) ([]byte, error) {
	var b bytes.Buffer
	encoder := gob.NewEncoder(&b)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c GobCodec) Unmarshal(data []byte, v interface{}) error {
	r := bytes.NewReader(data)
	decoder := gob.NewDecoder(r)
	return decoder.Decode(v)
}
