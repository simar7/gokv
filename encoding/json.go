package encoding

import "encoding/json"

type JSONCodec struct {
}

func (jc JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (jc JSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
