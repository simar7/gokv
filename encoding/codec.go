package encoding

type Codec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

var (
	JSON = JSONCodec{}
	Gob  = GobCodec{}
	Raw  = RawCodec{}
)
