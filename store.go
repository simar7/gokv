package gokv

type Store interface{
	Set(k string, v interface{}) error
	Get(k string, v interface{}) (found bool, err error)
	Delete(k string) error
	Close() error
}
