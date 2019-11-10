package bbolt

import (
	"errors"
	"reflect"

	"github.com/simar7/gokv/types"

	"github.com/simar7/gokv/encoding"
	"github.com/simar7/gokv/util"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrMultipleKVNotSupported = errors.New("multiple kv pair not supported")
)

type Options struct {
	BucketName string
	Path       string
	Codec      encoding.Codec
}

var DefaultOptions = Options{
	BucketName: "default",
	Path:       "bbolt.db",
	Codec:      encoding.JSON,
}

type Store struct {
	db         *bolt.DB
	bucketName string
	codec      encoding.Codec
}

func NewStore(options Options) (Store, error) {
	result := Store{}

	// Set default values
	if options.BucketName == "" {
		options.BucketName = DefaultOptions.BucketName
	}
	if options.Path == "" {
		options.Path = DefaultOptions.Path
	}
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	// Open DB
	db, err := bolt.Open(options.Path, 0600, nil)
	if err != nil {
		return result, err
	}

	// Create a bucket if it doesn't exist yet.
	// In bbolt key/value pairs are stored to and read from buckets.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(options.BucketName))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return result, err
	}

	result.db = db
	result.bucketName = options.BucketName
	result.codec = options.Codec

	return result, nil
}

func (s Store) Set(input types.SetItemInput) error {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return err
	}

	data, err := s.codec.Marshal(input.Value)
	if err != nil {
		return err
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		return b.Put([]byte(input.Key), data)
	})
	if err != nil {
		return err
	}
	return nil
}

// boltdb batch operations work on single kv pair
// but across multiple go routines. As a result, in this case
// we cannot accept multiple kv pairs.
func (s Store) BatchSet(input types.BatchSetItemInput) error {
	if len(input.Keys) > 1 {
		return ErrMultipleKVNotSupported
	}

	switch reflect.TypeOf(input.Values).Kind() {
	case reflect.Slice:
		values := reflect.ValueOf(input.Values)
		if values.Len() > 1 {
			return ErrMultipleKVNotSupported
		}
	}

	value := reflect.ValueOf(input.Values).Index(0).Interface()
	if err := util.CheckKeyAndValue(input.Keys[0], value); err != nil {
		return err
	}

	data, err := s.codec.Marshal(value)
	if err != nil {
		return err
	}

	err = s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		return b.Put([]byte(input.Keys[0]), data)
	})
	if err != nil {
		return err
	}
	return nil
}

func (s Store) Get(input types.GetItemInput) (found bool, err error) {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return false, err
	}

	var data []byte
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		txData := b.Get([]byte(input.Key))
		if txData != nil {
			data = append([]byte{}, txData...)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	if data == nil {
		return false, nil
	}

	return true, s.codec.Unmarshal(data, input.Value)
}

func (s Store) Delete(input types.DeleteItemInput) error {
	if err := util.CheckKey(input.Key); err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		return b.Delete([]byte(input.Key))
	})
}

func (s Store) Close() error {
	return s.db.Close()
}
