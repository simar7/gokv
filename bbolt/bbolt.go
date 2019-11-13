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
	ErrBucketNotFound         = errors.New("bucket not found")
)

type Options struct {
	DB             *bolt.DB
	RootBucketName string
	Path           string
	Codec          encoding.Codec
}

var DefaultOptions = Options{
	RootBucketName: "gokvbbolt",
	Path:           "bbolt.db",
	Codec:          encoding.JSON,
}

type RootBucketConfig struct {
	Name   string
	Bucket *bolt.Bucket
}

type Store struct {
	db         *bolt.DB
	rbc        RootBucketConfig
	bucketName string
	codec      encoding.Codec
}

func NewStore(options Options) (*Store, error) {
	result := Store{}

	// Set default values
	if options.RootBucketName == "" {
		options.RootBucketName = DefaultOptions.RootBucketName
	}
	if options.Path == "" {
		options.Path = DefaultOptions.Path
	}
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	if options.DB == nil {
		// Open DB
		var err error
		options.DB, err = bolt.Open(options.Path, 0600, nil)
		if err != nil {
			return nil, err
		}
	}

	result.db = options.DB
	err := result.db.Update(func(tx *bolt.Tx) error {
		var err error
		if result.rbc.Bucket, err = tx.CreateBucketIfNotExists([]byte(options.RootBucketName)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result.rbc.Name = options.RootBucketName
	result.codec = options.Codec
	return &result, nil
}

func (s *Store) createBucketIfNotExists(bucketName string) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists([]byte(s.rbc.Name))
		if err != nil {
			return err
		}

		_, err = root.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s Store) Set(input types.SetItemInput) error {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return err
	}

	err := s.createBucketIfNotExists(input.BucketName)
	if err != nil {
		return err
	}

	data, err := s.codec.Marshal(input.Value)
	if err != nil {
		return err
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		if b = tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(input.BucketName)); b == nil { // Untested
			return ErrBucketNotFound
		}
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

	// TODO: This check currently slows down the perf of BatchSet() by ~80%
	// If we can guarantee bucket existance before writing, we can avoid this.
	err := s.createBucketIfNotExists(input.BucketName)
	if err != nil {
		return err
	}

	data, err := s.codec.Marshal(value)
	if err != nil {
		return err
	}

	err = s.db.Batch(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		if b = tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(input.BucketName)); b == nil { // Untested
			return ErrBucketNotFound
		}
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
		var b *bolt.Bucket
		if b = tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(input.BucketName)); b == nil {
			return ErrBucketNotFound
		}
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
		var b *bolt.Bucket
		if b = tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(input.BucketName)); b == nil {
			return ErrBucketNotFound
		}
		return b.Delete([]byte(input.Key))
	})
}

func (s Store) Close() error {
	return s.db.Close()
}
