package bbolt

import (
	"bytes"
	"errors"
	"os"
	"time"

	"github.com/simar7/gokv/types"

	"github.com/simar7/gokv/encoding"
	"github.com/simar7/gokv/util"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrMultipleKVNotSupported = errors.New("multiple kv pair not supported")
	ErrBucketNotFound         = errors.New("bucket not found")
	ErrBucketCreationFailed   = errors.New("bucket creation failed")
)

type Options struct {
	DB             *bolt.DB
	RootBucketName string
	Path           string
	Codec          encoding.Codec
	ItemTTL        time.Duration
}

var DefaultOptions = Options{
	RootBucketName: "gokvbbolt",
	Path:           "bbolt.db",
	Codec:          encoding.JSON,
	ItemTTL:        -1, // indicates items never expire
}

type RootBucketConfig struct {
	Name   string
	Bucket *bolt.Bucket
}

type Store struct {
	db         *bolt.DB
	dbPath     string
	rbc        RootBucketConfig
	bucketName string
	codec      encoding.Codec
	ttl        time.Duration
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
	if options.ItemTTL == 0 {
		options.ItemTTL = DefaultOptions.ItemTTL
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
	result.dbPath = options.Path
	result.codec = options.Codec
	result.ttl = options.ItemTTL
	return &result, nil
}

func (s *Store) GetStoreOptions() Options {
	return Options{
		DB:             s.db,
		RootBucketName: s.rbc.Name,
		Path:           s.dbPath,
		Codec:          s.codec,
	}
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

		if s.ttl > 0 {
			_, err = root.CreateBucketIfNotExists([]byte(bucketName + "_ttlBucket"))
			if err != nil {
				return err
			}
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

	err := s.createBucketIfNotExists(input.BucketName) // TODO: Can we move this inside s.db.Update()?
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

	// set TTL on items if exists
	if s.ttl > 0 {
		err = s.db.Update(func(tx *bolt.Tx) error {
			var b *bolt.Bucket
			if b = tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(input.BucketName + "_ttlBucket")); b == nil { // Untested
				return ErrBucketNotFound
			}
			return b.Put([]byte(time.Now().UTC().Format(time.RFC3339Nano)), []byte(input.Key))
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// boltdb batch operations work on single kv pair
// but across multiple go routines. As a result, in this case
// we cannot accept multiple keys.
func (s Store) BatchSet(input types.BatchSetItemInput) error {
	if len(input.Keys) > 1 {
		return ErrMultipleKVNotSupported
	}

	data, err := s.codec.Marshal(input.Values)
	if err != nil {
		return err
	}

	err = s.db.Batch(func(tx *bolt.Tx) error {
		var b, b2 *bolt.Bucket
		if b = tx.Bucket([]byte(s.rbc.Name)); b == nil { // Untested
			return ErrBucketNotFound
		}

		if b2, err = b.CreateBucketIfNotExists([]byte(input.BucketName)); err != nil { // Untested
			return ErrBucketCreationFailed
		}

		return b2.Put([]byte(input.Keys[0]), data)
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

func (s Store) DeleteBucket(input types.DeleteBucketInput) error {
	if err := util.CheckBucketName(input.BucketName); err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		if b = tx.Bucket([]byte(s.rbc.Name)); b == nil {
			return ErrBucketNotFound
		}
		return b.DeleteBucket([]byte(input.BucketName))
	})
}

func (s Store) Scan(input types.ScanInput) (types.ScanOutput, error) {
	if err := util.CheckBucketName(input.BucketName); err != nil {
		return types.ScanOutput{}, err
	}

	var keys []string
	var values [][]byte

	if err := s.db.View(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		if b = tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(input.BucketName)); b == nil {
			return ErrBucketNotFound
		}

		if err := b.ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))
			values = append(values, v)
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return types.ScanOutput{}, err
	}

	return types.ScanOutput{
		Keys:   keys,
		Values: values,
	}, nil
}

func (s Store) Close() error {
	return s.db.Close()
}

func (s Store) Info() (types.StoreInfo, error) {
	f, err := os.Stat(s.dbPath)
	if err != nil {
		return types.StoreInfo{}, err
	}

	return types.StoreInfo{
		Name: s.rbc.Name,
		Size: f.Size(),
	}, nil
}

// BoltDB does not support builtin item expiration
// Reap takes care of handling TTL for items in BoltDB
func (s Store) Reap(itemBucket string) error {
	if s.ttl <= 0 {
		return nil
	}

	keys, err := s.getExpired(s.ttl, itemBucket+"_ttlBucket")
	if err != nil || len(keys) == 0 {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) (err error) {
		itemB := tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(itemBucket))

		for _, key := range keys {
			if err = itemB.Delete(key); err != nil {
				return
			}
		}
		return
	})
}

func (s Store) getExpired(maxAge time.Duration, ttlBucket string) ([][]byte, error) {
	var keys [][]byte
	var ttlKeys [][]byte

	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(ttlBucket)).Cursor()

		max := []byte(time.Now().UTC().Add(-maxAge).Format(time.RFC3339Nano))
		for k, v := c.First(); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
			keys = append(keys, v)
			ttlKeys = append(ttlKeys, k)
		}
		return nil
	})

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.rbc.Name)).Bucket([]byte(ttlBucket))
		for _, key := range ttlKeys {
			if err = b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})

	return keys, err
}
