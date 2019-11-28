package leveldb

import (
	"github.com/simar7/gokv/encoding"
	"github.com/simar7/gokv/types"
	"github.com/simar7/gokv/util"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Options struct {
	Path      string
	WriteSync bool
	Codec     encoding.Codec
}

var DefaultOptions = Options{
	Path:  "leveldb",
	Codec: encoding.JSON,
}

type Store struct {
	db        *leveldb.DB
	writeSync bool
	codec     encoding.Codec
}

func (s Store) Set(input types.SetItemInput) error {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return err
	}

	data, err := s.codec.Marshal(input.Value)
	if err != nil {
		return err
	}

	var writeOptions *opt.WriteOptions
	if s.writeSync {
		writeOptions = &opt.WriteOptions{
			Sync: true,
		}
	}
	return s.db.Put([]byte(input.Key), data, writeOptions)
}

func (s Store) BatchSet(input types.BatchSetItemInput) error {
	panic("implement me")
}

func (s Store) Get(input types.GetItemInput) (found bool, err error) {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return false, err
	}

	data, err := s.db.Get([]byte(input.Key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return false, nil
		}
		return false, err
	}

	return true, s.codec.Unmarshal(data, input.Value)
}

func (s Store) Delete(input types.DeleteItemInput) error {
	if err := util.CheckKey(input.Key); err != nil {
		return err
	}

	var writeOptions *opt.WriteOptions
	if s.writeSync {
		writeOptions = &opt.WriteOptions{
			Sync: true,
		}
	}
	return s.db.Delete([]byte(input.Key), writeOptions)
}

func (s Store) Close() error {
	return s.db.Close()
}

func (s Store) Scan(input types.ScanInput) (types.ScanOutput, error) {
	panic("implement me")
}

func NewStore(options Options) (Store, error) {
	result := Store{}

	if options.Path == "" {
		options.Path = DefaultOptions.Path
	}
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	db, err := leveldb.OpenFile(options.Path, nil)
	if err != nil {
		return result, err
	}

	result.db = db
	result.writeSync = options.WriteSync
	result.codec = options.Codec

	return result, nil
}
