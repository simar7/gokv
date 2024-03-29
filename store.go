package gokv

import "github.com/simar7/gokv/types"

type Store interface {
	Set(input types.SetItemInput) error
	BatchSet(input types.BatchSetItemInput) error
	Get(input types.GetItemInput) (found bool, err error)
	Delete(input types.DeleteItemInput) error
	DeleteBucket(input types.DeleteBucketInput) error
	Close() error
	Scan(input types.ScanInput) (types.ScanOutput, error)
	Info() (types.StoreInfo, error)
}
