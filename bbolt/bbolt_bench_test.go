package bbolt

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/simar7/gokv/encoding"

	"github.com/simar7/gokv/types"

	"github.com/stretchr/testify/assert"
)

func benchmarkSet(j int, b *testing.B) {
	b.ReportAllocs()

	s, f, err := setupStoreWithCodec(encoding.JSON)
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for i := 0; i <= j; i++ {
			wg.Add(1)
			go func(i int) {
				assert.NoError(b, s.Set(types.SetItemInput{
					Key:        fmt.Sprintf("foo%d", i),
					Value:      "bar",
					BucketName: "testing",
				}))
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
	b.StopTimer()

	assert.NoError(b, s.Close())
}

func benchmarkBatchSet(j int, b *testing.B) {
	b.ReportAllocs()

	s, f, err := setupStoreWithCodec(encoding.JSON)
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// batch set
		var wg sync.WaitGroup
		for i := 0; i <= j; i++ {
			wg.Add(1)
			go func(i int) {
				assert.NoError(b, s.BatchSet(types.BatchSetItemInput{
					Keys:       []string{fmt.Sprintf("foo%d", i)},
					Values:     "bar",
					BucketName: "testing",
				}))
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
	b.StopTimer()

	assert.NoError(b, s.Close())
}

func BenchmarkStore_Set_10(b *testing.B) {
	benchmarkSet(10, b)
}

func BenchmarkStore_BatchSet_10(b *testing.B) {
	benchmarkBatchSet(10, b)
}

func BenchmarkStore_Set_100(b *testing.B) {
	benchmarkSet(100, b)
}

func BenchmarkStore_BatchSet_100(b *testing.B) {
	benchmarkBatchSet(100, b)
}

func BenchmarkStore_Set_1000(b *testing.B) {
	benchmarkSet(1000, b)
}

func BenchmarkStore_BatchSet_1000(b *testing.B) {
	benchmarkBatchSet(1000, b)
}

//func BenchmarkStore_Set_10000(b *testing.B) {
//	benchmarkSet(10000, b)
//}
//
//func BenchmarkStore_BatchSet_10000(b *testing.B) {
//	benchmarkBatchSet(10000, b)
//}
