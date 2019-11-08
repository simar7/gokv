package bbolt

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)


func benchmarkSet(j int, b *testing.B){
	s, f, err := setupStore()
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(b, err)

	for i:=0; i<b.N; i++{
		// batch set
		var wg sync.WaitGroup
		for i:=0; i<=j; i++ {
			wg.Add(1)
			go func(i int) {
				assert.NoError(b, s.Set(fmt.Sprintf("foo%d", i), "bar"))
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
	assert.NoError(b, s.Close())
}

func benchmarkBatchSet(j int, b *testing.B){
	s, f, err := setupStore()
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(b, err)

	for i:=0; i<b.N; i++{
		// batch set
		var wg sync.WaitGroup
		for i:=0; i<=j; i++ {
			wg.Add(1)
			go func(i int) {
				assert.NoError(b, s.BatchSet(fmt.Sprintf("foo%d", i), "bar"))
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
	assert.NoError(b, s.Close())
}

func BenchmarkStore_Set_10(b *testing.B) {
	b.ReportAllocs()
	benchmarkSet(10, b)
}

func BenchmarkStore_BatchSet_10(b *testing.B) {
	b.ReportAllocs()
	benchmarkBatchSet(10, b)
}

func BenchmarkStore_Set_100(b *testing.B) {
	b.ReportAllocs()
	benchmarkSet(100, b)
}

func BenchmarkStore_BatchSet_100(b *testing.B) {
	b.ReportAllocs()
	benchmarkBatchSet(100, b)
}

func BenchmarkStore_Set_1000(b *testing.B) {
	b.ReportAllocs()
	benchmarkSet(1000, b)
}

func BenchmarkStore_BatchSet_1000(b *testing.B) {
	b.ReportAllocs()
	benchmarkBatchSet(1000, b)
}

func BenchmarkStore_Set_10000(b *testing.B) {
	b.ReportAllocs()
	benchmarkSet(10000, b)
}

func BenchmarkStore_BatchSet_10000(b *testing.B) {
	b.ReportAllocs()
	benchmarkBatchSet(10000, b)
}