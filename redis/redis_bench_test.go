package redis

import (
	"fmt"
	"sync"
	"testing"

	"github.com/simar7/gokv/encoding"

	"github.com/simar7/gokv/types"
	"github.com/stretchr/testify/assert"
)

// launch a docker container with redis
// docker run -p 6379:6379 redis
// bump up regular macos defaults
// sudo sysctl -w kern.maxfiles=1048600
// sudo sysctl -w kern.maxfilesperproc=1048576
// sudo sysctl -w kern.ipc.somaxconn=102400
// ulimit -S -n 1048576
const redisEndpoint = "127.0.0.1:6379"

func benchmarkSet(j int, b *testing.B) {
	b.ReportAllocs()

	s, err := NewStore(Options{
		Address: redisEndpoint,
		Codec:   encoding.Gob,
	})
	if !assert.NoError(b, err) {
		assert.FailNow(b, err.Error())
	}
	defer s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for i := 0; i <= j; i++ {
			wg.Add(1)
			go func(i int) {
				assert.NoError(b, s.Set(types.SetItemInput{
					Key:   fmt.Sprintf("foo%d", i),
					Value: "bar",
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

	s, err := NewStore(Options{
		Address: redisEndpoint,
	})
	if !assert.NoError(b, err) {
		assert.FailNow(b, err.Error())
	}
	defer s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// batch set
		var wg sync.WaitGroup
		for i := 0; i <= j; i++ {
			wg.Add(1)
			go func(i int) {
				assert.NoError(b, s.BatchSet(types.BatchSetItemInput{
					Keys:   []string{fmt.Sprintf("foo%d", i)},
					Values: []string{fmt.Sprintf("bar%d", i)},
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

func BenchmarkStore_Set_120(b *testing.B) {
	benchmarkSet(120, b)
}

func BenchmarkStore_BatchSet_120(b *testing.B) {
	benchmarkBatchSet(120, b)
}
