package redis

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

func TestNewStore(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		mr, err := miniredis.Run()
		assert.NoError(t, err)

		s, err := NewStore(Options{
			Address: mr.Addr(),
		})

		assert.NoError(t, err)
		assert.Equal(t, 10000, s.p.MaxActive)
		assert.Equal(t, 80, s.p.MaxIdle)
	})

	t.Run("sad path, ping fails", func(t *testing.T) {
		s, err := NewStore(Options{
			Address: "path/to/nowhere:1234",
		})
		assert.Equal(t, "redis initialization failed: dial tcp: lookup path/to/nowhere: no such host", err.Error())
		assert.Equal(t, Store{}, s)
	})
}
