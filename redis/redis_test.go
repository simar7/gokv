package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStore(t *testing.T) {
	s, err := NewStore(Options{
		Address: "someRedisServer:1234",
	})

	assert.NoError(t, err)
	assert.Equal(t, 10000, s.p.MaxActive)
	assert.Equal(t, 80, s.p.MaxIdle)
}
