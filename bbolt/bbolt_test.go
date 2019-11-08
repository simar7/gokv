package bbolt

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore_Set(t *testing.T) {
	f, _ := ioutil.TempFile("", "Bolt_TestStore_Get-*")
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()

	s, err := NewStore(Options{Path: f.Name()})
	assert.NoError(t, err)
	assert.NoError(t, s.Set("foo", "bar"))
}

func TestStore_Get(t *testing.T) {
	f, _ := ioutil.TempFile("", "Bolt_TestStore_Get-*")
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()

	s, err := NewStore(Options{Path: f.Name()})
	assert.NoError(t, err)
	assert.NoError(t, s.Set("foo", "bar"))

	var actualOutput string
	found, err := s.Get("foo", &actualOutput)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "bar", actualOutput)
}
