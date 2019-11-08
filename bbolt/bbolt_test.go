package bbolt

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupStore() (Store, *os.File, error) {
	f, err := ioutil.TempFile("", "Bolt_TestStore_Get-*")
	if err != nil {
		return Store{}, nil, err
	}

	s, err := NewStore(Options{Path: f.Name()})
	return s, f, err
}

func TestStore_Set(t *testing.T) {
	s, f, err := setupStore()
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(t, err)

	// set
	assert.NoError(t, s.Set("foo", "bar"))

	// close
	assert.NoError(t, s.Close())
}

func TestStore_Get(t *testing.T) {
	s, f, err := setupStore()
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(t, err)

	// set
	assert.NoError(t, s.Set("foo", "bar"))

	// get
	var actualOutput string
	found, err := s.Get("foo", &actualOutput)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "bar", actualOutput)

	// close
	assert.NoError(t, s.Close())
}

func TestStore_Delete(t *testing.T) {
	s, f, err := setupStore()
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(t, err)

	// set
	assert.NoError(t, s.Set("foo", "bar"))

	// delete
	assert.NoError(t, s.Delete("foo"))

	// close
	assert.NoError(t, s.Close())
}
