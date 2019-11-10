package bbolt

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/simar7/gokv/types"

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
	assert.NoError(t, s.Set(types.SetItemInput{Key: "foo", Value: "bar"}))

	// close
	assert.NoError(t, s.Close())
}

func TestStore_BatchSet(t *testing.T) {
	s, f, err := setupStore()
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(t, err)

	// batch set
	var wg sync.WaitGroup
	for i := 0; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			assert.NoError(t, s.BatchSet(types.BatchSetItemInput{
				Keys:   []string{fmt.Sprintf("foo%d", i)},
				Values: []string{"bar"},
			}))
			wg.Done()
		}(i)
	}
	wg.Wait()

	// check for set values
	for i := 0; i <= 5; i++ {
		var actualOutput string
		found, err := s.Get(types.GetItemInput{
			Key:   fmt.Sprintf("foo%d", i),
			Value: &actualOutput,
		})
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "bar", actualOutput)
	}

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
	assert.NoError(t, s.Set(types.SetItemInput{
		Key:   "foo",
		Value: "bar",
	}))

	// get
	var actualOutput string
	found, err := s.Get(types.GetItemInput{
		Key:   "foo",
		Value: &actualOutput,
	})
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
	assert.NoError(t, s.Set(types.SetItemInput{
		Key:   "foo",
		Value: "bar",
	}))

	// delete
	assert.NoError(t, s.Delete(types.DeleteItemInput{Key: "foo"}))

	// close
	assert.NoError(t, s.Close())
}
