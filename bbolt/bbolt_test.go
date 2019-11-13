package bbolt

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/simar7/gokv/util"

	"github.com/simar7/gokv/types"

	"github.com/stretchr/testify/assert"
)

func setupStore() (*Store, *os.File, error) {
	f, err := ioutil.TempFile(".", "Bolt_TestStore_Get-*")
	if err != nil {
		return nil, nil, err
	}

	s, err := NewStore(Options{Path: f.Name()})
	return s, f, err
}

func TestStore_Set(t *testing.T) {
	testCases := []struct {
		name          string
		inputBucket   string
		expectedError error
	}{
		{
			name:        "happy path",
			inputBucket: "setbucket",
		},
		// TODO: Add sad paths
	}

	for _, tc := range testCases {
		s, f, err := setupStore()
		defer func() {
			_ = f.Close()
			_ = os.RemoveAll(f.Name())
		}()
		assert.NoError(t, err)

		// set
		assert.Equal(t, tc.expectedError, s.Set(types.SetItemInput{
			Key:        "foo",
			Value:      "bar",
			BucketName: tc.inputBucket,
		},
		), tc.name)

		// close
		assert.NoError(t, s.Close())
	}

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
				Keys:       []string{fmt.Sprintf("foo%d", i)},
				Values:     []string{"bar"},
				BucketName: "batchsetbucket",
			}))
			wg.Done()
		}(i)
	}
	wg.Wait()

	// check for set values
	for i := 0; i <= 5; i++ {
		var actualOutput string
		found, err := s.Get(types.GetItemInput{
			Key:        fmt.Sprintf("foo%d", i),
			Value:      &actualOutput,
			BucketName: "batchsetbucket",
		})
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "bar", actualOutput)
	}

	// close
	assert.NoError(t, s.Close())
}

func TestStore_Get(t *testing.T) {
	testCases := []struct {
		name           string
		inputBucket    string
		inputKey       string
		expectedValue  string
		valueFound     bool
		expcectedError error
	}{
		{
			name:          "happy path",
			inputBucket:   "getbucket",
			inputKey:      "foo",
			expectedValue: "bar",
			valueFound:    true,
		},
		{
			name:          "happy path: key not found",
			inputBucket:   "getbucket",
			inputKey:      "badkey",
			expectedValue: "",
		},
		{
			name:           "sad path: bucket not found",
			inputBucket:    "badbucket",
			inputKey:       "foo",
			expcectedError: ErrBucketNotFound,
		},
		{
			name:           "sad path: passed key is empty",
			inputKey:       "",
			expcectedError: util.ErrEmptyKey,
		},
	}

	for _, tc := range testCases {
		s, f, err := setupStore()
		defer func() {
			_ = f.Close()
			_ = os.RemoveAll(f.Name())
		}()
		assert.NoError(t, err)

		// set
		assert.NoError(t, s.Set(types.SetItemInput{
			Key:        "foo",
			Value:      "bar",
			BucketName: "getbucket",
		}))

		// get
		var actualOutput string
		found, err := s.Get(types.GetItemInput{
			Key:        tc.inputKey,
			Value:      &actualOutput,
			BucketName: tc.inputBucket,
		})

		switch {
		case tc.expcectedError != nil:
			assert.Equal(t, tc.expcectedError, err, tc.name)
			assert.Empty(t, actualOutput, tc.name)
		default:
			assert.NoError(t, tc.expcectedError, tc.name)
			assert.Equal(t, tc.expectedValue, actualOutput, tc.name)
		}
		assert.Equal(t, tc.valueFound, found, tc.name)
		assert.NoError(t, s.Close())
	}
}

func TestStore_Delete(t *testing.T) {
	testCases := []struct {
		name          string
		inputBucket   string
		inputKey      string
		expectedError error
	}{
		{
			name:        "happy path",
			inputBucket: "deletebucket",
			inputKey:    "foo",
		},
		{
			name:          "sad path: input bucket not found",
			inputBucket:   "badinputbucket",
			inputKey:      "foo",
			expectedError: ErrBucketNotFound,
		},
		{
			name:        "sad path: input key not found",
			inputBucket: "deletebucket",
			inputKey:    "badkey",
		},
		{
			name:          "sad path: input key empty",
			inputBucket:   "deletebucket",
			inputKey:      "",
			expectedError: util.ErrEmptyKey,
		},
	}

	for _, tc := range testCases {
		s, f, err := setupStore()
		defer func() {
			_ = f.Close()
			_ = os.RemoveAll(f.Name())
		}()
		assert.NoError(t, err)

		// set
		assert.NoError(t, s.Set(types.SetItemInput{
			Key:        "foo",
			Value:      "bar",
			BucketName: "deletebucket",
		}))

		// delete
		assert.Equal(t, tc.expectedError, s.Delete(types.DeleteItemInput{
			Key: tc.inputKey, BucketName: tc.inputBucket}), tc.name)

		// close
		assert.NoError(t, s.Close())
	}

}
