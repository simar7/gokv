package bbolt

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	h "github.com/dustin/go-humanize"

	"github.com/simar7/gokv/encoding"
	"github.com/simar7/gokv/types"
	"github.com/simar7/gokv/util"
	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
)

func setupStoreWithCodec(codec encoding.Codec) (*Store, *os.File, error) {
	f, err := ioutil.TempFile(".", "Bolt_TestStore_Get-*")
	if err != nil {
		return nil, nil, err
	}

	s, err := NewStore(Options{Path: f.Name(), Codec: codec})
	return s, f, err
}

func setupStore() (*Store, *os.File, error) {
	return setupStoreWithCodec(encoding.JSON)
}

func TestNewStore(t *testing.T) {
	testCases := []struct {
		name                string
		existingDB          bool
		inputRootBucketName string
	}{
		{
			name: "happy path with no existing DB",
		},
		{
			name:       "happy with with existing DB",
			existingDB: true,
		},
	}

	for _, tc := range testCases {
		d, _ := ioutil.TempDir("", "TestNewStoreDir-*")
		f, _ := ioutil.TempFile(d, "TestNewStore-*.db")
		defer func() {
			_ = os.RemoveAll(d)
		}()

		inputOptions := Options{
			Path: f.Name(),
		}

		if tc.existingDB {
			tempDB, err := bolt.Open(f.Name(), 0600, nil)
			assert.NoError(t, err)
			inputOptions.DB = tempDB
		}

		s, err := NewStore(inputOptions)
		assert.NoError(t, err, tc.name)
		//assert.NotNil(t, s)
		assert.Equal(t, f.Name(), s.dbPath, tc.name)
	}

}

func TestStore_GetStoreOptions(t *testing.T) {
	s, f, err := setupStore()
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(t, err)

	so := s.GetStoreOptions()
	assert.NotNil(t, so.DB)
	assert.Equal(t, f.Name(), so.Path)
	assert.Equal(t, encoding.JSON, s.codec)
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
	t.Run("happy path", func(t *testing.T) {
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
					Values:     "bar",
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
	})

	t.Run("sad paths", func(t *testing.T) {
		testCases := []struct {
			name          string
			inputKeys     []string
			inputValues   interface{}
			expectedError error
		}{
			{
				name:          "multiple keys",
				inputKeys:     []string{"key1", "key2"},
				expectedError: ErrMultipleKVNotSupported,
			},
			// TODO: Add more sad paths
		}

		for _, tc := range testCases {
			s, f, err := setupStore()
			defer func() {
				_ = f.Close()
				_ = os.RemoveAll(f.Name())
			}()
			assert.NoError(t, err)
			assert.Equal(t, ErrMultipleKVNotSupported, s.BatchSet(types.BatchSetItemInput{
				BucketName: "batchbucket",
				Keys:       tc.inputKeys,
				Values:     tc.inputValues,
			}), tc.name)
		}
	})
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

func TestStore_Scan(t *testing.T) {

	t.Run("happy path", func(t *testing.T) {
		s, f, err := setupStore()
		defer func() {
			_ = f.Close()
			_ = os.RemoveAll(f.Name())
		}()
		assert.NoError(t, err)

		assert.NoError(t, s.Set(types.SetItemInput{
			Key:        "foo1",
			Value:      "bar1",
			BucketName: "scanbucket",
		}))

		assert.NoError(t, s.Set(types.SetItemInput{
			Key:        "foo2",
			Value:      "bar2",
			BucketName: "scanbucket",
		}))

		scanOut, err := s.Scan(types.ScanInput{BucketName: "scanbucket"})
		assert.NoError(t, err)

		assert.Equal(t, types.ScanOutput{
			Keys:   []string{"foo1", "foo2"},
			Values: [][]byte{{0x22, 0x62, 0x61, 0x72, 0x31, 0x22}, {0x22, 0x62, 0x61, 0x72, 0x32, 0x22}},
		}, scanOut)
	})

	t.Run("sad path: bucket not found", func(t *testing.T) {
		s, f, err := setupStore()
		defer func() {
			_ = f.Close()
			_ = os.RemoveAll(f.Name())
		}()
		assert.NoError(t, err)

		scanOut, err := s.Scan(types.ScanInput{BucketName: "badbucket"})
		assert.Equal(t, ErrBucketNotFound, err)
		assert.Empty(t, scanOut)
	})

	t.Run("sad path: bucket name empty", func(t *testing.T) {
		scanOut, err := Store{}.Scan(types.ScanInput{})
		assert.Equal(t, util.ErrEmptyBucketName, err)
		assert.Empty(t, scanOut)
	})
}

func TestStore_Info(t *testing.T) {
	s, f, err := setupStore()
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(f.Name())
	}()
	assert.NoError(t, err)

	actualInfo, err := s.Info()
	assert.NoError(t, err)
	assert.Equal(t, "gokvbbolt", actualInfo.Name)
	assert.Equal(t, "32 KiB", h.IBytes(uint64(actualInfo.Size)))
}

func TestStore_DeleteBucket(t *testing.T) {
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
		BucketName: "subbucket",
	}))

	// delete bucket
	assert.NoError(t, s.DeleteBucket(types.DeleteBucketInput{
		BucketName: "subbucket"},
	))

	// verify
	var actualValue string
	found, err := s.Get(types.GetItemInput{
		BucketName: "subbucket",
		Key:        "foo",
		Value:      &actualValue,
	})
	assert.Equal(t, ErrBucketNotFound, err)
	assert.False(t, found)
	assert.Empty(t, actualValue)

	t.Run("sad path, empty input bucket name", func(t *testing.T) {
		s, f, err := setupStore()
		defer func() {
			_ = f.Close()
			_ = os.RemoveAll(f.Name())
		}()
		assert.NoError(t, err)
		assert.Equal(t, util.ErrEmptyBucketName, s.DeleteBucket(types.DeleteBucketInput{}))
	})

	t.Run("sad path, bucket not found", func(t *testing.T) {
		s, f, err := setupStore()
		defer func() {
			_ = f.Close()
			_ = os.RemoveAll(f.Name())
		}()
		assert.NoError(t, err)
		assert.Equal(t, ErrBucketNotFound, s.DeleteBucket(types.DeleteBucketInput{
			BucketName: "badbucket",
		}))
	})
}

func TestStore_Reap(t *testing.T) {
	testCases := []struct {
		name          string
		ttl           time.Duration
		expectedValue string
	}{
		{
			name:          "happy path, item expires",
			ttl:           time.Nanosecond * 1,
			expectedValue: "",
		},
		//{
		//	name:          "happy path, item does not expire",
		//	ttl:           time.Hour * 1,
		//	expectedValue: "bar",
		//},
		//{
		//	name:          "happy path, ttl not set",
		//	expectedValue: "bar",
		//},
	}

	for _, tc := range testCases {
		s, f, err := setupStore()
		defer func() {
			_ = f.Close()
			_ = os.RemoveAll(f.Name())
		}()
		assert.NoError(t, err, tc.name)
		s.ttl = tc.ttl

		// set
		//assert.NoError(t, s.Set(types.SetItemInput{
		//	Key:        "foo",
		//	Value:      "bar",
		//	BucketName: "reapbucket",
		//}), tc.name)

		assert.NoError(t, s.BatchSet(types.BatchSetItemInput{
			Keys:       []string{"foo"},
			Values:     "bar",
			BucketName: "reapbucket",
		}), tc.name)

		// reap expired items
		assert.NoError(t, s.Reap("reapbucket"), tc.name)

		// get
		var actualOutput string
		_, err = s.Get(types.GetItemInput{
			Key:        "foo",
			Value:      &actualOutput,
			BucketName: "reapbucket",
		})
		assert.NoError(t, err, tc.name)
		assert.Equal(t, tc.expectedValue, actualOutput, tc.name)
	}
}
