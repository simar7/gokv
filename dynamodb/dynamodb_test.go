package dynamodb

import (
	"errors"
	"testing"

	"github.com/simar7/gokv/util"

	"github.com/simar7/gokv/types"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/stretchr/testify/assert"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	putItem        func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	getItem        func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	deleteItem     func(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	batchWriteItem func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	scan           func(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

func (md mockDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if md.putItem != nil {
		return md.putItem(input)
	}

	return &dynamodb.PutItemOutput{}, nil
}

func (md mockDynamoDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if md.getItem != nil {
		return md.getItem(input)
	}

	return &dynamodb.GetItemOutput{}, nil
}

func (md mockDynamoDB) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	if md.deleteItem != nil {
		return md.deleteItem(input)
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

func (md mockDynamoDB) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	if md.batchWriteItem != nil {
		return md.batchWriteItem(input)
	}

	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (md mockDynamoDB) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	if md.scan != nil {
		return md.scan(input)
	}

	return &dynamodb.ScanOutput{}, nil
}

func TestStore_Set(t *testing.T) {
	s, err := NewStore(Options{
		Region:         "ca-test-1",
		TableName:      "gokvtesttable",
		CustomEndpoint: "https://foo.bar/test",
	})
	assert.NoError(t, err)
	s.c = mockDynamoDB{putItem: func(input *dynamodb.PutItemInput) (output *dynamodb.PutItemOutput, e error) {
		assert.Equal(t, "testing", *input.TableName)
		assert.Equal(t, "foo", *input.Item[KeyAttrName].S)
		assert.Equal(t, []byte(`"bar"`), input.Item[ValAttrName].B)
		return &dynamodb.PutItemOutput{}, nil
	}}

	assert.NoError(t, s.Set(types.SetItemInput{
		Key:        "foo",
		Value:      "bar",
		BucketName: "testing",
	}))
	assert.NoError(t, s.Close())
}

func TestStore_Get(t *testing.T) {
	s, err := NewStore(Options{
		Region:         "ca-test-1",
		TableName:      "gokvtesttable",
		CustomEndpoint: "https://foo.bar/test",
	})
	assert.NoError(t, err)
	s.c = mockDynamoDB{
		getItem: func(input *dynamodb.GetItemInput) (output *dynamodb.GetItemOutput, e error) {
			assert.Equal(t, "testing", *input.TableName)
			assert.Equal(t, "foo", *input.Key[KeyAttrName].S)
			return &dynamodb.GetItemOutput{
				Item: map[string]*dynamodb.AttributeValue{
					KeyAttrName: {
						S: aws.String("foo"),
					},
					ValAttrName: {
						B: []byte(`"bar"`),
					},
				},
			}, nil
		},
	}

	var actualValue string
	found, err := s.Get(types.GetItemInput{
		Key:        "foo",
		Value:      &actualValue,
		BucketName: "testing",
	})
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "bar", actualValue)
	assert.NoError(t, s.Close())
}

func TestStore_Delete(t *testing.T) {
	s, err := NewStore(Options{
		Region:         "ca-test-1",
		TableName:      "gokvtesttable",
		CustomEndpoint: "https://foo.bar/test",
	})
	assert.NoError(t, err)
	s.c = mockDynamoDB{deleteItem: func(input *dynamodb.DeleteItemInput) (output *dynamodb.DeleteItemOutput, e error) {
		assert.Equal(t, "testing", *input.TableName)
		assert.Equal(t, "foo", *input.Key[KeyAttrName].S)
		return &dynamodb.DeleteItemOutput{}, nil
	}}

	assert.NoError(t, s.Delete(types.DeleteItemInput{Key: "foo", BucketName: "testing"}))
	assert.NoError(t, s.Close())

}

func TestStore_BatchSet(t *testing.T) {
	s, err := NewStore(Options{
		Region:         "ca-test-1",
		TableName:      "gokvtesttable",
		CustomEndpoint: "https://foo.bar/test",
	})
	assert.NoError(t, err)
	s.c = mockDynamoDB{
		batchWriteItem: func(input *dynamodb.BatchWriteItemInput) (output *dynamodb.BatchWriteItemOutput, e error) {
			assert.Equal(t, 1, len(input.RequestItems))

			var inputKey string
			for k := range input.RequestItems { // since there's only one key
				inputKey = k
			}
			assert.Equal(t, "testing", inputKey)

			assert.Equal(t, []*dynamodb.WriteRequest{
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							KeyAttrName: {
								S: aws.String("foo"),
							},
							ValAttrName: {
								B: []byte(`"bar"`),
							},
						},
					},
				},
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							KeyAttrName: {
								S: aws.String("faz"),
							},
							ValAttrName: {
								B: []byte(`"baz"`),
							},
						},
					},
				},
			}, input.RequestItems[inputKey])
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	assert.NoError(t, s.BatchSet(types.BatchSetItemInput{
		Keys:       []string{"foo", "faz"},
		Values:     []string{"bar", "baz"},
		BucketName: "testing",
	}))
	assert.NoError(t, s.Close())

}

func TestStore_Scan(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		s, err := NewStore(Options{
			Region:         "ca-test-1",
			TableName:      "gokvtesttable",
			CustomEndpoint: "https://foo.bar/test",
		})
		assert.NoError(t, err)
		s.c = mockDynamoDB{
			scan: func(input *dynamodb.ScanInput) (output *dynamodb.ScanOutput, e error) {
				return &dynamodb.ScanOutput{
					Items: []map[string]*dynamodb.AttributeValue{
						{
							KeyAttrName: &dynamodb.AttributeValue{
								SS: []*string{aws.String("key1")},
							},
							ValAttrName: &dynamodb.AttributeValue{
								BS: [][]byte{[]byte("val1")},
							},
						},
						{
							KeyAttrName: &dynamodb.AttributeValue{
								SS: []*string{aws.String("key2")},
							},
							ValAttrName: &dynamodb.AttributeValue{
								BS: [][]byte{[]byte("val2")},
							},
						},
					},
				}, nil
			},
		}

		output, err := s.Scan(types.ScanInput{BucketName: "scanbucket"})
		assert.NoError(t, err)
		assert.Equal(t, types.ScanOutput{
			Keys:   []string{"key1", "key2"},
			Values: [][]byte{{0x76, 0x61, 0x6c, 0x31}, {0x76, 0x61, 0x6c, 0x32}},
		}, output)
	})

	t.Run("sad path: missing bucket name", func(t *testing.T) {
		so, err := Store{}.Scan(types.ScanInput{})
		assert.Equal(t, util.ErrEmptyBucketName, err)
		assert.Empty(t, so)
	})

	t.Run("sad path: dynamodb scan fails", func(t *testing.T) {
		s, err := NewStore(Options{
			Region:         "ca-test-1",
			TableName:      "gokvtesttable",
			CustomEndpoint: "https://foo.bar/test",
		})
		assert.NoError(t, err)

		s.c = mockDynamoDB{
			scan: func(input *dynamodb.ScanInput) (output *dynamodb.ScanOutput, e error) {
				return nil, errors.New("dynamodb scan failed")
			}}

		so, err := s.Scan(types.ScanInput{BucketName: "testbucket"})
		assert.Equal(t, "dynamodb scan failed", err.Error())
		assert.Empty(t, so)
	})
}
