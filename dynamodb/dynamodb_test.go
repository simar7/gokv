package dynamodb

import (
	"testing"

	"github.com/simar7/gokv/types"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/stretchr/testify/assert"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	putItem    func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	getItem    func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	deleteItem func(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
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

func TestStore_Set(t *testing.T) {
	s, err := NewStore(Options{
		Region:         "ca-test-1",
		TableName:      "gokvtesttable",
		CustomEndpoint: "https://foo.bar/test",
	})
	assert.NoError(t, err)
	s.c = mockDynamoDB{putItem: func(input *dynamodb.PutItemInput) (output *dynamodb.PutItemOutput, e error) {
		assert.Equal(t, "foo", *input.Item[keyAttrName].S)
		assert.Equal(t, []byte(`"bar"`), input.Item[valAttrName].B)
		return &dynamodb.PutItemOutput{}, nil
	}}

	assert.NoError(t, s.Set(types.SetItemInput{Key: "foo", Value: "bar"}))
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
			assert.Equal(t, "foo", *input.Key[keyAttrName].S)
			return &dynamodb.GetItemOutput{
				Item: map[string]*dynamodb.AttributeValue{
					keyAttrName: {
						S: aws.String("foo"),
					},
					valAttrName: {
						B: []byte(`"bar"`),
					},
				},
			}, nil
		},
	}

	var actualValue string
	found, err := s.Get(types.GetItemInput{Key: "foo", Value: &actualValue})
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
		assert.Equal(t, "foo", *input.Key[keyAttrName].S)
		return &dynamodb.DeleteItemOutput{}, nil
	}}

	assert.NoError(t, s.Delete(types.DeleteItemInput{Key: "foo"}))
	assert.NoError(t, s.Close())

}
