package dynamodb

import (
	"testing"

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
	s.c = mockDynamoDB{}

	assert.NoError(t, s.Set("foo", "bar"))
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
	found, err := s.Get("foo", &actualValue)
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

	assert.NoError(t, s.Delete("foo"))
	assert.NoError(t, s.Close())

}
