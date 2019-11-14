package dynamodb

import (
	"errors"
	"reflect"

	"github.com/simar7/gokv/types"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/simar7/gokv/encoding"
	"github.com/simar7/gokv/util"
)

var (
	KeyAttrName = "k"
	ValAttrName = "v"
)

var (
	ErrMissingTableName = errors.New("table name is required")
	ErrNotImplemented   = errors.New("function not implemented")
)

type Options struct {
	Region             string
	TableName          string
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
	Codec              encoding.Codec
	CustomEndpoint     string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
}

var DefaultOptions = Options{
	ReadCapacityUnits:  5,
	WriteCapacityUnits: 5,
	Codec:              encoding.JSON,
}

type Store struct {
	c         dynamodbiface.DynamoDBAPI
	tableName string
	codec     encoding.Codec
}

func NewStore(options Options) (Store, error) {
	result := Store{}

	if options.TableName == "" {
		return result, ErrMissingTableName
	}

	if options.ReadCapacityUnits == 0 {
		options.ReadCapacityUnits = DefaultOptions.ReadCapacityUnits
	}

	if options.WriteCapacityUnits == 0 {
		options.WriteCapacityUnits = DefaultOptions.WriteCapacityUnits
	}

	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	creds := credentials.NewStaticCredentials(options.AWSAccessKeyID, options.AWSSecretAccessKey, "")

	config := aws.NewConfig()
	if options.Region != "" {
		config = config.WithRegion(options.Region)
	}
	if creds != nil {
		config = config.WithCredentials(creds)
	}
	if options.CustomEndpoint != "" {
		config = config.WithEndpoint(options.CustomEndpoint)
	}
	sessionOpts := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}
	sessionOpts.Config.MergeIn(config)
	awsSession, err := session.NewSessionWithOptions(sessionOpts)
	if err != nil {
		return result, err
	}

	result.c = awsdynamodb.New(awsSession)
	result.tableName = options.TableName
	result.codec = options.Codec

	return result, nil
}

func (s Store) Set(input types.SetItemInput) error {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return err
	}

	data, err := s.codec.Marshal(input.Value)
	if err != nil {
		return err
	}

	item := make(map[string]*awsdynamodb.AttributeValue)
	item[KeyAttrName] = &awsdynamodb.AttributeValue{
		S: &input.Key,
	}
	item[ValAttrName] = &awsdynamodb.AttributeValue{
		B: data,
	}
	putItemInput := awsdynamodb.PutItemInput{
		TableName: aws.String(input.BucketName),
		Item:      item,
	}
	_, err = s.c.PutItem(&putItemInput)
	if err != nil {
		return err
	}
	return nil
}

func (s Store) BatchSet(input types.BatchSetItemInput) error {
	var datas [][]byte
	var writeRequests []*awsdynamodb.WriteRequest

	for i := 0; i < len(input.Keys); i++ {
		if err := util.CheckKeyAndValue(input.Keys[i], input.Values); err != nil {
			return err
		}

		data, err := s.codec.Marshal(reflect.ValueOf(input.Values).Index(i).Interface())
		if err != nil {
			return err
		}
		datas = append(datas, data)

		writeRequests = append(writeRequests, &awsdynamodb.WriteRequest{
			PutRequest: &awsdynamodb.PutRequest{
				Item: map[string]*awsdynamodb.AttributeValue{
					KeyAttrName: {
						S: aws.String(input.Keys[i]),
					},
					ValAttrName: {
						B: datas[i],
					},
				},
			},
		})
	}

	batchItemInput := &awsdynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*awsdynamodb.WriteRequest{
			input.BucketName: writeRequests,
		},
	}

	_, err := s.c.BatchWriteItem(batchItemInput)
	if err != nil {
		return err
	}

	return nil
}

func (s Store) Get(input types.GetItemInput) (found bool, err error) {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return false, err
	}

	key := make(map[string]*awsdynamodb.AttributeValue)
	key[KeyAttrName] = &awsdynamodb.AttributeValue{
		S: &input.Key,
	}
	getItemInput := awsdynamodb.GetItemInput{
		TableName: aws.String(input.BucketName),
		Key:       key,
	}
	getItemOutput, err := s.c.GetItem(&getItemInput)
	if err != nil {
		return false, err
	} else if getItemOutput.Item == nil {
		return false, nil
	}
	attributeVal := getItemOutput.Item[ValAttrName]
	if attributeVal == nil {
		return false, nil
	}
	data := attributeVal.B

	return true, s.codec.Unmarshal(data, input.Value)
}

func (s Store) Delete(input types.DeleteItemInput) error {
	if err := util.CheckKey(input.Key); err != nil {
		return err
	}

	key := make(map[string]*awsdynamodb.AttributeValue)
	key[KeyAttrName] = &awsdynamodb.AttributeValue{
		S: &input.Key,
	}

	deleteItemInput := awsdynamodb.DeleteItemInput{
		TableName: aws.String(input.BucketName),
		Key:       key,
	}
	_, err := s.c.DeleteItem(&deleteItemInput)
	return err
}

func (s Store) Close() error {
	return nil
}

func (s Store) Scan(input types.ScanInput) (types.ScanOutput, error) {
	return types.ScanOutput{}, ErrNotImplemented
}
