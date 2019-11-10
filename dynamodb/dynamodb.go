package dynamodb

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/simar7/gokv/encoding"
	"github.com/simar7/gokv/util"
)

const (
	keyAttrName = "k"
	valAttrName = "v"
)

var (
	ErrMissingTableName = errors.New("table name is required")
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

func (s Store) Set(k string, v interface{}) error {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	data, err := s.codec.Marshal(v)
	if err != nil {
		return err
	}

	item := make(map[string]*awsdynamodb.AttributeValue)
	item[keyAttrName] = &awsdynamodb.AttributeValue{
		S: &k,
	}
	item[valAttrName] = &awsdynamodb.AttributeValue{
		B: data,
	}
	putItemInput := awsdynamodb.PutItemInput{
		TableName: &s.tableName,
		Item:      item,
	}
	_, err = s.c.PutItem(&putItemInput)
	if err != nil {
		return err
	}
	return nil
}

func (s Store) BatchSet(k string, v interface{}) error {
	panic("implement me")
}

func (s Store) Get(k string, v interface{}) (found bool, err error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return false, err
	}

	key := make(map[string]*awsdynamodb.AttributeValue)
	key[keyAttrName] = &awsdynamodb.AttributeValue{
		S: &k,
	}
	getItemInput := awsdynamodb.GetItemInput{
		TableName: &s.tableName,
		Key:       key,
	}
	getItemOutput, err := s.c.GetItem(&getItemInput)
	if err != nil {
		return false, err
	} else if getItemOutput.Item == nil {
		return false, nil
	}
	attributeVal := getItemOutput.Item[valAttrName]
	if attributeVal == nil {
		return false, nil
	}
	data := attributeVal.B

	return true, s.codec.Unmarshal(data, v)
}

func (s Store) Delete(k string) error {
	if err := util.CheckKey(k); err != nil {
		return err
	}

	key := make(map[string]*awsdynamodb.AttributeValue)
	key[keyAttrName] = &awsdynamodb.AttributeValue{
		S: &k,
	}

	deleteItemInput := awsdynamodb.DeleteItemInput{
		TableName: aws.String(s.tableName),
		Key:       key,
	}
	_, err := s.c.DeleteItem(&deleteItemInput)
	return err
}

func (s Store) Close() error {
	return nil
}
