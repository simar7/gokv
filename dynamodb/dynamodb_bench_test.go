package dynamodb_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws/endpoints"
	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/stretchr/testify/assert"

	"github.com/simar7/gokv/types"

	"github.com/simar7/gokv/dynamodb"
	"github.com/simar7/gokv/encoding"
)

// Launch a docker container in the background
// docker run -p 8000:8000 -v $(pwd)/local/dynamodb:/data/ amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -dbPath /data
// Create a dynamodb table for testing
// aws dynamodb create-table --attribute-definitions AttributeName="k",AttributeType="S" --key-schema=AttributeName="k",KeyType="HASH" --table-name="gokvtesting" --provisioned-throughput=ReadCapacityUnits=5,WriteCapacityUnits=5 --endpoint-url="http://localhost:8000"
const customEndpoint = "http://localhost:8000"
const dynamoDBBatchLimit = 25

type Value struct {
	v string
}

func createTable(tableName string, readCapacityUnits, writeCapacityUnits int64, waitForTableCreation bool, describeTableInput awsdynamodb.DescribeTableInput, svc *awsdynamodb.DynamoDB) error {
	keyAttrType := "S" // For "string"
	keyType := "HASH"  // As opposed to "RANGE"
	createTableInput := awsdynamodb.CreateTableInput{
		TableName: &tableName,
		AttributeDefinitions: []*awsdynamodb.AttributeDefinition{{
			AttributeName: &dynamodb.KeyAttrName,
			AttributeType: &keyAttrType,
		}},
		KeySchema: []*awsdynamodb.KeySchemaElement{{
			AttributeName: &dynamodb.KeyAttrName,
			KeyType:       &keyType,
		}},
		ProvisionedThroughput: &awsdynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  &readCapacityUnits,
			WriteCapacityUnits: &writeCapacityUnits,
		},
	}
	_, err := svc.CreateTable(&createTableInput)
	if err != nil {
		return err
	}
	// If configured (true by default), block until the table is created.
	// Typical table creation duration is 10 seconds.
	if waitForTableCreation {
		for try := 1; try < 16; try++ {
			describeTableOutput, err := svc.DescribeTable(&describeTableInput)
			if err != nil || *describeTableOutput.Table.TableStatus == "CREATING" {
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
		// Last try (16th) after 15 seconds of waiting.
		// Now handle error as such.
		describeTableOutput, err := svc.DescribeTable(&describeTableInput)
		if err != nil {
			return err
		}
		//if err != nil {
		//	return errors.New("The DynamoDB table couldn't be created")
		//}
		if *describeTableOutput.Table.TableStatus == "CREATING" {
			return errors.New("dynamodb table took too long to be created")
		}
	}

	return nil
}

func createStore(b *testing.B, tableName string, codec encoding.Codec) (*dynamodb.Store, error) {
	options := dynamodb.Options{
		Region:             endpoints.UsWest2RegionID,
		CustomEndpoint:     customEndpoint,
		Codec:              codec,
		TableName:          tableName,
		AWSSecretAccessKey: "fookey",
		AWSAccessKeyID:     "barsecretkey",
		ReadCapacityUnits:  5,
		WriteCapacityUnits: 5,
	}
	creds := credentials.NewStaticCredentials(options.AWSAccessKeyID, options.AWSSecretAccessKey, "")

	config := aws.NewConfig()
	config = config.WithRegion(options.Region).WithEndpoint(options.CustomEndpoint).WithCredentials(creds)

	sessionOpts := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}
	sessionOpts.Config.MergeIn(config)
	awsSession, err := session.NewSessionWithOptions(sessionOpts)
	if err != nil {
		return nil, err
	}

	svc := awsdynamodb.New(awsSession)
	// Create table if it doesn't exist.
	// Also serves as connection test.
	// Use context for timeout.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	describeTableInput := awsdynamodb.DescribeTableInput{
		TableName: &options.TableName,
	}
	_, err = svc.DescribeTableWithContext(timeoutCtx, &describeTableInput)
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if !ok {
			return nil, err
		} else if awsErr.Code() == awsdynamodb.ErrCodeResourceNotFoundException {
			err = createTable(options.TableName, options.ReadCapacityUnits, options.WriteCapacityUnits, true, describeTableInput, svc)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	store, err := dynamodb.NewStore(options)
	assert.NoError(b, err)

	return &store, nil
}

func benchmarkSet(j int, b *testing.B) {
	b.ReportAllocs()
	uniqRunID := rand.New(rand.NewSource(time.Now().UnixNano())).Float64()*10 + 10
	tableName := fmt.Sprintf("benchtesting%f", uniqRunID)

	s, err := createStore(b, tableName, encoding.JSON)
	if err != nil {
		b.Fatalf(err.Error())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		totalEntries := dynamoDBBatchLimit + (dynamoDBBatchLimit * (j / dynamoDBBatchLimit))
		var wg sync.WaitGroup
		for i := 0; i <= j; i++ {
			wg.Add(1)
			go func(i int) {
				for k := 0; k < totalEntries; k++ {
					assert.NoError(b, s.Set(types.SetItemInput{
						Key:        fmt.Sprintf("foo%d%d%f", i, k, uniqRunID),
						Value:      Value{v: fmt.Sprintf("bar%d", i)},
						BucketName: tableName,
					},
					))
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
	b.StopTimer()

	// cleanup
	for i := 0; i < j; i++ {
		for k := 0; k < dynamoDBBatchLimit; k++ {
			assert.NoError(b, s.Delete(types.DeleteItemInput{
				Key:        fmt.Sprintf("foo%d%d%f", i, k, uniqRunID),
				BucketName: tableName,
			}))
		}
	}
}

func benchmarkBatchSet(j int, b *testing.B) {
	b.ReportAllocs()
	uniqRunID := rand.New(rand.NewSource(time.Now().UnixNano())).Float64()*10 + 10
	tableName := fmt.Sprintf("benchtesting%f", uniqRunID)

	s, err := createStore(b, tableName, encoding.JSON)
	if err != nil {
		b.Fatalf(err.Error())
	}
	numGoroutinesSplits := int(j / dynamoDBBatchLimit) // 25 is the limit of dynamodb batch request input
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for g := 0; g <= numGoroutinesSplits; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				var batchKeys []string
				var batchValues []Value
				for i := 0; i < dynamoDBBatchLimit; i++ {
					batchKeys = append(batchKeys, fmt.Sprintf("foo%d%d%f", i, g, uniqRunID))
					batchValues = append(batchValues, Value{v: fmt.Sprintf("bar%d%d", i, g)})
				}
				assert.NoError(b, s.BatchSet(types.BatchSetItemInput{
					Keys:       batchKeys,
					Values:     batchValues,
					BucketName: fmt.Sprintf("benchtesting%f", uniqRunID),
				}))
			}(g)
		}
		wg.Wait()
	}
	b.StopTimer()

	// cleanup
	for g := 0; g <= numGoroutinesSplits; g++ {
		for i := 0; i < j; i++ {
			assert.NoError(b, s.Delete(types.DeleteItemInput{
				Key:        fmt.Sprintf("foo%d%d%f", i, g, uniqRunID),
				BucketName: fmt.Sprintf("benchtesting%f", uniqRunID),
			}))
		}
	}

}

func BenchmarkStore_Set_10(b *testing.B) {
	benchmarkSet(10, b)
}

func BenchmarkStore_BatchSet_10(b *testing.B) {
	benchmarkBatchSet(10, b)
}

func BenchmarkStore_Set_25(b *testing.B) {
	benchmarkSet(25, b)
}

func BenchmarkStore_BatchSet_25(b *testing.B) {
	benchmarkBatchSet(25, b)
}

func BenchmarkStore_Set_100(b *testing.B) {
	benchmarkSet(100, b)
}

func BenchmarkStore_BatchSet_100(b *testing.B) {
	benchmarkBatchSet(100, b)
}
