package dynamodb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/stretchr/testify/assert"
)

func TestStore_Set(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		expectedRequest := dynamodb.PutItemInput{
			Item: map[string]*dynamodb.AttributeValue{
				keyAttrName: {
					S: aws.String("foo"),
				},
				valAttrName: {
					B: []byte(`"bar"`),
				},
			},
			TableName: aws.String("gokvtesttable"),
		}

		actualRequest := dynamodb.PutItemInput{}
		assert.NoError(t, json.Unmarshal(body, &actualRequest))
		assert.Equal(t, expectedRequest, actualRequest)

		switch r.Method {
		case http.MethodPost:
			_, _ = fmt.Fprint(w, dynamodb.PutItemOutput{})
		default:
			assert.Fail(t, "invalid http method called: ", r.Method)
		}

	}))
	defer ts.Close()

	s, err := NewStore(Options{
		Region:             "ca-test-1",
		TableName:          "gokvtesttable",
		CustomEndpoint:     ts.URL,
		AWSAccessKeyID:     "fookey",
		AWSSecretAccessKey: "barsecretkey",
	})
	assert.NoError(t, err)
	assert.NoError(t, s.Set("foo", "bar"))
}
