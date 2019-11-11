dynamo-clean:
	docker rm -f dynamodblocal

dynamo-docker:
	docker run --name dynamodblocal -d -p 8000:8000 -v /tmp/local/dynamodb:/data/ amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -dbPath /data

dynamo-bench:
	-make clean docker
	go test github.com/simar7/gokv/dynamodb -bench .

bolt-bench:
	go test github.com/simar7/gokv/bbolt -bench .