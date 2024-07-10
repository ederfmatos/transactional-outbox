package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"time"
)

type DynamoStream struct {
	dynamoStreamClient *dynamodbstreams.DynamoDBStreams
	awsSession         *session.Session
	tableName          string
	dynamoDB           *dynamodb.DynamoDB
}

func NewDynamoStream(awsSession *session.Session, tableName string, dynamoDB *dynamodb.DynamoDB) OutboxStream {
	return &DynamoStream{
		dynamoStreamClient: dynamodbstreams.New(awsSession),
		dynamoDB:           dynamoDB,
		awsSession:         awsSession,
		tableName:          tableName,
	}
}

func (stream *DynamoStream) getStreamArn() (string, error) {
	result, err := stream.dynamoDB.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(stream.tableName)})
	if err != nil {
		return "", err
	}
	if result.Table.StreamSpecification != nil && *result.Table.StreamSpecification.StreamEnabled {
		return *result.Table.LatestStreamArn, nil
	}
	return "", fmt.Errorf("streams not enabled for table %s", stream.tableName)
}

func (stream *DynamoStream) FetchEvents() (chan string, error) {
	streamArn, err := stream.getStreamArn()
	if err != nil {
		return nil, err
	}
	events := make(chan string)
	describeStreamInput := &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(streamArn)}
	describeStreamOutput, err := stream.dynamoStreamClient.DescribeStream(describeStreamInput)
	if err != nil {
		return nil, err
	}
	for _, shard := range describeStreamOutput.StreamDescription.Shards {
		go stream.processShard(*shard.ShardId, events, streamArn)
	}
	return events, nil
}

func (stream *DynamoStream) processShard(shardID string, events chan<- string, streamArn string) {
	shardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamArn),
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
	}

	shardIteratorOutput, err := stream.dynamoStreamClient.GetShardIterator(shardIteratorInput)
	if err != nil {
		return
	}

	ShardIterator := shardIteratorOutput.ShardIterator
	backoff := time.Second

	for {
		getRecordsInput := &dynamodbstreams.GetRecordsInput{ShardIterator: ShardIterator}
		records, err := stream.dynamoStreamClient.GetRecords(getRecordsInput)
		if err != nil {
			continue
		}

		for _, record := range records.Records {
			id := record.Dynamodb.NewImage["id"].S
			status := record.Dynamodb.NewImage["status"].S
			if *record.EventName == "INSERT" {
				backoff = time.Second
				events <- *id
			} else if *record.EventName == "MODIFY" && *status == "ERROR" {
				backoff = time.Second
				go func(id string) {
					time.Sleep(5 * time.Second)
					events <- id
				}(*id)
			}
		}
		ShardIterator = records.NextShardIterator
		time.Sleep(backoff)

		if backoff < 30*time.Second {
			backoff *= 2
		} else {
			backoff = 30 * time.Second
		}
	}
}
