package main

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	TableName           = "outbox_events"
	AwsClientId         = "test"
	AwsClientSecret     = "test"
	AwsToken            = "test"
	AwsEndpoint         = "http://localhost:4566"
	AwsRegion           = "us-east-1"
	RabbitMqServer      = "amqp://guest:guest@localhost:5672/"
	MongoServer         = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0&readPreference=primary&ssl=false"
	MongoDatabaseName   = "outbox"
	MongoCollectionName = "events"
)

func main() {
	eventEmitter := NewRabbitMqEventEmitter(RabbitMqServer)
	outboxRepository, outboxStream := dynamoOutbox()
	outboxHandler := NewOutboxHandler(outboxRepository, eventEmitter)

	events, err := outboxStream.FetchEvents()
	if err != nil {
		panic(err)
	}

	for id := range events {
		outbox, err := outboxRepository.Get(id)
		if err != nil {
			continue
		}
		outboxHandler.Handle(outbox)
	}
}

func dynamoOutbox() (OutboxRepository, OutboxStream) {
	config := &aws.Config{
		Region:           aws.String(AwsRegion),
		Credentials:      credentials.NewStaticCredentials(AwsClientId, AwsClientSecret, AwsToken),
		Endpoint:         aws.String(AwsEndpoint),
		S3ForcePathStyle: aws.Bool(true),
	}
	awsSession, err := session.NewSession(config)
	if err != nil {
		panic(err)
	}
	dynamoClient := dynamodb.New(awsSession)
	outboxRepository := NewDynamoOutboxRepository(dynamoClient, TableName)
	dynamoStream := NewDynamoStream(awsSession, TableName, dynamoClient)
	return outboxRepository, dynamoStream
}

func mongoOutbox() (OutboxRepository, OutboxStream) {
	clientOptions := options.Client().ApplyURI(MongoServer)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		panic(err)
	}
	collection := client.Database(MongoDatabaseName).Collection(MongoCollectionName)
	mongoStream := NewMongoStream(collection)
	outboxRepository := NewMongoOutboxRepository(collection)
	return outboxRepository, mongoStream
}
