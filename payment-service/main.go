package main

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log/slog"
	"transactional-outbox/application/usecase/process_payment"
	"transactional-outbox/infra/events"
	"transactional-outbox/infra/gateway"
	"transactional-outbox/infra/repository"
)

const (
	TableName           = "outbox_events"
	AwsClientId         = "test"
	AwsClientSecret     = "test"
	AwsToken            = "test"
	AwsEndpoint         = "http://localhost:4566"
	AwsRegion           = "us-east-1"
	MongoServer         = "mongodb://localhost:27017"
	MongoDatabaseName   = "outbox"
	MongoCollectionName = "events"
)

func main() {
	outboxRepository := mongoOutboxRepository()
	outboxEventEmitter := events.NewOutboxEventEmitter(outboxRepository)
	paymentGateway := &gateway.VisaPaymentGateway{}
	processPayment := process_payment.New(outboxEventEmitter, paymentGateway)
	input := process_payment.Input{
		PurchaseId:         uuid.NewString(),
		Amount:             10,
		CardNumber:         "1234123412341234",
		CardHolderName:     "Any name",
		CardExpirationDate: "10/2024",
		CardCVV:            "123",
	}
	err := processPayment.Execute(input)
	if err != nil {
		slog.Error("Payment process is failed", err)
		return
	}
	slog.Info("Payment process is done")
}

func mongoOutboxRepository() repository.OutboxRepository {
	clientOptions := options.Client().ApplyURI(MongoServer)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		panic(err)
	}
	collection := client.Database(MongoDatabaseName).Collection(MongoCollectionName)
	return repository.NewMongoOutboxRepository(collection)
}

func dynamoOutboxRepository() repository.OutboxRepository {
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
	return repository.NewDynamoDBOutboxRepository(TableName, dynamoClient)
}
