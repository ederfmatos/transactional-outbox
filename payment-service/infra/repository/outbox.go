package repository

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type (
	Outbox struct {
		Id        string    `json:"id" bson:"_id"`
		Name      string    `json:"name" bson:"name"`
		Payload   string    `json:"payload" bson:"payload"`
		Status    string    `json:"status" bson:"status"`
		CreatedAt time.Time `json:"created_at" bson:"created_at"`
	}

	OutboxRepository interface {
		Save(outbox *Outbox) error
	}

	mongoOutboxRepository struct {
		collection *mongo.Collection
	}

	dynamoDBOutboxRepository struct {
		tableName    string
		dynamoClient *dynamodb.DynamoDB
	}
)

func NewOutbox(id, name, payload string) *Outbox {
	return &Outbox{
		Id:        id,
		Name:      name,
		Payload:   payload,
		Status:    "PENDING",
		CreatedAt: time.Now(),
	}
}

func NewDynamoDBOutboxRepository(tableName string, dynamoClient *dynamodb.DynamoDB) OutboxRepository {
	return &dynamoDBOutboxRepository{tableName: tableName, dynamoClient: dynamoClient}
}

func NewMongoOutboxRepository(collection *mongo.Collection) OutboxRepository {
	return &mongoOutboxRepository{collection: collection}
}

func (r *mongoOutboxRepository) Save(outbox *Outbox) error {
	_, err := r.collection.InsertOne(context.TODO(), outbox)
	return err
}

func (r *dynamoDBOutboxRepository) Save(outbox *Outbox) error {
	item, err := dynamodbattribute.MarshalMap(outbox)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      item,
	}
	_, err = r.dynamoClient.PutItem(input)
	return err
}
