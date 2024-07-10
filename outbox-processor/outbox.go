package main

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type (
	Outbox struct {
		Id              string     `json:"id" bson:"_id"`
		Name            string     `json:"name" bson:"name"`
		Payload         string     `json:"payload" bson:"payload"`
		Status          string     `json:"status" bson:"status"`
		CreatedAt       time.Time  `json:"created_at" bson:"created_at"`
		ProcessedAt     *time.Time `json:"processed_at" bson:"processed_at"`
		LastAttemptTime *time.Time `json:"last_attempt_time" bson:"last_attempt_time"`
	}

	OutboxRepository interface {
		Update(outbox *Outbox) error
		Get(id string) (*Outbox, error)
	}

	DynamoOutboxRepository struct {
		dynamoClient *dynamodb.DynamoDB
		tableName    string
	}

	MongoOutboxRepository struct {
		collection *mongo.Collection
	}
)

func (o *Outbox) MarkAsError() {
	o.Status = "ERROR"
	now := time.Now()
	o.LastAttemptTime = &now
}

func (o *Outbox) MarkAsProcessed() {
	o.Status = "PROCESSED"
	now := time.Now()
	o.ProcessedAt = &now
}

func NewMongoOutboxRepository(collection *mongo.Collection) *MongoOutboxRepository {
	return &MongoOutboxRepository{collection: collection}
}

func NewDynamoOutboxRepository(dynamoClient *dynamodb.DynamoDB, tableName string) OutboxRepository {
	return &DynamoOutboxRepository{dynamoClient: dynamoClient, tableName: tableName}
}

func (r *DynamoOutboxRepository) Update(outbox *Outbox) error {
	key, err := dynamodbattribute.MarshalMap(map[string]string{"id": outbox.Id})
	if err != nil {
		return err
	}
	update := expression.Set(expression.Name("status"), expression.Value(outbox.Status))
	update.Set(expression.Name("processed_at"), expression.Value(outbox.ProcessedAt))
	update.Set(expression.Name("last_attempt_time"), expression.Value(outbox.LastAttemptTime))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return err
	}
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(r.tableName),
		Key:                       key,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	}
	_, err = r.dynamoClient.UpdateItem(input)
	return err
}

func (r *DynamoOutboxRepository) Get(id string) (*Outbox, error) {
	key, err := dynamodbattribute.MarshalMap(map[string]string{"id": id})
	if err != nil {
		return nil, err
	}
	item, err := r.dynamoClient.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key:       key,
	})
	if err != nil {
		return nil, err
	}
	var outbox Outbox
	err = dynamodbattribute.UnmarshalMap(item.Item, &outbox)
	if err != nil {
		return nil, err
	}
	if outbox.Id == "" {
		return nil, nil
	}
	return &outbox, nil
}

func (r *MongoOutboxRepository) Update(outbox *Outbox) error {
	update := bson.M{
		"$set": bson.M{
			"status":            outbox.Status,
			"processed_at":      outbox.ProcessedAt,
			"last_attempt_time": outbox.LastAttemptTime,
		},
	}
	_, err := r.collection.UpdateByID(context.TODO(), outbox.Id, update)
	return err
}

func (r *MongoOutboxRepository) Get(id string) (*Outbox, error) {
	result := r.collection.FindOne(context.TODO(), bson.M{"_id": id})
	if result.Err() != nil {
		return nil, result.Err()
	}
	outbox := Outbox{}
	err := result.Decode(&outbox)
	if err != nil {
		return nil, err
	}
	return &outbox, nil
}
