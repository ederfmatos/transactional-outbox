package main

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type MongoStream struct {
	collection *mongo.Collection
}

func NewMongoStream(collection *mongo.Collection) *MongoStream {
	return &MongoStream{collection: collection}
}

func (stream *MongoStream) FetchEvents() (chan string, error) {
	ch := make(chan string)
	go stream.consumeExistingEvents(ch)
	go stream.consumeErrorEvents(ch)
	go stream.consumeNewEvents(ch)
	return ch, nil
}

func (stream *MongoStream) consumeExistingEvents(ch chan string) {
	cursor, err := stream.collection.Find(context.TODO(), bson.M{"status": bson.M{"$ne": "PROCESSED"}})
	if err != nil {
		log.Fatalf("Failed to find existing events: %v", err)
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var outbox Outbox
		if err := cursor.Decode(&outbox); err != nil {
			log.Printf("Failed to decode existing outbox: %v", err)
			continue
		}
		ch <- outbox.Id
	}

	if err := cursor.Err(); err != nil {
		log.Printf("Cursor error: %v", err)
	}
}

func (stream *MongoStream) consumeNewEvents(ch chan string) {
	pipeline := mongo.Pipeline{bson.D{{"$match", bson.D{{"operationType", "insert"}}}}}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	changeStream, err := stream.collection.Watch(context.TODO(), pipeline, opts)
	if err != nil {
		log.Fatalf("Failed to start change stream: %v", err)
	}

	defer changeStream.Close(context.TODO())
	defer close(ch)

	for changeStream.Next(context.TODO()) {
		var changeEvent struct {
			DocumentKey primitive.M `bson:"documentKey,omitempty"`
		}
		if err := changeStream.Decode(&changeEvent); err != nil {
			log.Printf("Failed to decode change stream document: %v", err)
			continue
		}
		ch <- changeEvent.DocumentKey["_id"].(string)
	}

	if err := changeStream.Err(); err != nil {
		log.Printf("Change stream error: %v", err)
	}
}

func (stream *MongoStream) consumeErrorEvents(ch chan string) {
	pipeline := mongo.Pipeline{bson.D{
		{"$match", bson.D{
			{"operationType", "update"},
			{"fullDocument.status", "ERROR"},
		}},
	}}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	changeStream, err := stream.collection.Watch(context.TODO(), pipeline, opts)
	if err != nil {
		log.Fatalf("Failed to start change stream: %v", err)
	}

	defer changeStream.Close(context.TODO())
	defer close(ch)

	for changeStream.Next(context.TODO()) {
		var changeEvent struct {
			DocumentKey primitive.M `bson:"documentKey,omitempty"`
		}
		if err := changeStream.Decode(&changeEvent); err != nil {
			log.Printf("Failed to decode change stream document: %v", err)
			continue
		}
		go func() {
			time.Sleep(time.Second * 5)
			ch <- changeEvent.DocumentKey["_id"].(string)
		}()
	}

	if err := changeStream.Err(); err != nil {
		log.Printf("Change stream error: %v", err)
	}
}
