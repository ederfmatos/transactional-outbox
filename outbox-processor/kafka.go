package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log/slog"
)

type KafkaEventEmitter struct {
	writer *kafka.Writer
}

func NewKafkaEventEmitter(brokers []string, topic string) *KafkaEventEmitter {
	return &KafkaEventEmitter{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (k *KafkaEventEmitter) Emit(event *Event) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		slog.Error("Error on emit event", "event", event, "error", err)
		return err
	}
	message := kafka.Message{
		Value: eventBytes,
		Topic: event.Name,
		Key:   []byte(event.ID),
	}
	return k.writer.WriteMessages(context.Background(), message)
}
