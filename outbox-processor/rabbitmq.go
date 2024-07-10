package main

import (
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
)

type RabbitMqEventEmitter struct {
	connection      *amqp.Connection
	producerChannel *amqp.Channel
}

func NewRabbitMqEventEmitter(server string) EventEmitter {
	connection, err := amqp.Dial(server)
	if err != nil {
		panic(err)
	}
	producerChannel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	return &RabbitMqEventEmitter{
		connection:      connection,
		producerChannel: producerChannel,
	}
}

func (e *RabbitMqEventEmitter) Emit(event *Event) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		slog.Error("Error on emit event", "event", event, "error", err)
		return err
	}
	err = e.producerChannel.Publish(
		"amq.direct",
		event.Name,
		false,
		false,
		amqp.Publishing{ContentType: "text/plain", Body: eventBytes},
	)
	if err != nil {
		slog.Error("Error on publish event", "event", event, "error", err)
		return err
	}
	return nil
}
