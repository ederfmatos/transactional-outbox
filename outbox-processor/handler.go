package main

import (
	"encoding/json"
	"log/slog"
)

type OutboxHandler struct {
	outboxRepository OutboxRepository
	eventEmitter     EventEmitter
}

func NewOutboxHandler(outboxRepository OutboxRepository, eventEmitter EventEmitter) *OutboxHandler {
	return &OutboxHandler{outboxRepository: outboxRepository, eventEmitter: eventEmitter}
}

func (handler OutboxHandler) Handle(outbox *Outbox) {
	if outbox == nil || outbox.Status == "PROCESSED" {
		return
	}
	var messageEvent Event
	err := json.Unmarshal([]byte(outbox.Payload), &messageEvent)
	if err != nil {
		_ = handler.outboxRepository.Update(outbox)
		slog.Error("Error unmarshalling message event: " + err.Error())
		return
	}
	err = handler.eventEmitter.Emit(&messageEvent)
	if err != nil {
		outbox.MarkAsError()
		_ = handler.outboxRepository.Update(outbox)
		return
	}
	outbox.MarkAsProcessed()
	_ = handler.outboxRepository.Update(outbox)
}
