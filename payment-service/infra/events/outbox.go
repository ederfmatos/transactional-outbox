package events

import (
	"encoding/json"
	"transactional-outbox/domain/events"
	"transactional-outbox/infra/repository"
)

type OutboxEventEmitter struct {
	outboxRepository repository.OutboxRepository
}

func NewOutboxEventEmitter(outboxRepository repository.OutboxRepository) *OutboxEventEmitter {
	return &OutboxEventEmitter{outboxRepository: outboxRepository}
}

func (d *OutboxEventEmitter) Emit(event *events.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	outbox := repository.NewOutbox(event.ID, event.Name, string(payload))
	return d.outboxRepository.Save(outbox)
}
