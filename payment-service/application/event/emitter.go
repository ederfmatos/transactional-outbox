package event

import "transactional-outbox/domain/events"

type Emitter interface {
	Emit(event *events.Event) error
}
