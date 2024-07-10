package main

type Event struct {
	ID      string            `json:"id,omitempty" bson:"id,omitempty"`
	Name    string            `json:"name,omitempty" bson:"name,omitempty"`
	Payload map[string]string `json:"payload,omitempty" bson:"payload,omitempty"`
}

type EventEmitter interface {
	Emit(event *Event) error
}
