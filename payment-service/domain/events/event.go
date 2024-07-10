package events

import "github.com/google/uuid"

type Event struct {
	ID      string            `json:"id,omitempty"`
	Name    string            `json:"name,omitempty"`
	Payload map[string]string `json:"payload,omitempty"`
}

func NewPaymentProcessedEvent(purchaseId, transactionId string) *Event {
	return &Event{
		ID:   uuid.NewString(),
		Name: "PAYMENT_PROCESSED",
		Payload: map[string]string{
			"purchaseId":    purchaseId,
			"transactionId": transactionId,
		},
	}
}

func NewPaymentFailedEvent(purchaseId, reason string) *Event {
	return &Event{
		ID:   uuid.NewString(),
		Name: "PAYMENT_FAILED",
		Payload: map[string]string{
			"purchaseId": purchaseId,
			"reason":     reason,
		},
	}
}
