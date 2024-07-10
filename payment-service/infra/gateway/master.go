package gateway

import (
	"errors"
	"github.com/google/uuid"
	"transactional-outbox/application/gateway/payment"
)

type MasterCardPaymentGateway struct{}

func (m *MasterCardPaymentGateway) Pay(input payment.Input) (*payment.Output, error) {
	if input.Amount <= 20 {
		return nil, errors.New("amount too high")
	}
	return &payment.Output{
		TransactionId: uuid.NewString(),
	}, nil
}
