package process_payment

import (
	"transactional-outbox/application/event"
	"transactional-outbox/application/gateway/payment"
	"transactional-outbox/domain/events"
)

type (
	ProcessPaymentUseCase struct {
		eventEmitter   event.Emitter
		paymentGateway payment.Gateway
	}

	Input struct {
		PurchaseId         string
		Amount             float64
		CardNumber         string
		CardHolderName     string
		CardExpirationDate string
		CardCVV            string
	}
)

func New(eventEmitter event.Emitter, paymentGateway payment.Gateway) *ProcessPaymentUseCase {
	return &ProcessPaymentUseCase{eventEmitter: eventEmitter, paymentGateway: paymentGateway}
}

func (uc *ProcessPaymentUseCase) Execute(input Input) error {
	paymentInput := payment.Input{
		CardNumber:         input.CardNumber,
		CardHolderName:     input.CardHolderName,
		CardExpirationDate: input.CardExpirationDate,
		CardCVV:            input.CardCVV,
		Amount:             input.Amount,
	}
	paymentOutput, err := uc.paymentGateway.Pay(paymentInput)
	if err != nil {
		return uc.eventEmitter.Emit(events.NewPaymentFailedEvent(input.PurchaseId, err.Error()))
	}
	return uc.eventEmitter.Emit(events.NewPaymentProcessedEvent(input.PurchaseId, paymentOutput.TransactionId))
}
