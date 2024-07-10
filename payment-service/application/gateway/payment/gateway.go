package payment

type (
	Input struct {
		CardNumber         string
		CardHolderName     string
		CardExpirationDate string
		CardCVV            string
		Amount             float64
	}

	Output struct {
		TransactionId string
	}

	Gateway interface {
		Pay(payment Input) (*Output, error)
	}
)
