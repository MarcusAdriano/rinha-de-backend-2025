package model

import (
	"github.com/google/uuid"
	"time"
)

type PaymentParams struct {
	CorrelationID uuid.UUID `json:"correlation_id"`
	RequestedAt   time.Time `json:"requested_at"`
	Amount        float64   `json:"amount"`
}
