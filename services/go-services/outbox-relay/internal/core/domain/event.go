package domain

import (
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

type Event struct {
	ID            string
	EventType     platform.EventType
	AggregateType platform.AggregateType
	AggregateID   string
	Payload       []byte
	CorrelationID string
	OccurredAt    time.Time
	PublishTopic  string
}
