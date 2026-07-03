package domain

import "time"

type Event struct {
	ID            string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	Payload       []byte    `json:"payload"`
	CorrelationID string    `json:"correlation_id"`
	OccurredAt    time.Time `json:"occurred_at"`
	PublishTopic  string    `json:"publish_topic"`
}
