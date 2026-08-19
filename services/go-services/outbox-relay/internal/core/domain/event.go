package domain

import (
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
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
	Headers       map[string]string // For trace propagation (W3C traceparent, baggage)
}

func DeriveKafkaKey(e Event) string {
	key := e.AggregateID
	if envelope, err := platform.UnmarshalEnvelope(e.Payload); err == nil {
		if e.PublishTopic == platform.TopicNotify {
			switch p := envelope.Payload.(type) {
			case *eventsv1.EventEnvelope_TransferCompleted:
				key = p.TransferCompleted.MerchantId
			case *eventsv1.EventEnvelope_TransferFailed:
				key = p.TransferFailed.MerchantId
			case *eventsv1.EventEnvelope_WebhookDelivered:
				key = p.WebhookDelivered.MerchantId
			case *eventsv1.EventEnvelope_WebhookFailed:
				key = p.WebhookFailed.MerchantId
			}
		}
	}
	return key
}
