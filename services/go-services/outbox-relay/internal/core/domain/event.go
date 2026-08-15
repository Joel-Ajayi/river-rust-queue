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
	Headers       map[string]string // For trace propagation (W3C traceparent, baggage)
}

// DeriveKafkaKey determines the Kafka partition key for an event.
func DeriveKafkaKey(e Event) string {
	key := e.AggregateID
	if envelope, err := platform.UnmarshalEnvelope(e.Payload); err == nil {
		if e.PublishTopic == platform.TopicNotify {
			if whDeliv := envelope.GetWebhookDelivered(); whDeliv != nil {
				key = whDeliv.MerchantId
			} else if whFail := envelope.GetWebhookFailed(); whFail != nil {
				key = whFail.MerchantId
			}
		}
	}
	return key
}
