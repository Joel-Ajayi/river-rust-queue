package domain

import (
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"google.golang.org/protobuf/proto"
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
	var envelope eventsv1.EventEnvelope
	if err := proto.Unmarshal(e.Payload, &envelope); err == nil {
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
