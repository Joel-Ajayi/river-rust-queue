package kafka

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/encoding/protojson"
)

type EventPublisher struct {
	writer *kafka.Writer
}

var _ port.EventPublisher = (*EventPublisher)(nil)

func NewEventPublisher(writer *kafka.Writer) *EventPublisher {
	return &EventPublisher{writer: writer}
}

func (p *EventPublisher) PublishEvents(ctx context.Context, events []domain.Event) ([]string, error) {
	if len(events) == 0 {
		return []string{}, nil
	}

	messages := make([]kafka.Message, 0, len(events))
	var eventIDs []string
	for _, e := range events {
		valueBytes := e.Payload

		key := []byte(e.AggregateID)

		// For webhook notifications, we must key by merchant_id to guarantee per-merchant ordering (I5).
		if e.PublishTopic == platform.TopicNotify {
			var envelope eventsv1.EventEnvelope
			if err := protojson.Unmarshal(e.Payload, &envelope); err == nil {
				if whDeliv := envelope.GetWebhookDelivered(); whDeliv != nil {
					key = []byte(whDeliv.MerchantId)
				} else if whFail := envelope.GetWebhookFailed(); whFail != nil {
					key = []byte(whFail.MerchantId)
				}
			}
		}

		msg := kafka.Message{
			Topic: e.PublishTopic,
			Key:   key,
			Value: valueBytes,
		}

		messages = append(messages, msg)
		eventIDs = append(eventIDs, e.ID)
	}

	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return eventIDs, err
	}
	return eventIDs, nil
}
