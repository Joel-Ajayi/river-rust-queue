package kafka

import (
	"context"
	"encoding/json"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/segmentio/kafka-go"
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

	//
	messages := make([]kafka.Message, 0, len(events))
	eventIDs := make([]string, 0, len(events))
	for _, e := range events {
		// We encode the entire Event struct as JSON so downstream consumers have all the context
		valueBytes, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}

		msg := kafka.Message{
			Topic: e.PublishTopic,
			Key:   []byte(e.AggregateID),
			Value: valueBytes,
		}

		messages = append(messages, msg)
		eventIDs = append(eventIDs, e.ID)
	}

	// Send the entire batch at once for performance
	err := p.writer.WriteMessages(ctx, messages...)
	return eventIDs, err
}
