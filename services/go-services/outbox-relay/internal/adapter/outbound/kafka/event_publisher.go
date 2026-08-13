package kafka

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type EventPublisher struct {
	writer        *kafka.Writer
	inFlightBytes atomic.Int64 // total bytes submitted to WriteMessages awaiting ACK
	mu            sync.Mutex
	perShardBytes map[string]*atomic.Int64 // bytes per shard awaiting ACK
}

var _ port.EventPublisher = (*EventPublisher)(nil)

func NewEventPublisher(writer *kafka.Writer) *EventPublisher {
	return &EventPublisher{
		writer:        writer,
		perShardBytes: make(map[string]*atomic.Int64),
	}
}

func (p *EventPublisher) getShardCounter(shardID string) *atomic.Int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.perShardBytes[shardID]; ok {
		return c
	}
	c := &atomic.Int64{}
	p.perShardBytes[shardID] = c
	return c
}

// InFlightBytes returns the total bytes currently awaiting Kafka ACK.
func (p *EventPublisher) InFlightBytes() int64 {
	return p.inFlightBytes.Load()
}

// InFlightBytesForShard returns bytes awaiting ACK for the given shard.
func (p *EventPublisher) InFlightBytesForShard(shardID string) int64 {
	p.mu.Lock()
	c, ok := p.perShardBytes[shardID]
	p.mu.Unlock()
	if !ok {
		return 0
	}
	return c.Load()
}

func (p *EventPublisher) PublishBatch(ctx context.Context, shardID string, events []domain.Event) ([]string, error) {
	if len(events) == 0 {
		return []string{}, nil
	}

	messages := make([]kafka.Message, 0, len(events))
	var eventIDs []string
	for _, e := range events {
		valueBytes := e.Payload

		key := []byte(domain.DeriveKafkaKey(e))

		// Stamp each message with the event ID as a header so consumers can
		// dedup across producer retries. A retry of the same event will land
		// in the same partition and consumers can detect the duplicate via
		// the event_id header.

		headers := []kafka.Header{
			{Key: platform.HeaderEventID, Value: []byte(e.ID)},
			{Key: platform.HeaderEventType, Value: []byte(e.EventType)},
		}

		// Extract Traceparent from protobuf envelope
		var envelope eventsv1.EventEnvelope
		if err := proto.Unmarshal(valueBytes, &envelope); err == nil && envelope.Traceparent != "" {
			headers = append(headers, kafka.Header{
				Key:   platform.TraceparentHeader,
				Value: []byte(envelope.Traceparent),
			})
		}

		msg := kafka.Message{
			Topic:   e.PublishTopic,
			Key:     key,
			Value:   valueBytes,
			Headers: headers,
		}

		messages = append(messages, msg)
		eventIDs = append(eventIDs, e.ID)
	}

	var bytes int64
	for _, m := range messages {
		bytes += int64(len(m.Value))
	}
	p.inFlightBytes.Add(bytes)
	shardCounter := p.getShardCounter(shardID)
	shardCounter.Add(bytes)
	defer p.inFlightBytes.Add(-bytes)
	defer shardCounter.Add(-bytes)

	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return eventIDs, err
	}
	return eventIDs, nil
}
