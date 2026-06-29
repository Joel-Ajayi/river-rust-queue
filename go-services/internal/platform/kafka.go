package platform

import (
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// NewKafkaWriter creates a synchronous Kafka writer for the given topic.
func NewKafkaWriter(brokers []string, topic string, log *zap.Logger) *kafka.Writer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	log.Info("kafka writer created", zap.String("topic", topic))
	return w
}

// NewKafkaReader creates a Kafka consumer for the given topic and group.
func NewKafkaReader(brokers []string, topic, groupID string, log *zap.Logger) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	log.Info("kafka reader created", zap.String("topic", topic), zap.String("group", groupID))
	return r
}
