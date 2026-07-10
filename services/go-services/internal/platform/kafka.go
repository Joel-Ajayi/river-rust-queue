package platform

import (
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	// Publish topics
	TopicJobs                 = "jobs"
	TopicJobsRetry            = "jobs.retry"
	TopicNotify               = "notify"
	TopicXShardPrefix         = "xshard."
	TopicXShardRetryPrefix    = "xshard.retry."
	ConsumerGroupLedgerWorker = "ledger-worker"
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
	kafkaLog := log.Named(LogComponentKafka)
	kafkaLog.Info("Created Kafka writer", zap.String(LogFieldEvent, LogEventKafkaWriterCreated), zap.String(LogFieldTopic, topic))
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
	kafkaLog := log.Named(LogComponentKafka)
	kafkaLog.Info("Created Kafka reader", zap.String(LogFieldEvent, LogEventKafkaReaderCreated), zap.String(LogFieldTopic, topic), zap.String(LogFieldGroup, groupID))
	return r
}
