package platform

import (
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	// Publish topics
	TopicJobs            = "jobs"
	TopicNotify          = "notify"
	TopicXShardPrefix    = "xshard."
	TopicSuffixRequested = "requested"
	TopicSuffixSettled   = "settled"
	TopicSuffixFailed    = "failed"

	TopicXShardRequested = TopicXShardPrefix + TopicSuffixRequested
	TopicXShardSettled   = TopicXShardPrefix + TopicSuffixSettled
	TopicXShardFailed    = TopicXShardPrefix + TopicSuffixFailed

	// consumer groups
	ConsumerGroupLedgerWorker  = "ledger-worker-"
	ConsumerGroupWebhookWorker = "webhook-worker"
	ConsumerGroupFraudWorker   = "fraud-worker"

	// Kafka reader default buffer limits
	KafkaReaderMinBytes = 1
	KafkaReaderMaxBytes = 10e6

	// Label for xshard topics in metrics
	TopicLabelXShard = "xshard"
)

// NewKafkaWriter creates a synchronous Kafka writer for the given topic.
func NewKafkaWriter(brokers []string, topic string, log *zap.Logger) *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: true,
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
		MinBytes: KafkaReaderMinBytes,
		MaxBytes: KafkaReaderMaxBytes,
	})
	kafkaLog := log.Named(LogComponentKafka)
	kafkaLog.Info("Created Kafka reader", zap.String(LogFieldEvent, LogEventKafkaReaderCreated), zap.String(LogFieldTopic, topic), zap.String(LogFieldGroup, groupID))
	return r
}
