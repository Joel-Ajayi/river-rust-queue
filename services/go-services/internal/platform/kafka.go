package platform

import (
	"time"

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

	// Label for xshard topics in metrics
	TopicLabelXShard = "xshard"
)

func NewKafkaWriter(cfg *Config, brokers []string, topic string, batchSize int, batchTimeout time.Duration, log *zap.Logger) *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Murmur2Balancer{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: true,
		MaxAttempts:            cfg.GlobalCapacity.KafkaWriterMaxAttempts,
	}
	if batchSize > 0 {
		w.BatchSize = batchSize
	}
	if batchTimeout > 0 {
		w.BatchTimeout = batchTimeout
	}
	kafkaLog := log.Named(LogComponentKafka)
	kafkaLog.Info("Created Kafka writer",
		zap.String(LogFieldEvent, LogEventKafkaWriterCreated),
		zap.String(LogFieldTopic, topic),
		zap.Int("batch_size", w.BatchSize),
		zap.Duration("batch_timeout", w.BatchTimeout),
	)
	return w
}

// NewKafkaConsumerReader creates a Kafka consumer for the given topic and group
// with engine-derived buffer limits.
func NewKafkaConsumerReader(cfg *Config, brokers []string, topic, groupID string, sessionTimeout, heartbeatInterval time.Duration, log *zap.Logger) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           brokers,
		Topic:             topic,
		GroupID:           groupID,
		MinBytes:          cfg.GlobalCapacity.KafkaReaderMinBytes,
		MaxBytes:          cfg.GlobalCapacity.KafkaReaderMaxBytes,
		MaxWait:           time.Duration(cfg.GlobalCapacity.KafkaReaderMaxWaitMs) * time.Millisecond,
		SessionTimeout:    sessionTimeout,
		HeartbeatInterval: heartbeatInterval,
	})
	kafkaLog := log.Named(LogComponentKafka)
	kafkaLog.Info("Created Kafka consumer reader",
		zap.String(LogFieldTopic, topic),
		zap.String(LogFieldGroup, groupID),
		zap.Duration("session_timeout", sessionTimeout),
		zap.Duration("heartbeat_interval", heartbeatInterval),
	)
	return r
}
