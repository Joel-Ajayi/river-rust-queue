package kafka

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/app"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

type ConsumerManager struct {
	logger        *zap.Logger
	jobReader     *kafka.Reader
	xshardReaders []*kafka.Reader
	processor     *app.Processor
	xshardService *app.XShardService
}

func NewConsumerManager(
	logger *zap.Logger,
	jobReader *kafka.Reader,
	xshardReaders []*kafka.Reader,
	processor *app.Processor,
	xshardService *app.XShardService,
) *ConsumerManager {
	return &ConsumerManager{
		logger:        logger,
		jobReader:     jobReader,
		xshardReaders: xshardReaders,
		processor:     processor,
		xshardService: xshardService,
	}
}

func (m *ConsumerManager) Start(ctx context.Context) {
	go m.consumeJobs(ctx)
	for _, r := range m.xshardReaders {
		go m.consumeXShard(ctx, r)
	}
}

// poll for job requested events and process them
func (m *ConsumerManager) consumeJobs(ctx context.Context) {
	for {
		msg, err := m.jobReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Error("failed to fetch job message", zap.Error(err))
			continue
		}

		var envelope eventsv1.EventEnvelope
		if err := protojson.Unmarshal(msg.Value, &envelope); err != nil {
			m.logger.Error("failed to unmarshal job event", zap.Error(err))
			m.jobReader.CommitMessages(ctx, msg)
			continue
		}

		if payload := envelope.GetJobRequested(); payload != nil {
			if err := m.processor.ProcessJob(ctx, payload); err != nil {
				m.logger.Error("failed to process job", zap.Error(err))
			}
		}

		// ACK the message to prevent redelivery
		if err := m.jobReader.CommitMessages(ctx, msg); err != nil {
			m.logger.Error("failed to commit job message", zap.Error(err))
		}
	}
}

// poll for xshard sagas
func (m *ConsumerManager) consumeXShard(ctx context.Context, r *kafka.Reader) {
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Error("failed to fetch xshard message", zap.Error(err))
			continue
		}

		var envelope eventsv1.EventEnvelope
		if err := protojson.Unmarshal(msg.Value, &envelope); err != nil {
			m.logger.Error("failed to unmarshal xshard event", zap.Error(err))
			r.CommitMessages(ctx, msg)
			continue
		}

		var processErr error
		switch {
		case envelope.GetXshardTransferRequested() != nil:
			processErr = m.xshardService.HandleXShardRequested(ctx, envelope.GetXshardTransferRequested())
		case envelope.GetXshardTransferSettled() != nil:
			processErr = m.xshardService.HandleXShardSettled(ctx, envelope.GetXshardTransferSettled())
		case envelope.GetXshardTransferFailed() != nil:
			processErr = m.xshardService.HandleXShardFailed(ctx, envelope.GetXshardTransferFailed())
		}

		if processErr != nil {
			m.logger.Error("failed to process xshard event", zap.Error(processErr))
		}

		r.CommitMessages(ctx, msg)
	}
}
