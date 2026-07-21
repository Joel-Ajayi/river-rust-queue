package domain

import (
	"time"
)

type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"
	StatusDelivered DeliveryStatus = "delivered"
	StatusDLQ       DeliveryStatus = "dlq"
)

// Merchant contains the configuration required to deliver webhooks to a merchant.
type Merchant struct {
	ID            string
	WebhookURL    string
	WebhookSecret string
	Status        string
	ShardID       string
}

// WebhookDelivery represents an attempt to deliver an event to a merchant.
// It maps directly to the webhook_deliveries table in the shard.
type WebhookDelivery struct {
	ID            string
	MerchantID    string
	SourceEventID string
	URL           string
	Payload       []byte
	Signature     string
	AttemptCount  int
	LastAttemptAt *time.Time
	NextRetryAt   *time.Time
	LastError     *string
	LastStatus    *int
	Status        DeliveryStatus
	DeliveredAt   *time.Time
	CreatedAt     time.Time
}
