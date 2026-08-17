package platform

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

var (
	entropyPool = sync.Pool{
		New: func() interface{} {
			return rand.New(rand.NewSource(time.Now().UnixNano()))
		},
	}
)

// generateULID securely and concurrently generates a new ULID
func generateULID() string {
	entropy := entropyPool.Get().(*rand.Rand)
	defer entropyPool.Put(entropy)
	return ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
}

// NewJobID generates a prefixed ULID for Jobs.
func NewJobID() string {
	return fmt.Sprintf("%s_%s", AggregateTypeJob, generateULID())
}

func IsValidJobID(jobID string) bool {
	parts := strings.Split(jobID, "_")
	if len(parts) != 2 {
		return false
	}
	if parts[0] != string(AggregateTypeJob) {
		return false
	}
	_, err := ulid.Parse(parts[1])
	return err == nil
}

// NewEventID generates a prefixed ULID for Events.
func NewEventID() string {
	return fmt.Sprintf("%s_%s", AggregateTypeEvent, generateULID())
}

// NewDeterministicTransferID derives a stable Transfer ID from the Job ID so
// that Kafka redeliveries of the same job resolve to the same transfer.
func NewDeterministicTransferID(jobID string) string {
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // OID namespace
	id := uuid.NewSHA1(namespace, []byte(jobID))
	return fmt.Sprintf("%s_%s", AggregateTypeTransfer, id.String())
}

// NewMerchantID generates a UUID v7 for Merchants.
func NewMerchantID() string {
	id, _ := uuid.NewV7()
	return fmt.Sprintf("%s_%s", AggregateTypeMerchant, id.String())
}

// NewDeterministicDeliveryID generates a UUIDv5 based on the Event ID and Merchant ID to ensure idempotency.
func NewDeterministicDeliveryID(eventID, merchantID string) string {
	// Use a fixed namespace for webhooks
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // OID namespace
	id := uuid.NewSHA1(namespace, []byte(eventID+merchantID))
	return fmt.Sprintf("%s_%s", AggregateTypeDelivery, id.String())
}

// Validate merchant id
func IsValidMerchantID(merchantID string) bool {
	parts := strings.Split(merchantID, "_")
	if len(parts) != 2 {
		return false
	}
	return parts[0] == string(AggregateTypeMerchant) && uuid.Validate(parts[1]) == nil
}

// NewWalletID generates a composite Wallet ID: {merchant_id_uuidv7}.{uuidv7}
func NewWalletID(merchantID string) string {
	id, _ := uuid.NewV7()
	return fmt.Sprintf("%s.%s", merchantID, id.String())
}

func IsValidWalletID(walletID string) (string, string, bool) {
	parts := strings.Split(walletID, ".")
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], IsValidMerchantID(parts[0]) && uuid.Validate(parts[1]) == nil
}
