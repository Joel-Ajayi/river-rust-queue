package platform

import (
	"fmt"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// MarshalEnvelope serializes an EventEnvelope using the canonical JSON
// encoding (protojson with EmitUnpopulated) shared by every outbox producer.
func MarshalEnvelope(env *eventsv1.EventEnvelope) ([]byte, error) {
	return (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(env)
}

// UnmarshalEnvelope decodes an EventEnvelope from its canonical JSON encoding.
func UnmarshalEnvelope(payload []byte) (*eventsv1.EventEnvelope, error) {
	var env eventsv1.EventEnvelope
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("decode event envelope: %w", err)
	}
	return &env, nil
}
