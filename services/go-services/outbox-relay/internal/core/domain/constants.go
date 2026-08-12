package domain

const (
	// Error / DLQ Reasons
	ReasonCorruptedPayload = "Corrupted JSON payload"
	ReasonMessageTooLarge  = "Message exceeds size limit"
	ReasonInvalidSchema    = "Invalid EventEnvelope schema"
	ReasonPanic            = "Panic recovered during processing"
	ReasonRetryExhausted   = "retry_exhausted"
)
