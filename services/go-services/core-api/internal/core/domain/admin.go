package domain

// DLQReplayResult represents the domain outcome of a DLQ batch replay operation.
type DLQReplayResult struct {
	ReplayedCount int
	ShardID       string
}
