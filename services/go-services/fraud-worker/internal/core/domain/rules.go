package domain

type WalletID string

type VelocityRule struct {
	Name          string
	WindowSeconds int
	Threshold     int
	Reason        string
}

type Event struct {
	EventID   string
	WalletID  string
	Timestamp int64 // unix ms
}
