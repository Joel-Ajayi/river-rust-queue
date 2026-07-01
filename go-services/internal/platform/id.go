package platform

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"
)

var counter uint64

func init() {
	var b [8]byte
	rand.Read(b[:])
	counter = binary.BigEndian.Uint64(b[:])
}

// NewID generates a time-sortable, collision-resistant ID with the given prefix.
func NewID(prefix string) string {
	ts := time.Now().UnixMicro()
	seq := atomic.AddUint64(&counter, 1)
	return fmt.Sprintf("%s_%d_%06x", prefix, ts, seq&0xFFFFFF)
}

func NewJobID() string   { return NewID(string(AggregateTypeJob)) }
func NewEventID() string { return NewID(string(AggregateTypeEvent)) }
