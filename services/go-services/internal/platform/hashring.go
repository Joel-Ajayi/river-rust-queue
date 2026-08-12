package platform

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

const (
	// hashRingVNodeSeparator separates shard IDs from virtual-node indices on the ring.
	hashRingVNodeSeparator = "#"
)

// HashRing implements consistent hashing (clockwise walk) for shard routing.
type HashRing struct {
	mu       sync.RWMutex
	ring     map[uint32]string
	sorted   []uint32
	shardIDs []string
	vNodes   int
}

// NewHashRing creates a consistent hash ring from the given shard IDs
// with vNodes virtual nodes per shard (engine-derived KETAMA_VNODES; default 160).
func NewHashRing(shardIDs []string, vNodes int) *HashRing {
	if vNodes <= 0 {
		vNodes = 160
	}

	r := &HashRing{
		ring:     make(map[uint32]string, len(shardIDs)*vNodes),
		shardIDs: shardIDs,
		vNodes:   vNodes,
	}

	for _, sid := range shardIDs {
		r.addNodeLocked(sid)
	}

	return r
}

func (r *HashRing) addNodeLocked(shardID string) {
	for i := 0; i < r.vNodes; i++ {
		key := shardID + hashRingVNodeSeparator + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(key))
		r.ring[hash] = shardID
		r.sorted = append(r.sorted, hash)
	}
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

// AddNode dynamically adds a shard node to the ring.
func (r *HashRing) AddNode(shardID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, sid := range r.shardIDs {
		if sid == shardID {
			return
		}
	}
	r.shardIDs = append(r.shardIDs, shardID)
	r.addNodeLocked(shardID)
}

// RemoveNode dynamically removes a shard node from the ring.
func (r *HashRing) RemoveNode(shardID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	newShards := make([]string, 0, len(r.shardIDs))
	for _, sid := range r.shardIDs {
		if sid != shardID {
			newShards = append(newShards, sid)
		}
	}
	r.shardIDs = newShards

	newRing := make(map[uint32]string, len(r.ring))
	newSorted := make([]uint32, 0, len(r.sorted))
	for hash, sid := range r.ring {
		if sid != shardID {
			newRing[hash] = sid
			newSorted = append(newSorted, hash)
		}
	}
	r.ring = newRing
	r.sorted = newSorted
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

// ShardFor computes CRC32 of key and walks clockwise to the nearest shard.
func (r *HashRing) ShardFor(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sorted) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(r.sorted), func(i int) bool {
		return r.sorted[i] >= hash
	})

	if idx == len(r.sorted) {
		idx = 0
	}

	return r.ring[r.sorted[idx]]
}

// Shards returns all shard IDs in the ring.
func (r *HashRing) Shards() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	shardsCopy := make([]string, len(r.shardIDs))
	copy(shardsCopy, r.shardIDs)
	return shardsCopy
}
