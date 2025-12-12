package node

import (
	"sync"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"github.com/llllleeeewwwiis/distributed_core/vclock"
)

// StorageAdapter is a minimal interface the node server uses.
// Start with an in-memory map implementation; later implement adapter to standalone storage.
type StorageAdapter interface {
	Put(key []byte, vv *pb.VersionedValue) error
	Get(key []byte) ([]*pb.VersionedValue, error)
}

// MemoryStorage simple map-based storage for testing.
type MemoryStorage struct {
	mu sync.RWMutex
	m  map[string][]*pb.VersionedValue
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		m: make(map[string][]*pb.VersionedValue),
	}
}

// Put now merges incoming version with existing versions using vector-clock rules.
func (s *MemoryStorage) Put(key []byte, vv *pb.VersionedValue) error {
	k := string(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	// get existing
	existing := s.m[k]
	// combine
	all := make([]*pb.VersionedValue, 0, len(existing)+1)
	all = append(all, existing...)
	all = append(all, vv)

	// merge using vector clock rules
	merged := vclock.MergeVersionedValues(all)
	// store merged result
	s.m[k] = merged
	return nil
}

func (s *MemoryStorage) Get(key []byte) ([]*pb.VersionedValue, error) {
	k := string(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	vs := s.m[k]
	if vs == nil {
		return []*pb.VersionedValue{}, nil
	}
	// return copy to avoid caller mutating internal slice
	out := make([]*pb.VersionedValue, len(vs))
	copy(out, vs)
	return out, nil
}
