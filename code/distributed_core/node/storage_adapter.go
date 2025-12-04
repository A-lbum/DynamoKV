package node

import (
	"sync"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
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

func (s *MemoryStorage) Put(key []byte, vv *pb.VersionedValue) error {
	k := string(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	// naive append; in real impl you would merge vector clocks
	s.m[k] = append(s.m[k], vv)
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
	return vs, nil
}
