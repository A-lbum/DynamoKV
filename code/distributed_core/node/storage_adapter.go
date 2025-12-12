package node

import (
	"sync"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"github.com/llllleeeewwwiis/distributed_core/vclock"
)

// StorageAdapter is a minimal interface the node server uses.
type StorageAdapter interface {
	Put(key []byte, vv *pb.VersionedValue) error
	Get(key []byte) ([]*pb.VersionedValue, error)
	StoreHint(target string, h *pb.Hint) error
	GetHints(target string) []*pb.Hint
	PopHints(target string) []*pb.Hint
}

// MemoryStorage simple map-based storage for testing.
type MemoryStorage struct {
	mu    sync.RWMutex
	m     map[string][]*pb.VersionedValue
	hints map[string][]*pb.Hint // keyed by original target node
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		m:     make(map[string][]*pb.VersionedValue),
		hints: make(map[string][]*pb.Hint),
	}
}

func (s *MemoryStorage) Put(key []byte, vv *pb.VersionedValue) error {
	k := string(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.m[k]
	all := make([]*pb.VersionedValue, 0, len(existing)+1)
	all = append(all, existing...)
	all = append(all, vv)

	merged := vclock.MergeVersionedValues(all)
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
	out := make([]*pb.VersionedValue, len(vs))
	copy(out, vs)
	return out, nil
}

// StoreHint stores a hint for target node (when this node acts as fallback).
func (s *MemoryStorage) StoreHint(target string, h *pb.Hint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hints[target] = append(s.hints[target], h)
	return nil
}

// GetHints returns stored hints for a target (for observation).
func (s *MemoryStorage) GetHints(target string) []*pb.Hint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := s.hints[target]
	if h == nil {
		return nil
	}
	dup := make([]*pb.Hint, len(h))
	copy(dup, h)
	return dup
}

// PopHints returns stored hints for target and clears them (atomic).
func (s *MemoryStorage) PopHints(target string) []*pb.Hint {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hints[target]
	if h == nil {
		return nil
	}
	dup := make([]*pb.Hint, len(h))
	copy(dup, h)
	// clear stored hints for that target
	delete(s.hints, target)
	return dup
}

// applyHintsToStorage is helper to apply HandoffBatch to main storage
func (s *MemoryStorage) applyHintsToStorage(batch *pb.HandoffBatch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, hint := range batch.Hints {
		k := string(hint.Key)
		existing := s.m[k]
		all := make([]*pb.VersionedValue, 0, len(existing)+1)
		all = append(all, existing...)
		all = append(all, hint.Data)
		s.m[k] = vclock.MergeVersionedValues(all)
	}
}
