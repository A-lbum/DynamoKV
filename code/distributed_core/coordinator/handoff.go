package coordinator

import (
	"sync"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
)

// HintBuffer stores hints per target node and provides a flush function.
type HintBuffer struct {
	mu    sync.Mutex
	hints map[string][]*pb.Hint
}

func NewHintBuffer() *HintBuffer {
	return &HintBuffer{
		hints: make(map[string][]*pb.Hint),
	}
}

// StoreHint appends a hint for target.
func (hb *HintBuffer) StoreHint(target string, h *pb.Hint) {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	hb.hints[target] = append(hb.hints[target], h)
}

// Flush attempts to deliver hints for each target using deliver(target, batch) callback.
// deliver returns nil on success and non-nil on failure; on success hints for target are removed.
func (hb *HintBuffer) Flush(deliver func(target string, batch *pb.HandoffBatch) error) {
	hb.mu.Lock()
	targets := make([]string, 0, len(hb.hints))
	for t := range hb.hints {
		targets = append(targets, t)
	}
	hb.mu.Unlock()

	for _, t := range targets {
		hb.mu.Lock()
		hlist := hb.hints[t]
		hb.mu.Unlock()
		if len(hlist) == 0 {
			continue
		}
		batch := &pb.HandoffBatch{Hints: hlist}
		// try deliver
		if err := deliver(t, batch); err != nil {
			// keep hints
			continue
		}
		// on success, remove hints
		hb.mu.Lock()
		delete(hb.hints, t)
		hb.mu.Unlock()
	}
}

// GetHintsForTest helper (not used in production) to inspect hints stored.
func (hb *HintBuffer) GetHintsForTest(target string) []*pb.Hint {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	dup := make([]*pb.Hint, len(hb.hints[target]))
	copy(dup, hb.hints[target])
	return dup
}
