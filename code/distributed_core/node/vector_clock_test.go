package node

import (
	"testing"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
)

func vc(entries map[string]int64) *pb.VectorClock {
	out := &pb.VectorClock{}
	for n, c := range entries {
		out.Entries = append(out.Entries, &pb.VectorClockEntry{Node: n, Counter: c})
	}
	return out
}

func TestMergeVersionedValues_DominatedAndConcurrent(t *testing.T) {
	st := NewMemoryStorage()

	// put v1: A:1
	v1 := &pb.VersionedValue{Value: []byte("v1"), Clock: vc(map[string]int64{"A": 1})}
	if err := st.Put([]byte("k"), v1); err != nil {
		t.Fatalf("put v1 failed: %v", err)
	}
	got, _ := st.Get([]byte("k"))
	if len(got) != 1 {
		t.Fatalf("expect 1 version after first put, got %d", len(got))
	}

	// put v2 dominated by v1? (A:0,B:2) is concurrent with v1 => both should remain
	v2 := &pb.VersionedValue{Value: []byte("v2"), Clock: vc(map[string]int64{"B": 2})}
	if err := st.Put([]byte("k"), v2); err != nil {
		t.Fatalf("put v2 failed: %v", err)
	}
	got, _ = st.Get([]byte("k"))
	if len(got) != 2 {
		t.Fatalf("expect 2 versions after concurrent put, got %d", len(got))
	}

	// put v3 that dominates v1 and v2: A:5,B:5 -> should remove previous and keep single v3
	v3 := &pb.VersionedValue{Value: []byte("v3"), Clock: vc(map[string]int64{"A": 5, "B": 5})}
	if err := st.Put([]byte("k"), v3); err != nil {
		t.Fatalf("put v3 failed: %v", err)
	}
	got, _ = st.Get([]byte("k"))
	if len(got) != 1 {
		t.Fatalf("expect 1 version after dominated put, got %d", len(got))
	}
	if string(got[0].Value) != "v3" {
		t.Fatalf("expected v3 present, got %s", string(got[0].Value))
	}
}
