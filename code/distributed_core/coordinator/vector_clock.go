package coordinator

import pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"

// Vector clock utility functions: compare, merge, helpers.

// Compare returns:
//
//	-1 if a < b (a causally before b: all entries <= and at least one <)
//	 0 if a == b
//	 1 if a > b (a causally after b)
//	 2 if concurrent (neither dominates)
func CompareVC(a, b *pb.VectorClock) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil && b != nil {
		// treat nil as empty
		a = &pb.VectorClock{}
	}
	if b == nil && a != nil {
		b = &pb.VectorClock{}
	}
	ma := make(map[string]int64)
	mb := make(map[string]int64)
	for _, e := range a.Entries {
		ma[e.Node] = e.Counter
	}
	for _, e := range b.Entries {
		mb[e.Node] = e.Counter
	}
	aLess := false
	aGreater := false
	for k, va := range ma {
		vb := mb[k]
		if va < vb {
			aLess = true
		} else if va > vb {
			aGreater = true
		}
	}
	for k, vb := range mb {
		if _, ok := ma[k]; !ok {
			// ma missing key => treat as zero
			if vb > 0 {
				aLess = true
			}
		}
	}
	switch {
	case aLess && !aGreater:
		return -1
	case aGreater && !aLess:
		return 1
	case !aLess && !aGreater:
		return 0
	default:
		return 2 // concurrent
	}
}

// MergeVC returns merged vector clock (element-wise max)
func MergeVC(clocks ...*pb.VectorClock) *pb.VectorClock {
	if len(clocks) == 0 {
		return &pb.VectorClock{}
	}
	m := map[string]int64{}
	for _, c := range clocks {
		if c == nil {
			continue
		}
		for _, e := range c.Entries {
			if cur, ok := m[e.Node]; !ok || e.Counter > cur {
				m[e.Node] = e.Counter
			}
		}
	}
	out := &pb.VectorClock{}
	for n, v := range m {
		out.Entries = append(out.Entries, &pb.VectorClockEntry{Node: n, Counter: v})
	}
	return out
}

// MergeVersionedValues: drop versions that are dominated by others; keep concurrent ones.
// naive O(n^2) approach (fine for small lists).
func MergeVersionedValues(vals []*pb.VersionedValue) []*pb.VersionedValue {
	out := make([]*pb.VersionedValue, 0, len(vals))
	for i, vi := range vals {
		dominated := false
		for j, vj := range vals {
			if i == j {
				continue
			}
			cmp := CompareVC(vi.Clock, vj.Clock)
			// if vi is dominated by vj (vi < vj)
			if cmp == -1 {
				dominated = true
				break
			}
		}
		if !dominated {
			out = append(out, vi)
		}
	}
	// dedupe by value bytes if identical (best-effort)
	seen := map[string]bool{}
	final := []*pb.VersionedValue{}
	for _, v := range out {
		key := string(v.Value)
		if !seen[key] {
			seen[key] = true
			final = append(final, v)
		}
	}
	return final
}
