package vclock

import (
	"github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
)

// CompareVC 返回：-1 (a < b), 0 (a == b), 1 (a > b), 2 (concurrent)
func CompareVC(a, b *dynamo.VectorClock) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		a = &dynamo.VectorClock{}
	}
	if b == nil {
		b = &dynamo.VectorClock{}
	}
	ma := map[string]int64{}
	mb := map[string]int64{}
	for _, e := range a.Entries {
		ma[e.Node] = e.Counter
	}
	for _, e := range b.Entries {
		mb[e.Node] = e.Counter
	}
	aLess := false
	aGreater := false
	// check keys in a
	for k, va := range ma {
		vb := mb[k]
		if va < vb {
			aLess = true
		} else if va > vb {
			aGreater = true
		}
	}
	// check keys in b that not in a
	for k, vb := range mb {
		if _, ok := ma[k]; !ok {
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
		return 2
	}
}

// MergeVC 对多个 vector clocks 做 element-wise max，返回合并结果（entries 顺序未保证）。
func MergeVC(clocks ...*dynamo.VectorClock) *dynamo.VectorClock {
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
	out := &dynamo.VectorClock{}
	for n, v := range m {
		out.Entries = append(out.Entries, &dynamo.VectorClockEntry{Node: n, Counter: v})
	}
	return out
}

// MergeVersionedValues: 从传入的版本列表中剔除被其它版本“支配”的版本，保留并发版本。
// 规则：如果 v_i.clock < v_j.clock （被支配），则丢弃 v_i。
// O(n^2) 实现，适合小集合（Dynamo 的 typical case）。
func MergeVersionedValues(vals []*dynamo.VersionedValue) []*dynamo.VersionedValue {
	out := make([]*dynamo.VersionedValue, 0, len(vals))
	for i, vi := range vals {
		dominated := false
		for j, vj := range vals {
			if i == j {
				continue
			}
			cmp := CompareVC(vi.Clock, vj.Clock)
			if cmp == -1 { // vi < vj => vi 被支配
				dominated = true
				break
			}
		}
		if !dominated {
			out = append(out, vi)
		}
	}
	// 最后按 value bytes 做一次去重（若两个版本 value 完全相同则只保留一个）
	seen := map[string]bool{}
	final := make([]*dynamo.VersionedValue, 0, len(out))
	for _, v := range out {
		key := string(v.Value)
		if !seen[key] {
			seen[key] = true
			final = append(final, v)
		}
	}
	return final
}
