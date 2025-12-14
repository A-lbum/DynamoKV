package hash

import (
	"bytes"
	"testing"
)

//
// ------------------------------------------------------------
// Test 1: 基础功能测试（添加节点 + 获取副本）
// ------------------------------------------------------------
// 用途：验证基本 AddNode / GetReplicas 是否工作正常
//

func TestPartitionerBasic(t *testing.T) {
	p := NewPartitioner(50)

	p.AddNode("node1")
	p.AddNode("node2")
	p.AddNode("node3")

	replicas := p.GetReplicas([]byte("hello"), 3)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}
}

//
// ------------------------------------------------------------
// Test 2: 一致性测试（同一个 key 多次查询结果必须一致）
// ------------------------------------------------------------
// 用途：一致性哈希必须是 deterministic —— 同一个 key 重复查询必须给同样结果
//

func TestDeterminism(t *testing.T) {
	p := NewPartitioner(50)
	p.AddNode("A")
	p.AddNode("B")
	p.AddNode("C")

	key := []byte("user:12345")

	r1 := p.GetReplicas(key, 3)
	r2 := p.GetReplicas(key, 3)
	r3 := p.GetReplicas(key, 3)

	if !bytes.Equal([]byte(r1[0]), []byte(r2[0])) ||
		!bytes.Equal([]byte(r2[0]), []byte(r3[0])) {
		t.Fatalf("inconsistent replica mapping for same key: %v %v %v", r1, r2, r3)
	}
}

//
// ------------------------------------------------------------
// Test 3: 节点越多分布越均匀（随机 keys 的分布比例）
// ------------------------------------------------------------
// 用途：确保一致性哈希 + 虚拟节点带来较均匀的数据分布
//

func TestDistribution(t *testing.T) {
	p := NewPartitioner(200)
	nodes := []NodeID{"n1", "n2", "n3", "n4", "n5"}

	for _, n := range nodes {
		p.AddNode(n)
	}

	count := map[NodeID]int{}
	N := 50000

	for i := 0; i < N; i++ {
		key := []byte(string(rune(i)))
		r := p.GetReplicas(key, 1)
		count[r[0]]++
	}

	// 期望大致均匀：每个节点应该有 ~ 1/5 的 key
	avg := float64(N) / float64(len(nodes))

	for n, c := range count {
		if float64(c) < avg*0.75 || float64(c) > avg*1.25 {
			t.Fatalf("node %s distribution too skewed: %d (avg %.2f)", n, c, avg)
		}
	}
}

//
// ------------------------------------------------------------
// Test 4: 添加节点时的数据迁移比例应较小（符合一致性哈希特性）
// ------------------------------------------------------------
// 用途：一致性哈希的核心理念：添加一个节点时，只有 O(1/N) 的 key 会改变归属
//

func TestMinimalMovement(t *testing.T) {
	p := NewPartitioner(100)
	p.AddNode("node1")
	p.AddNode("node2")

	// 记录旧分布
	oldMap := make([]NodeID, 0, 10000)
	for i := 0; i < 10000; i++ {
		k := []byte(string(rune(i)))
		oldMap = append(oldMap, p.GetReplicas(k, 1)[0])
	}

	// 添加新节点
	p.AddNode("node3")

	moved := 0
	for i := 0; i < 10000; i++ {
		k := []byte(string(rune(i)))
		newNode := p.GetReplicas(k, 1)[0]
		if newNode != oldMap[i] {
			moved++
		}
	}

	ratio := float64(moved) / float64(10000)

	// 对于 N=2 → N=3，理想移动比例大约是 33%，我们设一个合理范围
	if ratio < 0.20 || ratio > 0.50 {
		t.Fatalf("unexpected key movement ratio %.2f", ratio)
	}
}

//
// ------------------------------------------------------------
// Test 5: 删除节点时，原本属于该节点的 key 必须重新映射到新节点
// ------------------------------------------------------------
// 用途：检测 RemoveNode 逻辑是否正常
//

func TestRemoveNode(t *testing.T) {
	p := NewPartitioner(100)
	p.AddNode("node1")
	p.AddNode("node2")
	p.AddNode("node3")

	key := []byte("test-key")

	oldReplicas := p.GetReplicas(key, 3)

	// 删除 primary node
	p.RemoveNode(oldReplicas[0])

	newReplicas := p.GetReplicas(key, 3)
	if newReplicas[0] == oldReplicas[0] {
		t.Fatalf("primary replica should change after node removal")
	}
}

//
// ------------------------------------------------------------
// Test 6: 副本列表唯一性（不会重复同一个物理节点）
// ------------------------------------------------------------
// 用途：换句话说：虚拟节点不能导致重复副本
//

func TestReplicaUniqueness(t *testing.T) {
	p := NewPartitioner(200)
	p.AddNode("A")
	p.AddNode("B")
	p.AddNode("C")

	replicas := p.GetReplicas([]byte("something"), 3)

	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}

	if hasDuplicate(replicas) {
		t.Fatalf("replicas contain duplicated nodes: %v", replicas)
	}
}

func hasDuplicate(xs []NodeID) bool {
	m := map[NodeID]struct{}{}
	for _, x := range xs {
		if _, ok := m[x]; ok {
			return true
		}
		m[x] = struct{}{}
	}
	return false
}

//
// ------------------------------------------------------------
// Test 7: 环排序正确
// ------------------------------------------------------------
// 用途：确保虚拟节点 hash 是严格递增的，保证二分查找和 wrap-around 正常
//

func TestRingOrdering(t *testing.T) {
	p := NewPartitioner(10)
	p.AddNode("A")
	p.AddNode("B")

	last := uint32(0)
	for i, v := range p.ring {
		if i > 0 && v.Hash < last {
			t.Fatalf("ring is not sorted: %v < %v", v.Hash, last)
		}
		last = v.Hash
	}
}

//
// ------------------------------------------------------------
// Test 8: 副本数超过节点数 → 只返回所有节点
// ------------------------------------------------------------
// 用途：保证系统不会 panic 并安全降级
//

func TestReplicaOverflow(t *testing.T) {
	p := NewPartitioner(50)
	p.AddNode("A")
	p.AddNode("B")

	reps := p.GetReplicas([]byte("xxx"), 5) // only 2 nodes exist

	if len(reps) != 2 {
		t.Fatalf("replicas overflow should cap to available nodes")
	}
}
