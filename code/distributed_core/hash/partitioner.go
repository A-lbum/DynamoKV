package hash

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type NodeID string

// Virtual node on the hash ring
type vnode struct {
	Hash uint32
	Node NodeID
}

type Partitioner struct {
	mu     sync.RWMutex
	ring   []vnode // sorted by Hash
	nodes  map[NodeID]struct{}
	vnodes int // number of virtual nodes per physical node
}

// NewPartitioner creates a consistent hash ring with vnodes per node.
func NewPartitioner(vnodes int) *Partitioner {
	return &Partitioner{
		ring:   make([]vnode, 0),
		nodes:  make(map[NodeID]struct{}),
		vnodes: vnodes,
	}
}

// hashKey returns a 32-bit hash of input using SHA-1.
func hashKey(key []byte) uint32 {
	h := sha1.Sum(key)
	return binary.BigEndian.Uint32(h[:4])
}

// AddNode inserts a node with multiple virtual nodes.
func (p *Partitioner) AddNode(id NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.nodes[id]; exists {
		return
	}
	p.nodes[id] = struct{}{}

	// Add virtual nodes
	for i := 0; i < p.vnodes; i++ {
		vkey := fmt.Sprintf("%s#%d", id, i)
		h := hashKey([]byte(vkey))
		p.ring = append(p.ring, vnode{Hash: h, Node: id})
	}

	sort.Slice(p.ring, func(i, j int) bool {
		return p.ring[i].Hash < p.ring[j].Hash
	})
}

// RemoveNode deletes a node and its virtual nodes.
func (p *Partitioner) RemoveNode(id NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.nodes[id]; !exists {
		return
	}
	delete(p.nodes, id)

	filtered := p.ring[:0]
	for _, v := range p.ring {
		if v.Node != id {
			filtered = append(filtered, v)
		}
	}
	p.ring = filtered
}

// GetReplicas returns n replica nodes for the given key.
func (p *Partitioner) GetReplicas(key []byte, n int) []NodeID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.ring) == 0 || n <= 0 {
		return nil
	}

	hash := hashKey(key)

	// binary search for the first vnode >= hash
	idx := sort.Search(len(p.ring), func(i int) bool {
		return p.ring[i].Hash >= hash
	})
	if idx == len(p.ring) {
		idx = 0 // wrap around
	}

	replicas := make([]NodeID, 0, n)
	used := make(map[NodeID]struct{})

	// walk forward to collect unique nodes
	for i := 0; i < len(p.ring) && len(replicas) < n; i++ {
		vnode := p.ring[(idx+i)%len(p.ring)]
		if _, exists := used[vnode.Node]; !exists {
			replicas = append(replicas, vnode.Node)
			used[vnode.Node] = struct{}{}
		}
	}

	return replicas
}

func (p *Partitioner) NodeCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.nodes)
}
