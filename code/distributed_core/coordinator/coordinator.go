package coordinator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/llllleeeewwwiis/distributed_core/hash"
	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

// Coordinator orchestrates replication, quorum, hints, and basic VC handling.
type Coordinator struct {
	partitioner *hash.Partitioner

	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.DynamoRPCClient

	// replication/quorum config
	N int // replication factor
	R int // read quorum
	W int // write quorum

	// identity + local logical counter
	LocalNodeID string
	clockMu     sync.Mutex
	localClock  map[string]int64 // node->counter; coordinator keeps its own counter entry

	// hint buffer
	handoff *HintBuffer

	// RPC timeouts
	rpcTimeout time.Duration
	dialOpts   []grpc.DialOption

	closed int32
}

// NewCoordinator constructs a Coordinator. vnodes for partitioner (virtual nodes count).
func NewCoordinator(vnodes int, N, R, W int, localNodeID string) *Coordinator {
	p := hash.NewPartitioner(vnodes)
	c := &Coordinator{
		partitioner: p,
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.DynamoRPCClient),
		N:           N,
		R:           R,
		W:           W,
		LocalNodeID: localNodeID,
		localClock:  map[string]int64{localNodeID: 0},
		handoff:     NewHintBuffer(),
		rpcTimeout:  1 * time.Second,
		dialOpts:    []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()},
	}
	// start hint flusher background goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if atomic.LoadInt32(&c.closed) == 1 {
				return
			}
			c.FlushHints()
		}
	}()
	return c
}

// RegisterNode registers a node id and gRPC address, adds node to partitioner and dials.
func (c *Coordinator) RegisterNode(nodeID string, addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// add to partitioner
	c.partitioner.AddNode(hash.NodeID(nodeID))

	// dial if no existing connection
	if _, ok := c.clients[nodeID]; ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, c.dialOpts...)
	if err != nil {
		// leave ring membership; coordinator will attempt later registrations
		return fmt.Errorf("dial %s failed: %w", addr, err)
	}

	c.conns[nodeID] = conn
	c.clients[nodeID] = pb.NewDynamoRPCClient(conn)
	return nil
}

// UnregisterNode removes node and closes connection.
func (c *Coordinator) UnregisterNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitioner.RemoveNode(hash.NodeID(nodeID))
	if conn, ok := c.conns[nodeID]; ok {
		_ = conn.Close()
		delete(c.conns, nodeID)
	}
	delete(c.clients, nodeID)
}

// Close shuts down connections and background tasks.
func (c *Coordinator) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	c.mu.Lock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.mu.Unlock()
}

// getClient returns client for node
func (c *Coordinator) getClient(node string) pb.DynamoRPCClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clients[node]
}

// incrLocalClock increments and returns the new local counter
func (c *Coordinator) incrLocalClock() int64 {
	c.clockMu.Lock()
	defer c.clockMu.Unlock()
	c.localClock[c.LocalNodeID]++
	return c.localClock[c.LocalNodeID]
}

// getReplicasForKey returns up to N replica nodeIDs (strings).
func (c *Coordinator) getReplicasForKey(key []byte) []string {
	nodeIDs := c.partitioner.GetReplicas(key, c.N)
	out := make([]string, 0, len(nodeIDs))
	for _, n := range nodeIDs {
		out = append(out, string(n))
	}
	return out
}

// Put implements write path: create VersionedValue with VC, send to N replicas concurrently,
// require at least W successes. Failed targets receive hinted handoff stored locally.
func (c *Coordinator) Put(key []byte, value []byte) error {
	replicas := c.getReplicasForKey(key)
	if len(replicas) == 0 {
		return fmt.Errorf("no replicas available")
	}

	// create versioned value with simple VC (coordinator increments its counter)
	counter := c.incrLocalClock()
	vv := &pb.VersionedValue{
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Clock: &pb.VectorClock{
			Entries: []*pb.VectorClockEntry{
				{Node: c.LocalNodeID, Counter: counter},
			},
		},
	}

	req := &pb.InternalPutRequest{
		Key:          key,
		Data:         vv,
		IsHint:       false,
		OriginalNode: "",
	}

	var wg sync.WaitGroup
	var success int32
	errCh := make(chan error, len(replicas))

	for _, node := range replicas {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			if err := c.callInternalPut(node, req); err != nil {
				// store hint
				c.handoff.StoreHint(node, &pb.Hint{
					Key:        key,
					Data:       vv,
					TargetNode: node,
				})
				errCh <- fmt.Errorf("node %s put failed: %w", node, err)
				return
			}
			atomic.AddInt32(&success, 1)
		}(node)
	}
	wg.Wait()
	close(errCh)

	if int(success) >= c.W {
		return nil
	}
	// build combined error
	var last error
	for e := range errCh {
		last = e
	}
	return fmt.Errorf("write quorum not reached: %d/%d, lastErr=%v", success, c.W, last)
}

// Get implements read path: query up to N replicas, wait for R successes, and merge versions.
// Merge policy: drop dominated versions, keep concurrent versions (return list).
func (c *Coordinator) Get(key []byte) ([]*pb.VersionedValue, error) {
	replicas := c.getReplicasForKey(key)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no replicas available")
	}

	type res struct {
		vers []*pb.VersionedValue
		err  error
	}
	resCh := make(chan res, len(replicas))
	var wg sync.WaitGroup

	for _, node := range replicas {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			vers, err := c.callInternalGet(node, &pb.InternalGetRequest{Key: key})
			resCh <- res{vers: vers, err: err}
		}(node)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	var successes int
	collected := make([]*pb.VersionedValue, 0)
	for r := range resCh {
		if r.err == nil {
			successes++
			collected = append(collected, r.vers...)
		}
	}

	if successes < c.R {
		return collected, fmt.Errorf("read quorum not reached: %d/%d", successes, c.R)
	}

	// merge collected versions using vector clock rules
	merged := MergeVersionedValues(collected)
	return merged, nil
}

// callInternalPut helper
func (c *Coordinator) callInternalPut(node string, req *pb.InternalPutRequest) error {
	client := c.getClient(node)
	if client == nil {
		return fmt.Errorf("no client for node %s", node)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
	defer cancel()
	_, err := client.InternalPut(ctx, req)
	return err
}

// callInternalGet helper
func (c *Coordinator) callInternalGet(node string, req *pb.InternalGetRequest) ([]*pb.VersionedValue, error) {
	client := c.getClient(node)
	if client == nil {
		return nil, fmt.Errorf("no client for node %s", node)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
	defer cancel()
	resp, err := client.InternalGet(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Versions, nil
}

// FlushHints triggers handoff buffer flush attempt.
func (c *Coordinator) FlushHints() {
	// delegate to handoff buffer which will attempt delivery to known clients
	c.handoff.Flush(func(target string, batch *pb.HandoffBatch) error {
		client := c.getClient(target)
		if client == nil {
			return fmt.Errorf("no client for target %s", target)
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
		defer cancel()
		_, err := client.SendHints(ctx, batch)
		return err
	})
}
