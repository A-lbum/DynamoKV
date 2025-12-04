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

// Coordinator is the routing/coordination layer.
// It uses a Partitioner to map keys -> replica node IDs and talks to nodes via DynamoRPC.
type Coordinator struct {
	partitioner *hash.Partitioner

	// gRPC clients / connections keyed by nodeID (nodeID == NodeID used in partitioner)
	mu       sync.RWMutex
	conns    map[string]*grpc.ClientConn
	clients  map[string]pb.DynamoRPCClient
	dialOpts []grpc.DialOption

	// replication/quorum config
	N int // replication factor
	R int // read quorum
	W int // write quorum

	// node identity (this coordinator's node id, used in vector clock entries)
	LocalNodeID string

	// hint buffer (simple in-memory for now): map[targetNode] -> []Hint
	hintMu sync.Mutex
	hints  map[string][]*pb.Hint

	// RPC timeouts
	rpcTimeout time.Duration

	// connection dial timeout
	dialTimeout time.Duration

	closed int32
}

// NewCoordinator constructs a Coordinator.
// nodes: initial list of nodeIDs (their addresses must be provided with RegisterNodeAddress).
// vnodes: number of virtual nodes for the partitioner.
func NewCoordinator(vnodes int, N, R, W int, localNodeID string) *Coordinator {
	// p := hash.NewPartitioner(vnodes)
	return &Coordinator{
		partitioner: hash.NewPartitioner(vnodes),
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.DynamoRPCClient),
		dialOpts:    []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}, // for local testing
		N:           N,
		R:           R,
		W:           W,
		LocalNodeID: localNodeID,
		hints:       make(map[string][]*pb.Hint),
		rpcTimeout:  1 * time.Second,
		dialTimeout: 500 * time.Millisecond,
	}
}

// RegisterNode adds a node to the ring and dials it.
// nodeID must be the same string used in partitioner ring; addr is host:port reachable by gRPC.
func (c *Coordinator) RegisterNode(nodeID string, addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// add to partitioner
	c.partitioner.AddNode(hash.NodeID(nodeID))

	// dial only if not already
	if _, ok := c.clients[nodeID]; ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, c.dialOpts...)
	if err != nil {
		// don't remove node from partitioner; dialing may be later retried
		return fmt.Errorf("dial %s failed: %w", addr, err)
	}

	client := pb.NewDynamoRPCClient(conn)
	c.conns[nodeID] = conn
	c.clients[nodeID] = client
	return nil
}

// UnregisterNode removes a node from the ring and closes connection.
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

// Close closes all conns.
func (c *Coordinator) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}

// Put implements the coordinator write path.
// - compute replicas using partitioner
// - build a VersionedValue and send InternalPut to each replica in parallel
// - wait for W successes, otherwise return error
// - if some replica fails, store hint for that target node
func (c *Coordinator) Put(key []byte, value []byte) error {
	replicas := c.getReplicasForKey(key)
	if len(replicas) == 0 {
		return fmt.Errorf("no replicas available")
	}

	// create simple versioned value:
	vv := &pb.VersionedValue{
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Clock: &pb.VectorClock{
			Entries: []*pb.VectorClockEntry{
				{Node: c.LocalNodeID, Counter: time.Now().UnixNano()},
			},
		},
	}

	req := &pb.InternalPutRequest{
		Key:          key,
		Data:         vv,
		IsHint:       false,
		OriginalNode: "", // coordinator is originator
	}

	var wg sync.WaitGroup
	var success int32
	errCh := make(chan error, len(replicas))

	for _, node := range replicas {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			if err := c.callInternalPut(node, req); err != nil {
				// store hint for this target node
				c.storeHint(node, &pb.Hint{
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

	// collect error messages for debugging
	var lastErr error
	for e := range errCh {
		lastErr = e
	}
	return fmt.Errorf("write quorum not reached: success=%d, want=%d, lastErr=%v", success, c.W, lastErr)
}

// Get implements the coordinator read path.
// - compute replicas
// - concurrently call InternalGet on replicas
// - wait for R successful responses (or until all return)
// - return merged version list (MVP: unique values)
func (c *Coordinator) Get(key []byte) ([]*pb.VersionedValue, error) {
	replicas := c.getReplicasForKey(key)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no replicas available")
	}

	type resp struct {
		vers []*pb.VersionedValue
		err  error
	}

	resCh := make(chan resp, len(replicas))
	var wg sync.WaitGroup

	for _, node := range replicas {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			vers, err := c.callInternalGet(node, &pb.InternalGetRequest{Key: key})
			resCh <- resp{vers: vers, err: err}
		}(node)
	}

	// wait for all goroutines in background
	go func() {
		wg.Wait()
		close(resCh)
	}()

	// collect up to R successful responses (but we will read all to merge)
	var successes int
	merged := make([]*pb.VersionedValue, 0)

	seen := make(map[string]struct{}) // simple dedupe by value bytes
	for r := range resCh {
		if r.err == nil {
			successes++
			for _, v := range r.vers {
				keyVal := string(v.Value)
				if _, ok := seen[keyVal]; !ok {
					seen[keyVal] = struct{}{}
					merged = append(merged, v)
				}
			}
		}
	}

	if successes < c.R {
		return merged, fmt.Errorf("read quorum not reached: successes=%d, want=%d", successes, c.R)
	}

	return merged, nil
}

// getReplicasForKey returns a slice of nodeIDs (strings) for the given key.
// If requested N exceeds available nodes, it returns all available unique nodes.
func (c *Coordinator) getReplicasForKey(key []byte) []string {
	// ask partitioner for N replicas
	nodeIDs := c.partitioner.GetReplicas(key, c.N)
	out := make([]string, 0, len(nodeIDs))
	for _, n := range nodeIDs {
		out = append(out, string(n))
	}
	return out
}

// callInternalPut sends InternalPut to specified node.
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

// callInternalGet sends InternalGet and returns versions or error
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

// getClient returns a DynamoRPCClient for node (thread-safe)
func (c *Coordinator) getClient(node string) pb.DynamoRPCClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clients[node]
}

// storeHint stores a hint to be delivered later.
func (c *Coordinator) storeHint(target string, h *pb.Hint) {
	c.hintMu.Lock()
	defer c.hintMu.Unlock()
	c.hints[target] = append(c.hints[target], h)
}

// FlushHints attempts to deliver stored hints to their original target nodes.
// It tries each target; on success removes those hints. Non-destructive on failures.
func (c *Coordinator) FlushHints() {
	c.hintMu.Lock()
	targets := make([]string, 0, len(c.hints))
	for t := range c.hints {
		targets = append(targets, t)
	}
	c.hintMu.Unlock()

	for _, target := range targets {
		c.hintMu.Lock()
		hlist := c.hints[target]
		c.hintMu.Unlock()
		if len(hlist) == 0 {
			continue
		}

		// try to send batch
		c.mu.RLock()
		client := c.clients[target]
		c.mu.RUnlock()
		if client == nil {
			// can't deliver yet
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
		_, err := client.SendHints(ctx, &pb.HandoffBatch{Hints: hlist})
		cancel()
		if err != nil {
			// keep hints for later
			continue
		}

		// success -> remove hints for target
		c.hintMu.Lock()
		delete(c.hints, target)
		c.hintMu.Unlock()
	}
}
