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

// Coordinator orchestrates replication, quorum, sloppy quorum fallback, hints, and health checking.
type Coordinator struct {
	partitioner *hash.Partitioner

	mu        sync.RWMutex
	conns     map[string]*grpc.ClientConn
	clients   map[string]pb.DynamoRPCClient
	nodeAddrs map[string]string // persist node addresses for reconnect

	dialOpts   []grpc.DialOption
	rpcTimeout time.Duration

	// replication quorum configuration
	N int
	R int
	W int

	// vector clock local counter
	LocalNodeID string
	clockMu     sync.Mutex
	localClock  map[string]int64

	// hinted handoff buffer
	handoff *HintBuffer

	// node up/down state (true if node is considered down/unreachable)
	nodeDown map[string]bool

	closed int32
}

// ----------------------------------------------------------------------
// Constructor
// ----------------------------------------------------------------------

func NewCoordinator(vnodes int, N, R, W int, localNodeID string) *Coordinator {
	p := hash.NewPartitioner(vnodes)
	c := &Coordinator{
		partitioner: p,
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.DynamoRPCClient),
		nodeAddrs:   make(map[string]string),
		dialOpts:    []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()},
		rpcTimeout:  1 * time.Second,
		N:           N,
		R:           R,
		W:           W,
		LocalNodeID: localNodeID,
		localClock:  map[string]int64{localNodeID: 0},
		handoff:     NewHintBuffer(),
		nodeDown:    make(map[string]bool),
	}
	go c.healthChecker()
	return c
}

// ----------------------------------------------------------------------
// Membership
// ----------------------------------------------------------------------

func (c *Coordinator) RegisterNode(nodeID string, addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// persist address and ensure node remains on ring
	c.nodeAddrs[nodeID] = addr
	c.partitioner.AddNode(hash.NodeID(nodeID))

	// if already have client, mark up and return
	if _, ok := c.clients[nodeID]; ok {
		c.nodeDown[nodeID] = false
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, c.dialOpts...)
	if err != nil {
		// do not remove node from ring on dial failure: keep node in ring but mark it down
		c.nodeDown[nodeID] = true
		return fmt.Errorf("dial failed: %w", err)
	}

	c.conns[nodeID] = conn
	c.clients[nodeID] = pb.NewDynamoRPCClient(conn)
	// mark node as up
	c.nodeDown[nodeID] = false
	return nil
}

func (c *Coordinator) UnregisterNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// DO NOT remove node from partitioner here — keep it in ring so preference lists still include it.
	// c.partitioner.RemoveNode(hash.NodeID(nodeID))

	// close active connection and remove client, mark node down
	if conn, ok := c.conns[nodeID]; ok {
		_ = conn.Close()
		delete(c.conns, nodeID)
	}
	delete(c.clients, nodeID)

	// mark down so callers/put logic will treat it as unreachable (and trigger hinted handoff)
	c.nodeDown[nodeID] = true
	// keep nodeAddrs so healthChecker can reconnect later
}

// ----------------------------------------------------------------------
// Health Checking + Reconnect (FIXED)
// ----------------------------------------------------------------------

func (c *Coordinator) healthChecker() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if atomic.LoadInt32(&c.closed) == 1 {
			return
		}

		// copy nodeAddrs
		c.mu.RLock()
		addrs := make(map[string]string, len(c.nodeAddrs))
		for nid, addr := range c.nodeAddrs {
			addrs[nid] = addr
		}
		c.mu.RUnlock()

		for nid, addr := range addrs {
			// check if connected
			c.mu.RLock()
			client := c.clients[nid]
			c.mu.RUnlock()

			//-------------------------------------------------------------
			// Case 1: Not connected → try reconnect
			//-------------------------------------------------------------
			if client == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				conn, err := grpc.DialContext(ctx, addr, c.dialOpts...)
				cancel()
				if err != nil {
					continue // still down
				}

				// *** 修改：成功重连后 mark UP ***
				c.mu.Lock()
				c.conns[nid] = conn
				c.clients[nid] = pb.NewDynamoRPCClient(conn)
				c.partitioner.AddNode(hash.NodeID(nid))
				c.mu.Unlock()

				continue
			}

			//-------------------------------------------------------------
			// Case 2: Connected → send gossip (act as health ping)
			//-------------------------------------------------------------
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			_, err := client.PushGossip(ctx, &pb.GossipState{
				Nodes: []*pb.NodeState{
					{Node: c.LocalNodeID, Alive: true, Heartbeat: time.Now().UnixNano()},
				},
			})
			cancel()

			if err != nil {
				//---------------------------------------------------------
				// *** 修改：探测失败 → mark DOWN ***
				//---------------------------------------------------------
				c.UnregisterNode(nid)
			}
		}
	}
}

// ----------------------------------------------------------------------
// Shutdown
// ----------------------------------------------------------------------

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

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

func (c *Coordinator) incrLocalClock() int64 {
	c.clockMu.Lock()
	defer c.clockMu.Unlock()
	c.localClock[c.LocalNodeID]++
	return c.localClock[c.LocalNodeID]
}

func (c *Coordinator) getClient(node string) pb.DynamoRPCClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clients[node]
}

func (c *Coordinator) callInternalPut(node string, req *pb.InternalPutRequest) error {
	c.mu.RLock()
	down := c.nodeDown[node]
	c.mu.RUnlock()
	if down {
		return fmt.Errorf("node %s is marked down", node)
	}

	client := c.getClient(node)
	if client == nil {
		return fmt.Errorf("no client for node %s", node)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
	defer cancel()
	_, err := client.InternalPut(ctx, req)
	// if rpc error, consider marking node down (best-effort)
	if err != nil {
		// mark node down so future attempts will fallback quickly
		c.mu.Lock()
		c.nodeDown[node] = true
		// close and remove conn/client if present
		if conn, ok := c.conns[node]; ok {
			_ = conn.Close()
			delete(c.conns, node)
		}
		delete(c.clients, node)
		c.mu.Unlock()
	}
	return err
}

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

// ----------------------------------------------------------------------
// Replica Selection
// ----------------------------------------------------------------------

func (c *Coordinator) getAllCandidates(key []byte) []string {
	total := c.partitioner.NodeCount()
	nodeIDs := c.partitioner.GetReplicas(key, total)

	out := make([]string, 0, len(nodeIDs))
	for _, n := range nodeIDs {
		out = append(out, string(n))
	}
	return out
}

// ----------------------------------------------------------------------
// PUT (Sloppy quorum + fallback + hints)
// ----------------------------------------------------------------------

func (c *Coordinator) Put(key []byte, value []byte) error {
	preference := c.getAllCandidates(key)
	if len(preference) == 0 {
		return fmt.Errorf("no nodes available")
	}

	intended := preference
	if len(intended) > c.N {
		intended = intended[:c.N]
	}

	// build versioned value
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

	var wg sync.WaitGroup
	var success int32
	errCh := make(chan error, len(intended))

	used := make(map[string]struct{})
	var usedMu sync.Mutex

	for i, node := range intended {
		wg.Add(1)
		go func(node string, idx int) {
			defer wg.Done()

			req := &pb.InternalPutRequest{
				Key:          key,
				Data:         vv,
				IsHint:       false,
				OriginalNode: "",
			}

			if err := c.callInternalPut(node, req); err != nil {
				// fallback sloppy quorum
				fallbackDone := false
				for _, cand := range preference {
					usedMu.Lock()
					_, wasUsed := used[cand]
					usedMu.Unlock()
					if wasUsed || cand == node {
						continue
					}

					hreq := &pb.InternalPutRequest{
						Key:          key,
						Data:         vv,
						IsHint:       true,
						OriginalNode: node,
					}
					if err2 := c.callInternalPut(cand, hreq); err2 == nil {
						usedMu.Lock()
						used[cand] = struct{}{}
						usedMu.Unlock()
						atomic.AddInt32(&success, 1)
						fallbackDone = true
						break
					}
				}

				if !fallbackDone {
					c.handoff.StoreHint(node, &pb.Hint{
						Key:        key,
						Data:       vv,
						TargetNode: node,
					})
					errCh <- fmt.Errorf("node %s + fallback failed", node)
				}
				return
			}

			usedMu.Lock()
			used[node] = struct{}{}
			usedMu.Unlock()
			atomic.AddInt32(&success, 1)
		}(node, i)
	}

	wg.Wait()
	close(errCh)

	if int(success) >= c.W {
		return nil
	}

	var last error
	for e := range errCh {
		last = e
	}
	return fmt.Errorf("write quorum failed: %d/%d last=%v", success, c.W, last)
}

// ----------------------------------------------------------------------
// GET (quorum + merge)
// ----------------------------------------------------------------------

func (c *Coordinator) Get(key []byte) ([]*pb.VersionedValue, error) {
	preference := c.getAllCandidates(key)
	if len(preference) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	type res struct {
		vers []*pb.VersionedValue
		err  error
	}

	resCh := make(chan res, len(preference))
	var wg sync.WaitGroup

	// read up to N replicas
	limit := c.N
	if len(preference) < c.N {
		limit = len(preference)
	}

	for _, node := range preference[:limit] {
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
	collected := []*pb.VersionedValue{}

	for r := range resCh {
		if r.err == nil {
			successes++
			collected = append(collected, r.vers...)
		}
	}

	if successes < c.R {
		return collected, fmt.Errorf("read quorum not reached: %d/%d", successes, c.R)
	}

	merged := MergeVersionedValues(collected)
	return merged, nil
}

// ----------------------------------------------------------------------
// Hinted Handoff Flush
// ----------------------------------------------------------------------

func (c *Coordinator) FlushHints() {
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
