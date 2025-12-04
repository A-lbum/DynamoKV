package coordinator

import (
	"net"
	"testing"
	"time"

	"github.com/llllleeeewwwiis/distributed_core/hash"
	"github.com/llllleeeewwwiis/distributed_core/node"
	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

// helper: start a NodeServer on a dynamic port and return nodeID and address.
// nodeID must be provided by caller (e.g., "node-1").
func startNodeServer(t *testing.T, nodeID string) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	addr = lis.Addr().String()

	grpcServer := grpc.NewServer()
	storage := node.NewMemoryStorage()
	srv := node.NewNodeServer(storage)
	pb.RegisterDynamoRPCServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	stop = func() {
		grpcServer.Stop()
		lis.Close()
	}
	return addr, stop
}

func TestCoordinatorPutGetIntegration(t *testing.T) {
	// Create coordinator (vnode num 100, N=3, R=2, W=2)
	c := NewCoordinator(100, 3, 2, 2, "coord-1")

	// start 3 node servers
	nodes := []string{"node-1", "node-2", "node-3"}
	var stops []func()
	for _, nid := range nodes {
		addr, stop := startNodeServer(t, nid)
		stops = append(stops, stop)
		// register node with coordinator
		if err := c.RegisterNode(nid, addr); err != nil {
			t.Fatalf("RegisterNode %s failed: %v (addr=%s)", nid, err, addr)
		}
	}
	// cleanup
	defer func() {
		for _, stop := range stops {
			stop()
		}
		c.Close()
	}()

	// Put a key
	key := []byte("test-key")
	value := []byte("hello-1")
	if err := c.Put(key, value); err != nil {
		t.Fatalf("Coordinator.Put failed: %v", err)
	}

	// Give a small time for replication
	time.Sleep(50 * time.Millisecond)

	// Get via coordinator
	vers, err := c.Get(key)
	if err != nil {
		t.Fatalf("Coordinator.Get failed: %v", err)
	}
	if len(vers) == 0 {
		t.Fatalf("expected >=1 versions, got 0")
	}
	found := false
	for _, v := range vers {
		if string(v.Value) == string(value) {
			found = true
		}
	}
	if !found {
		t.Fatalf("value not found in returned versions: %v", vers)
	}

	// basic hint behavior test: unregister one node and ensure write still succeeds (sloppy quorums)
	c.UnregisterNode("node-3") // simulate node-3 down

	err = c.Put([]byte("k2"), []byte("v2"))
	if err != nil {
		// With N=3 and W=2 this should still succeed if at least 2 replicas reachable
		t.Fatalf("Put after node removal failed: %v", err)
	}

	// flush hints (no-op if target node unreachable)
	c.FlushHints()
}

func TestGetReplicaSelectionDeterminism(t *testing.T) {
	p := hash.NewPartitioner(100)
	p.AddNode("node-1")
	p.AddNode("node-2")
	p.AddNode("node-3")

	k := []byte("some-key")
	a := p.GetReplicas(k, 3)
	b := p.GetReplicas(k, 3)
	if len(a) != len(b) {
		t.Fatalf("replica lists length differ")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("replica selection not deterministic: %v vs %v", a, b)
		}
	}
}
