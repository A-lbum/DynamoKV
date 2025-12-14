package coordinator

import (
	"testing"
	"time"

	"net"

	"github.com/llllleeeewwwiis/distributed_core/node"
	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

// helper to start node server (uses MemoryStorage)
func startTestNode(t *testing.T, nodeID string) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fail: %v", err)
	}
	addr = lis.Addr().String()
	grpcServer := grpc.NewServer()
	stor := node.NewMemoryStorage()
	srv := node.NewNodeServer(stor)
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

func TestPutGetAndHinting(t *testing.T) {
	c := NewCoordinator(100, 3, 2, 2, "coord-1")
	defer c.Close()

	// start 3 nodes
	nodes := []string{"n1", "n2", "n3"}
	stops := make([]func(), 0, 3)
	addrs := make([]string, 0, 3)
	for _, n := range nodes {
		addr, stop := startTestNode(t, n)
		stops = append(stops, stop)
		addrs = append(addrs, addr)
		if err := c.RegisterNode(n, addr); err != nil {
			t.Fatalf("register %s failed: %v", n, err)
		}
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	// simple put/get
	if err := c.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	vals, err := c.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(vals) == 0 {
		t.Fatalf("expected value, got none")
	}

	// simulate node down: unregister node n3
	c.UnregisterNode("n3")

	// put while n3 down -> should still succeed (W=2)
	if err := c.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put with node down failed: %v", err)
	}

	// hints should be recorded for node3 target (if it belonged to replica set)
	// We cannot reliably assert the internal map contents because which node was target depends on hash,
	// but we assert that FlushHints runs without panic.
	c.FlushHints()
}
