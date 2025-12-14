package coordinator

import (
	"net"
	"testing"
	"time"

	"github.com/llllleeeewwwiis/distributed_core/node"
	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

// startNode 启动一个 NodeServer，返回真实监听地址、stop 函数、以及 storage 对象
func startNode(t *testing.T, nodeID string) (addr string, stop func(), storage *node.MemoryStorage) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed for %s: %v", nodeID, err)
	}
	addr = lis.Addr().String()

	grpcServer := grpc.NewServer()
	storage = node.NewMemoryStorage()
	srv := node.NewNodeServer(storage)
	pb.RegisterDynamoRPCServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	stop = func() {
		grpcServer.Stop()
		lis.Close()
	}
	return addr, stop, storage
}

func TestDynamicJoinAndLeave(t *testing.T) {
	// Coordinator config: N=2, R=1, W=1 for simplicity
	coord := NewCoordinator(100, 2, 1, 1, "coord-1")
	defer coord.Close()

	// Start two nodes n1, n2
	addr1, stop1, st1 := startNode(t, "n1")
	addr2, stop2, _ := startNode(t, "n2")

	// Register both nodes with their real addresses
	if err := coord.RegisterNode("n1", addr1); err != nil {
		t.Fatalf("RegisterNode n1 failed: %v", err)
	}
	if err := coord.RegisterNode("n2", addr2); err != nil {
		t.Fatalf("RegisterNode n2 failed: %v", err)
	}

	key := []byte("dyn-key")
	val := []byte("before-join")

	// Initial write
	if err := coord.Put(key, val); err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	// --- STEP 1: Dynamic JOIN (add new node n3) ---
	addr3, stop3, st3 := startNode(t, "n3")
	// register n3
	if err := coord.RegisterNode("n3", addr3); err != nil {
		t.Fatalf("RegisterNode n3 failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// write after join
	val2 := []byte("after-join")
	if err := coord.Put(key, val2); err != nil {
		t.Fatalf("Put after join failed: %v", err)
	}

	// At least one version should appear in n3
	time.Sleep(200 * time.Millisecond)
	vers3, err := st3.Get(key)
	if err != nil {
		t.Fatalf("st3.Get error: %v", err)
	}
	if len(vers3) == 0 {
		t.Fatalf("n3 did NOT receive replicated data after dynamic join")
	}

	// --- STEP 2: Dynamic LEAVE (remove n2) ---
	// stop n2 process and unregister from coordinator
	stop2()
	coord.UnregisterNode("n2")

	time.Sleep(200 * time.Millisecond)

	val3 := []byte("after-leave")
	if err := coord.Put(key, val3); err != nil {
		t.Fatalf("Put after leave failed: %v", err)
	}

	// Check the remaining nodes (n1 or n3) received data
	time.Sleep(200 * time.Millisecond)
	vers1, err := st1.Get(key)
	if err != nil {
		t.Fatalf("st1.Get error: %v", err)
	}
	vers3, err = st3.Get(key)
	if err != nil {
		t.Fatalf("st3.Get error: %v", err)
	}
	if len(vers1) == 0 && len(vers3) == 0 {
		t.Fatalf("after n2 leaves, remaining replicas did NOT receive data")
	}

	// --- STEP 3: Recovery JOIN (restart n2) ---
	addr2b, stop2b, st2b := startNode(t, "n2")
	// ensure cleanup
	defer stop2b()
	if err := coord.RegisterNode("n2", addr2b); err != nil {
		t.Fatalf("re-register n2 failed: %v", err)
	}

	// coordinator should attempt to flush hints
	coord.FlushHints()
	time.Sleep(400 * time.Millisecond)

	vers2b, err := st2b.Get(key)
	if err != nil {
		t.Fatalf("st2b.Get error: %v", err)
	}
	if len(vers2b) == 0 {
		t.Fatalf("after recovery, n2 did NOT receive hinted handoff data")
	}

	// cleanup
	stop1()
	stop3()
}
