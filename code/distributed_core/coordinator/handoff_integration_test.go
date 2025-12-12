package coordinator

import (
	"net"
	"testing"
	"time"

	"github.com/llllleeeewwwiis/distributed_core/node"
	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

// helper: start node server and return address, stop func, and storage pointer
func startNodeWithStorage(t *testing.T, nodeID string) (addr string, stop func(), storage *node.MemoryStorage) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen err: %v", err)
	}
	addr = lis.Addr().String()
	grpcServer := grpc.NewServer()
	st := node.NewMemoryStorage()
	srv := node.NewNodeServer(st)
	pb.RegisterDynamoRPCServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	stop = func() {
		grpcServer.Stop()
		lis.Close()
	}
	return addr, stop, st
}

func TestHintedHandoffAndRecovery(t *testing.T) {
	c := NewCoordinator(100, 3, 2, 2, "coord-1")
	defer c.Close()

	// start nodes n1,n2,n3
	addr1, stop1, st1 := startNodeWithStorage(t, "n1")
	addr2, stop2, st2 := startNodeWithStorage(t, "n2")
	addr3, stop3, _ := startNodeWithStorage(t, "n3")

	if err := c.RegisterNode("n1", addr1); err != nil {
		t.Fatalf("register n1 failed: %v", err)
	}
	if err := c.RegisterNode("n2", addr2); err != nil {
		t.Fatalf("register n2 failed: %v", err)
	}
	if err := c.RegisterNode("n3", addr3); err != nil {
		t.Fatalf("register n3 failed: %v", err)
	}

	// stop n3 to simulate crash
	stop3()

	// Give healthChecker time to detect (it runs every ~2s). Alternatively, force Unregister:
	c.UnregisterNode("n3")

	// Put key; if n3 was in preference list, coordinator should write hint to fallback node
	key := []byte("hkey")
	val := []byte("hval")

	if err := c.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// wait a bit for fallback writes
	time.Sleep(200 * time.Millisecond)

	// check fallback nodes' stored hints: either st1 or st2 should have hint for original 'n3'
	hints1 := st1.GetHints("n3")
	hints2 := st2.GetHints("n3")
	if len(hints1) == 0 && len(hints2) == 0 {
		t.Fatalf("expected some fallback node to have stored hints for n3, but none found")
	}

	// now restart n3
	addr3b, stop3b, _ := startNodeWithStorage(t, "n3")
	defer stop3b()
	// register again
	if err := c.RegisterNode("n3", addr3b); err != nil {
		t.Fatalf("re-register n3 failed: %v", err)
	}

	// Flush hints (coordinator will try to deliver to n3)
	c.FlushHints()
	time.Sleep(200 * time.Millisecond)

	// verify that n3 now has the hinted key present
	vers, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get after recovery failed: %v", err)
	}
	found := false
	for _, v := range vers {
		if string(v.Value) == string(val) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("hinted value not found on recovery")
	}

	// cleanup
	stop1()
	stop2()
}
