package docker_test

import (
	"context"
	"testing"
	"time"

	"github.com/llllleeeewwwiis/distributed_core/coordinator"
	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

// ------------------------------------------------------------
// Helper: connect to a node via exposed host port
// ------------------------------------------------------------
func mustDial(t *testing.T, addr string) pb.DynamoRPCClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial %s failed: %v", addr, err)
	}
	return pb.NewDynamoRPCClient(conn)
}

// ------------------------------------------------------------
// Test: Coordinator Put → Node replication → Node Get
// ------------------------------------------------------------
func TestDockerCoordinatorPutGet(t *testing.T) {
	/*
	   ⚠️ 运行前假设：
	   docker compose up --scale node=3 已经启动
	   并且端口映射类似：
	     node-1 → localhost:65335
	     node-2 → localhost:65334
	     node-3 → localhost:65336
	*/

	nodes := map[string]string{
		"node-1": "localhost:65335",
		"node-2": "localhost:65334",
		"node-3": "localhost:65336",
	}

	// ------------------------------------------------------------
	// 1. Create coordinator (same params as your system)
	// ------------------------------------------------------------
	c := coordinator.NewCoordinator(
		100, // vnodes
		3,   // N
		2,   // R
		2,   // W
		"coord-test",
	)
	defer c.Close()

	// ------------------------------------------------------------
	// 2. Register nodes with coordinator
	// ------------------------------------------------------------
	for nid, addr := range nodes {
		if err := c.RegisterNode(nid, addr); err != nil {
			t.Fatalf("RegisterNode %s failed: %v", nid, err)
		}
	}

	// ------------------------------------------------------------
	// 3. Put via coordinator
	// ------------------------------------------------------------
	key := []byte("docker-key-1")
	value := []byte("docker-value-1")

	if err := c.Put(key, value); err != nil {
		t.Fatalf("Coordinator.Put failed: %v", err)
	}

	// allow async replication
	time.Sleep(200 * time.Millisecond)

	// ------------------------------------------------------------
	// 4. Get directly from a replica node
	// ------------------------------------------------------------
	client := mustDial(t, nodes["node-2"])

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resp, err := client.InternalGet(ctx, &pb.InternalGetRequest{Key: key})
	if err != nil {
		t.Fatalf("InternalGet failed: %v", err)
	}

	if len(resp.Versions) == 0 {
		t.Fatalf("expected value replicated to node-2, got 0 versions")
	}

	found := false
	for _, v := range resp.Versions {
		if string(v.Value) == string(value) {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("value not found on node-2 replicas: %+v", resp.Versions)
	}

	t.Logf("SUCCESS: Coordinator.Put replicated value to node-2")
}
