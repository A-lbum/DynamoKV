package node

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

// helper: start a test server on a random port
func startTestServer(t *testing.T) (addr string, client pb.DynamoRPCClient, cleanup func()) {
	t.Helper()

	// use :0 to let OS auto-assign a free port
	addr = "127.0.0.1:0"
	storage := NewMemoryStorage()

	grpcServer, lis := startServerOnRandomPort(t, addr, storage)

	// dial client
	cc, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("grpc dial failed: %v", err)
	}
	client = pb.NewDynamoRPCClient(cc)

	cleanup = func() {
		grpcServer.Stop()
		cc.Close()
	}

	return lis.Addr().String(), client, cleanup
}

// actually start server with :0 dynamic port
func startServerOnRandomPort(t *testing.T, addr string, storage StorageAdapter) (*grpc.Server, net.Listener) {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen error: %v", err)
	}
	grpcServer := grpc.NewServer()
	srv := NewNodeServer(storage)
	pb.RegisterDynamoRPCServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	return grpcServer, lis
}

// ------------------------------------------------------------
// Test 1: basic InternalPut + InternalGet
// ------------------------------------------------------------

func TestInternalPutGetBasic(t *testing.T) {
	_, client, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Prepare write
	putReq := &pb.InternalPutRequest{
		Key: []byte("a"),
		Data: &pb.VersionedValue{
			Value:     []byte("1"),
			Timestamp: time.Now().UnixNano(),
		},
		IsHint:       false,
		OriginalNode: "",
	}

	// Put
	putResp, err := client.InternalPut(ctx, putReq)
	if err != nil {
		t.Fatalf("InternalPut RPC error: %v", err)
	}
	if !putResp.Ok {
		t.Fatalf("InternalPut returned Ok=false")
	}

	// Get
	getResp, err := client.InternalGet(ctx, &pb.InternalGetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("InternalGet RPC error: %v", err)
	}
	if len(getResp.Versions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(getResp.Versions))
	}
	if string(getResp.Versions[0].Value) != "1" {
		t.Fatalf("value mismatch, expected '1', got %q", string(getResp.Versions[0].Value))
	}
}

// ------------------------------------------------------------
// Test 2: multiple versions (simulate version branching)
// ------------------------------------------------------------

func TestInternalPutMultipleVersions(t *testing.T) {
	_, client, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Write version 1
	_, _ = client.InternalPut(ctx, &pb.InternalPutRequest{
		Key: []byte("k"),
		Data: &pb.VersionedValue{
			Value:     []byte("v1"),
			Timestamp: time.Now().UnixNano(),
		},
		IsHint: false,
	})

	// Write version 2 (another branch)
	_, _ = client.InternalPut(ctx, &pb.InternalPutRequest{
		Key: []byte("k"),
		Data: &pb.VersionedValue{
			Value:     []byte("v2"),
			Timestamp: time.Now().UnixNano(),
		},
		IsHint: false,
	})

	// Now GET: expect 2 versions
	getResp, err := client.InternalGet(ctx, &pb.InternalGetRequest{Key: []byte("k")})
	if err != nil {
		t.Fatalf("InternalGet error: %v", err)
	}
	if len(getResp.Versions) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(getResp.Versions))
	}
}

// ------------------------------------------------------------
// Test 3: hinted-handoff flag accepted
// ------------------------------------------------------------

func TestInternalPutHintedHandoff(t *testing.T) {
	_, client, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.InternalPut(ctx, &pb.InternalPutRequest{
		Key: []byte("x"),
		Data: &pb.VersionedValue{
			Value:     []byte("hv"),
			Timestamp: time.Now().UnixNano(),
		},
		IsHint:       true,
		OriginalNode: "node-3",
	})
	if err != nil {
		t.Fatalf("InternalPut (hinted) failed: %v", err)
	}
}

// ------------------------------------------------------------
// Test 4: SendHints / PushGossip sanity test
// ------------------------------------------------------------

func TestSendHintsAndPushGossip(t *testing.T) {
	_, client, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// SendHints
	_, err := client.SendHints(ctx, &pb.HandoffBatch{
		Hints: []*pb.Hint{
			{
				Key: []byte("k"),
				Data: &pb.VersionedValue{
					Value: []byte("v"),
				},
				TargetNode: "node-x",
			},
		},
	})
	if err != nil {
		t.Fatalf("SendHints error: %v", err)
	}

	// PushGossip
	_, err = client.PushGossip(ctx, &pb.GossipState{
		Nodes: []*pb.NodeState{
			{Node: "n1", Alive: true, Heartbeat: 1},
		},
	})
	if err != nil {
		t.Fatalf("PushGossip error: %v", err)
	}
}
