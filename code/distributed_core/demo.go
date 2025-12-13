package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/llllleeeewwwiis/distributed_core/coordinator"
	"github.com/llllleeeewwwiis/distributed_core/node"
	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

// ---------------- Helper ----------------
func mustDial(addr string) pb.DynamoRPCClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(fmt.Sprintf("dial %s failed: %v", addr, err))
	}
	return pb.NewDynamoRPCClient(conn)
}

func startNode(nodeID string) (addr string, stop func(), storage *node.MemoryStorage) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr = lis.Addr().String()
	storage = node.NewMemoryStorage()
	srv := node.NewNodeServer(storage)
	grpcServer := grpc.NewServer()
	pb.RegisterDynamoRPCServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	stop = func() {
		grpcServer.Stop()
		lis.Close()
	}
	fmt.Printf("Node %s started at %s\n", nodeID, addr)
	return
}

// ---------------- Main ----------------
func main() {
	fmt.Println("=== DEMO START ===")

	// 1. 启动 coordinator
	c := coordinator.NewCoordinator(100, 3, 2, 2, "coord-demo")
	defer c.Close()
	fmt.Println("Coordinator started")

	// 2. 启动初始节点 n1,n2,n3
	addr1, stop1, _ := startNode("n1")
	addr2, stop2, _ := startNode("n2")
	addr3, stop3, _ := startNode("n3")
	defer stop1()
	defer stop2()
	defer stop3()

	// 注册节点
	c.RegisterNode("n1", addr1)
	c.RegisterNode("n2", addr2)
	c.RegisterNode("n3", addr3)

	// 3. Put/Get 基础演示
	key := []byte("demo-key")
	val := []byte("v1")
	fmt.Printf("\nPUT %s -> %s\n", key, val)
	if err := c.Put(key, val); err != nil {
		panic(err)
	}
	time.Sleep(200 * time.Millisecond)

	client2 := mustDial(addr2)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	resp, _ := client2.InternalGet(ctx, &pb.InternalGetRequest{Key: key})
	for _, v := range resp.Versions {
		fmt.Printf("Node-2 value: %s\n", string(v.Value))
	}

	// 4. 动态加入节点 n4
	addr4, stop4, st4 := startNode("n4")
	defer stop4()
	c.RegisterNode("n4", addr4)
	fmt.Println("\nNode n4 joined")
	time.Sleep(200 * time.Millisecond)

	val2 := []byte("v2-after-join")
	fmt.Printf("PUT %s -> %s\n", key, val2)
	c.Put(key, val2)
	time.Sleep(200 * time.Millisecond)
	vers4, _ := st4.Get(key)
	for _, v := range vers4 {
		fmt.Printf("Node-4 value: %s\n", string(v.Value))
	}

	// 5. 模拟节点下线 n2
	stop2()
	c.UnregisterNode("n2")
	fmt.Println("\nNode n2 left")

	val3 := []byte("v3-after-leave")
	fmt.Printf("PUT %s -> %s\n", key, val3)
	c.Put(key, val3)
	time.Sleep(200 * time.Millisecond)

	// 查看其他节点接收情况
	client1 := mustDial(addr1)
	resp1, _ := client1.InternalGet(ctx, &pb.InternalGetRequest{Key: key})
	for _, v := range resp1.Versions {
		fmt.Printf("Node-1 value: %s\n", string(v.Value))
	}
	vers4, _ = st4.Get(key)
	for _, v := range vers4 {
		fmt.Printf("Node-4 value: %s\n", string(v.Value))
	}

	// 6. 节点恢复 n2
	addr2b, stop2b, st2b := startNode("n2")
	defer stop2b()
	c.RegisterNode("n2", addr2b)
	fmt.Println("\nNode n2 recovered")

	// Flush hints
	c.FlushHints()
	time.Sleep(400 * time.Millisecond)
	vers2b, _ := st2b.Get(key)
	for _, v := range vers2b {
		fmt.Printf("Node-2 (recovered) value: %s\n", string(v.Value))
	}

	fmt.Println("\n=== DEMO END ===")
}
