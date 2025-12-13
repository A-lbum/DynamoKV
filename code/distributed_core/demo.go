package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/llllleeeewwwiis/distributed_core/coordinator"
	"github.com/llllleeeewwwiis/distributed_core/node"
	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

/*
========================================================
 Demo Node abstraction (in-process)
========================================================
*/

type demoNode struct {
	id      string
	addr    string
	lis     net.Listener
	grpcSrv *grpc.Server
	storage *node.MemoryStorage
}

func startNode(id string) *demoNode {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	st := node.NewMemoryStorage()
	srv := grpc.NewServer()
	pb.RegisterDynamoRPCServer(srv, node.NewNodeServer(st))

	n := &demoNode{
		id:      id,
		addr:    lis.Addr().String(),
		lis:     lis,
		grpcSrv: srv,
		storage: st,
	}

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("[NODE %s] stopped: %v", id, err)
		}
	}()

	log.Printf("[NODE %s] started at %s", id, n.addr)
	return n
}

func stopNode(n *demoNode) {
	if n == nil {
		return
	}
	log.Printf("[NODE %s] stopping", n.id)
	n.grpcSrv.Stop()
	_ = n.lis.Close()
}

/*
========================================================
 Helper: read value via RPC
========================================================
*/

func readFromNode(n *demoNode, key string) {
	conn, err := grpc.Dial(n.addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("[READ %s] dial failed: %v", n.id, err)
		return
	}
	defer conn.Close()

	cli := pb.NewDynamoRPCClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resp, err := cli.InternalGet(ctx, &pb.InternalGetRequest{Key: []byte(key)})
	if err != nil || len(resp.Versions) == 0 {
		log.Printf("[READ %s] key=%s -> <empty>", n.id, key)
		return
	}

	log.Printf("[READ %s] key=%s -> %s",
		n.id, key, string(resp.Versions[0].Value))
}

func readAllVersionsFromNode(n *demoNode, key string) {
	conn, err := grpc.Dial(n.addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("[READ %s] dial failed: %v", n.id, err)
		return
	}
	defer conn.Close()

	cli := pb.NewDynamoRPCClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resp, err := cli.InternalGet(ctx, &pb.InternalGetRequest{Key: []byte(key)})
	if err != nil || len(resp.Versions) == 0 {
		log.Printf("[READ %s] key=%s -> <empty>", n.id, key)
		return
	}

	log.Printf("[READ %s] key=%s versions=%d", n.id, key, len(resp.Versions))
	for i, v := range resp.Versions {
		log.Printf(
			"  version[%d]: value=%s clock=%v",
			i,
			string(v.Value),
			v.Clock,
		)
	}
}

/*
========================================================
 DEMO
========================================================
*/

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	fmt.Println("========== DYNAMO FULL DEMO START ==========")

	//--------------------------------------------------
	// 1. Coordinator
	//--------------------------------------------------
	coord := coordinator.NewCoordinator(
		64,
		3,
		2,
		2,
		"coord-demo",
	)
	defer coord.Close()

	log.Println("[MEMBERSHIP] coordinator started")

	//--------------------------------------------------
	// 2. Start initial nodes n1,n2,n3
	//--------------------------------------------------
	n1 := startNode("n1")
	n2 := startNode("n2")
	n3 := startNode("n3")

	coord.RegisterNode("n1", n1.addr)
	coord.RegisterNode("n2", n2.addr)
	coord.RegisterNode("n3", n3.addr)

	log.Println("[MEMBERSHIP] n1,n2,n3 registered")
	time.Sleep(200 * time.Millisecond)

	//--------------------------------------------------
	// 3. Normal PUT
	//--------------------------------------------------
	fmt.Println("\n=== PUT k1 = v1 (all nodes alive) ===")
	coord.Put([]byte("k1"), []byte("v1"))
	time.Sleep(200 * time.Millisecond)

	readFromNode(n1, "k1")
	readFromNode(n2, "k1")
	readFromNode(n3, "k1")

	//--------------------------------------------------
	// 4. Node join (n4)
	//--------------------------------------------------
	fmt.Println("\n=== NODE n4 JOIN ===")
	n4 := startNode("n4")
	coord.RegisterNode("n4", n4.addr)
	log.Println("[MEMBERSHIP] n4 registered")
	time.Sleep(200 * time.Millisecond)

	fmt.Println("\n=== PUT k1 = v2-after-join ===")
	coord.Put([]byte("k1"), []byte("v2-after-join"))
	time.Sleep(200 * time.Millisecond)

	readFromNode(n4, "k1")

	//--------------------------------------------------
	// 5. Node failure (n2 DOWN)
	//--------------------------------------------------
	fmt.Println("\n=== NODE n2 DOWN ===")
	stopNode(n2)
	coord.UnregisterNode("n2")
	log.Println("[MEMBERSHIP] n2 unregistered")
	time.Sleep(200 * time.Millisecond)

	//--------------------------------------------------
	// 6. PUT with hinted handoff
	//--------------------------------------------------
	fmt.Println("\n=== PUT k1 = v3-with-hint (n2 down) ===")
	coord.Put([]byte("k1"), []byte("v3-with-hint"))
	time.Sleep(200 * time.Millisecond)

	readFromNode(n1, "k1")
	readFromNode(n3, "k1")
	readFromNode(n4, "k1")

	//--------------------------------------------------
	// 7. Node recovery (n2)
	//--------------------------------------------------
	fmt.Println("\n=== NODE n2 RECOVER ===")
	n2 = startNode("n2")
	coord.RegisterNode("n2", n2.addr)
	log.Println("[MEMBERSHIP] n2 re-registered")

	time.Sleep(1 * time.Second)
	readFromNode(n2, "k1")

	//--------------------------------------------------
	// 8. Vector Clock CONCURRENT WRITE DEMO  ★ 新增
	//--------------------------------------------------
	fmt.Println("\n=== VECTOR CLOCK CONCURRENT WRITE DEMO ===")

	key := "k-conflict"

	go func() {
		coord.Put([]byte(key), []byte("v-from-n1"))
	}()

	go func() {
		coord.Put([]byte(key), []byte("v-from-n3"))
	}()

	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n--- READ AFTER CONCURRENT PUT (expect siblings) ---")
	readAllVersionsFromNode(n1, key)

	fmt.Println("\n--- RESOLVE CONFLICT (PUT merged value) ---")
	coord.Put([]byte(key), []byte("v-resolved"))
	time.Sleep(300 * time.Millisecond)

	fmt.Println("\n--- READ AFTER RESOLUTION (single version) ---")
	readAllVersionsFromNode(n2, key)
	readAllVersionsFromNode(n3, key)
	readAllVersionsFromNode(n4, key)

	fmt.Println("\n========== DYNAMO FULL DEMO END ==========")
}
