package main

import (
	"log"
	"os"
	"strings"

	"github.com/llllleeeewwwiis/distributed_core/coordinator"
	"github.com/llllleeeewwwiis/distributed_core/node"
)

func main() {
	// 读取环境变量
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID not set")
	}
	bind := os.Getenv("BIND")
	if bind == "" {
		bind = ":8080"
	}
	seedsEnv := os.Getenv("SEEDS")
	seeds := []string{}
	if seedsEnv != "" {
		seeds = strings.Split(seedsEnv, ",")
	}

	// 初始化本地存储
	storage := node.NewMemoryStorage()

	// 启动 coordinator（默认 N=3,R=2,W=2，可以修改）
	coord := coordinator.NewCoordinator(10, 3, 2, 2, nodeID)

	// 注册自身到 coordinator
	for _, seed := range seeds {
		if seed == bind || seed == nodeID {
			continue
		}
		coord.RegisterNode(seed, seed)
	}

	// 启动 gRPC server
	log.Printf("Starting Dynamo node %s at %s with seeds: %v\n", nodeID, bind, seeds)
	if err := node.Serve(bind, storage); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
