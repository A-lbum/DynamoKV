package main

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/llllleeeewwwiis/distributed_core/coordinator"
	"github.com/llllleeeewwwiis/distributed_core/node"
)

func mustGetenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env %s", key)
	}
	return v
}

func main() {
	// ------------------------------------------------------------
	// 1. Read config from env (container-friendly)
	// ------------------------------------------------------------
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		h, err := os.Hostname()
		if err != nil {
			log.Fatalf("cannot get hostname: %v", err)
		}
		nodeID = h
	}
	bind := os.Getenv("BIND")
	if bind == "" {
		bind = ":8080"
	}

	seedEnv := os.Getenv("NODE_SEEDS")
	var seeds []string
	if seedEnv != "" {
		seeds = strings.Split(seedEnv, ",")
	}

	log.Printf("[BOOT] node=%s bind=%s seeds=%v\n", nodeID, bind, seeds)

	// ------------------------------------------------------------
	// 2. Init local storage
	// (现在是 MemoryStorage；报告中可说明可替换为 Badger)
	// ------------------------------------------------------------
	storage := node.NewMemoryStorage()

	// ------------------------------------------------------------
	// 3. Init embedded Coordinator (Dynamo-style)
	// ------------------------------------------------------------
	coord := coordinator.NewCoordinator(
		100, // virtual nodes
		3,   // N
		2,   // R
		2,   // W
		nodeID,
	)

	// ------------------------------------------------------------
	// 4. Bootstrap membership from seeds
	// ------------------------------------------------------------
	if len(seeds) > 0 {
		coord.InitFromSeeds(nodeID, seeds)
	}

	// ------------------------------------------------------------
	// 5. Start gRPC node server
	// ------------------------------------------------------------
	go func() {
		if err := node.Serve(bind, storage); err != nil {
			log.Fatalf("node server failed: %v", err)
		}
	}()

	log.Printf("[BOOT] Dynamo node %s is running\n", nodeID)

	// ------------------------------------------------------------
	// 6. Block forever (until container exits)
	// ------------------------------------------------------------
	for {
		time.Sleep(10 * time.Second)
	}
}
