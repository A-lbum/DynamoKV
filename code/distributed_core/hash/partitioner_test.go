package hash

import (
	"fmt"
	"testing"
)

func TestBasicPartitioner(t *testing.T) {
	p := NewPartitioner(50)

	p.AddNode("node1")
	p.AddNode("node2")
	p.AddNode("node3")

	replicas := p.GetReplicas([]byte("hello"), 3)

	fmt.Println("Replicas:", replicas)

	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}
}
