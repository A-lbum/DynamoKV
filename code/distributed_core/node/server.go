package node

import (
	"net"
	"os"

	"log"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
	"google.golang.org/grpc"
)

func Serve(addr string, storage StorageAdapter) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	srv := NewNodeServer(storage)
	pb.RegisterDynamoRPCServer(grpcServer, srv)
	log.Printf("DynamoRPC node listening on %s\n", addr)
	return grpcServer.Serve(lis)
}

// small helper to run in goroutine in tests or main
func ServeAsync(addr string, storage StorageAdapter) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	grpcServer := grpc.NewServer()
	srv := NewNodeServer(storage)
	pb.RegisterDynamoRPCServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	return grpcServer, nil
}

func ServeWithConfig() error {
	nodeID := os.Getenv("NODE_ID")
	bind := os.Getenv("BIND")
	if bind == "" {
		bind = ":8080"
	}

	storage := NewMemoryStorage()
	log.Printf("Node %s starting on %s\n", nodeID, bind)
	return Serve(bind, storage)
}
