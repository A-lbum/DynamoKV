package node

import (
	"context"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
)

type NodeServer struct {
	pb.UnimplementedDynamoRPCServer
	storage StorageAdapter
}

func NewNodeServer(storage StorageAdapter) *NodeServer {
	return &NodeServer{storage: storage}
}

// InternalPut: coordinator -> replica
func (s *NodeServer) InternalPut(ctx context.Context, req *pb.InternalPutRequest) (*pb.InternalPutResponse, error) {
	// TODO: validate req.Data, handle is_hint etc.
	if err := s.storage.Put(req.Key, req.Data); err != nil {
		return &pb.InternalPutResponse{Ok: false}, nil
	}
	return &pb.InternalPutResponse{Ok: true}, nil
}

// InternalGet: coordinator -> replica
func (s *NodeServer) InternalGet(ctx context.Context, req *pb.InternalGetRequest) (*pb.InternalGetResponse, error) {
	versions, err := s.storage.Get(req.Key)
	if err != nil {
		return &pb.InternalGetResponse{Versions: nil}, nil
	}
	return &pb.InternalGetResponse{Versions: versions}, nil
}

func (s *NodeServer) SendHints(ctx context.Context, batch *pb.HandoffBatch) (*pb.HandoffAck, error) {
	// placeholder: store in local hinted store (not yet implemented)
	return &pb.HandoffAck{Ok: true}, nil
}

func (s *NodeServer) PushGossip(ctx context.Context, st *pb.GossipState) (*pb.GossipResponse, error) {
	// placeholder: integrate gossip state
	return &pb.GossipResponse{Ok: true}, nil
}
