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

func (s *NodeServer) InternalPut(ctx context.Context, req *pb.InternalPutRequest) (*pb.InternalPutResponse, error) {
	if req == nil || req.Data == nil {
		return &pb.InternalPutResponse{Ok: false}, nil
	}
	// if this is a hinted write, store as hint for original node
	if req.IsHint && req.OriginalNode != "" {
		// store hint keyed by original node id
		h := &pb.Hint{
			Key:        req.Key,
			Data:       req.Data,
			TargetNode: req.OriginalNode,
		}
		_ = s.storage.StoreHint(req.OriginalNode, h)
		return &pb.InternalPutResponse{Ok: true}, nil
	}

	// normal write
	if err := s.storage.Put(req.Key, req.Data); err != nil {
		return &pb.InternalPutResponse{Ok: false}, nil
	}
	return &pb.InternalPutResponse{Ok: true}, nil
}

func (s *NodeServer) InternalGet(ctx context.Context, req *pb.InternalGetRequest) (*pb.InternalGetResponse, error) {
	versions, err := s.storage.Get(req.Key)
	if err != nil {
		return &pb.InternalGetResponse{Versions: nil}, nil
	}
	return &pb.InternalGetResponse{Versions: versions}, nil
}

func (s *NodeServer) SendHints(ctx context.Context, batch *pb.HandoffBatch) (*pb.HandoffAck, error) {
	// When original node receives HandoffBatch, it should apply hints to its local storage
	// We will implement applyHintsToStorage on MemoryStorage.
	if ms, ok := s.storage.(*MemoryStorage); ok {
		ms.applyHintsToStorage(batch)
		return &pb.HandoffAck{Ok: true}, nil
	}
	// fallback: iterate and Put normally
	for _, h := range batch.Hints {
		_ = s.storage.Put(h.Key, h.Data)
	}
	return &pb.HandoffAck{Ok: true}, nil
}

func (s *NodeServer) PushGossip(ctx context.Context, st *pb.GossipState) (*pb.GossipResponse, error) {
	// basic placeholder: accept gossip and return OK (we may extend to update local state)
	return &pb.GossipResponse{Ok: true}, nil
}
