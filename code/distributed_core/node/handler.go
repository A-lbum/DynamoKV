package node

import (
	"context"

	pb "github.com/llllleeeewwwiis/distributed_core/proto/pkg/dynamo"
)

// NodeServer previously defined elsewhere; ensure it embeds UnimplementedDynamoRPCServer.

func (s *NodeServer) InternalPut(ctx context.Context, req *pb.InternalPutRequest) (*pb.InternalPutResponse, error) {
	if req == nil || req.Data == nil {
		return &pb.InternalPutResponse{Ok: false}, nil
	}

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

	if err := s.storage.Put(req.Key, req.Data); err != nil {
		return &pb.InternalPutResponse{Ok: false}, nil
	}
	return &pb.InternalPutResponse{Ok: true}, nil
}

func (s *NodeServer) InternalGet(ctx context.Context, req *pb.InternalGetRequest) (*pb.InternalGetResponse, error) {
	vers, _ := s.storage.Get(req.Key)
	return &pb.InternalGetResponse{Versions: vers}, nil
}

func (s *NodeServer) SendHints(ctx context.Context, batch *pb.HandoffBatch) (*pb.HandoffAck, error) {
	// apply hints into local storage
	if ms, ok := s.storage.(*MemoryStorage); ok {
		ms.applyHintsToStorage(batch)
		return &pb.HandoffAck{Ok: true}, nil
	}
	// fallback: write hints normally
	for _, h := range batch.Hints {
		_ = s.storage.Put(h.Key, h.Data)
	}
	return &pb.HandoffAck{Ok: true}, nil
}

// FetchHints: return and clear hints that this node stored for target originalNode
func (s *NodeServer) FetchHints(ctx context.Context, req *pb.FetchHintsRequest) (*pb.HandoffBatch, error) {
	target := req.TargetNode
	if target == "" {
		return &pb.HandoffBatch{Hints: []*pb.Hint{}}, nil
	}
	if ms, ok := s.storage.(*MemoryStorage); ok {
		hints := ms.PopHints(target)
		if hints == nil {
			return &pb.HandoffBatch{Hints: []*pb.Hint{}}, nil
		}
		return &pb.HandoffBatch{Hints: hints}, nil
	}
	// if storage not memory, no hints
	return &pb.HandoffBatch{Hints: []*pb.Hint{}}, nil
}
