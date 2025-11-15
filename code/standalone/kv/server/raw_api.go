package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// ------------------ RawGet ------------------
func (server *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil || val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// ------------------ RawPut ------------------
func (server *Server) RawPut(ctx context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	if err := server.storage.Write(nil, batch); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// ------------------ RawDelete ------------------
func (server *Server) RawDelete(ctx context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}
	if err := server.storage.Write(nil, batch); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// ------------------ RawScan ------------------
func (server *Server) RawScan(ctx context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	it := reader.IterCF(req.Cf)
	defer it.Close()

	var kvs []*kvrpcpb.KvPair
	for it.Seek(req.StartKey); it.Valid() && len(kvs) < int(req.Limit); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		value, _ := item.ValueCopy(nil)
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
