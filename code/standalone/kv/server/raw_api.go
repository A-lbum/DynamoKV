package server

import (
	"context"

	"github.com/llllleeeewwwiis/standalone/kv/storage"
	"github.com/llllleeeewwwiis/standalone/proto/pkg/rawkv"
)

// ------------------ RawGet ------------------
func (server *Server) RawGet(ctx context.Context, req *rawkv.RawGetRequest) (*rawkv.RawGetResponse, error) {
	reader, err := server.storage.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil || val == nil {
		return &rawkv.RawGetResponse{NotFound: true}, nil
	}
	return &rawkv.RawGetResponse{Value: val}, nil
}

// ------------------ RawPut ------------------
func (server *Server) RawPut(ctx context.Context, req *rawkv.RawPutRequest) (*rawkv.RawPutResponse, error) {
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	if err := server.storage.Write(batch); err != nil {
		return nil, err
	}
	return &rawkv.RawPutResponse{}, nil
}

// ------------------ RawDelete ------------------
func (server *Server) RawDelete(ctx context.Context, req *rawkv.RawDeleteRequest) (*rawkv.RawDeleteResponse, error) {
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}
	if err := server.storage.Write(batch); err != nil {
		return nil, err
	}
	return &rawkv.RawDeleteResponse{}, nil
}

// ------------------ RawScan ------------------
func (server *Server) RawScan(ctx context.Context, req *rawkv.RawScanRequest) (*rawkv.RawScanResponse, error) {
	reader, err := server.storage.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	it := reader.IterCF(req.Cf)
	defer it.Close()

	var kvs []*rawkv.KvPair
	for it.Seek(req.StartKey); it.Valid() && len(kvs) < int(req.Limit); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		value, _ := item.ValueCopy(nil)
		kvs = append(kvs, &rawkv.KvPair{
			Key:   key,
			Value: value,
		})
	}

	return &rawkv.RawScanResponse{Kvs: kvs}, nil
}
