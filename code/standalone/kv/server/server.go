package server

import (
	"github.com/llllleeeewwwiis/standalone/kv/storage"
	"github.com/llllleeeewwwiis/standalone/proto/pkg/rawkv"
)

type Server struct {
	rawkv.UnimplementedRawKVServer
	storage storage.Storage
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
	}
}
