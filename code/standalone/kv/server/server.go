package server

import (
	"github.com/llllleeeewwwiis/standalone/kv/storage"
)

type Server struct {
	storage storage.Storage
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
	}
}
