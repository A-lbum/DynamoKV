package main

import (
	"flag"
	"log"
	"net"

	"github.com/llllleeeewwwiis/standalone/kv/config"
	"github.com/llllleeeewwwiis/standalone/kv/server"
	"github.com/llllleeeewwwiis/standalone/kv/storage/standalone_storage"
	"github.com/llllleeeewwwiis/standalone/proto/pkg/rawkv"

	"google.golang.org/grpc"
)

var (
	addr   = flag.String("addr", "127.0.0.1:20160", "listen address")
	dbPath = flag.String("path", "/tmp/standalone", "database path")
)

func main() {
	flag.Parse()

	conf := &config.Config{
		StoreAddr: *addr,
		DBPath:    *dbPath,
		LogLevel:  "info",
	}

	// 初始化存储
	store := standalone_storage.NewStandAloneStorage(conf)
	if err := store.Start(); err != nil {
		log.Fatal(err)
	}

	// 初始化 Server（只带 RawKV）
	srv := server.NewServer(store)

	// 启动 gRPC
	grpcServer := grpc.NewServer()
	rawkv.RegisterRawKVServer(grpcServer, srv)

	listener, err := net.Listen("tcp", conf.StoreAddr)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("RawKV server running at %s\n", conf.StoreAddr)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
