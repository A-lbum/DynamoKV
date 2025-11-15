package config

import (
	"os"
)

type Config struct {
	// Basic server settings
	StoreAddr string // gRPC 服务监听地址
	LogLevel  string // 日志等级
	DBPath    string // BadgerDB 路径

	// Whether to use Raft. StandaloneKV always sets this to false.
	Raft bool
}

// Get log level from environment (compatible with original version)
func getLogLevel() (logLevel string) {
	logLevel = "info"
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		logLevel = l
	}
	return
}

func NewDefaultConfig() *Config {
	return &Config{
		StoreAddr: "127.0.0.1:20160",
		DBPath:    "/tmp/badger",
		LogLevel:  getLogLevel(),

		// StandaloneKV: disable Raft
		Raft: false,
	}
}

func NewTestConfig() *Config {
	return &Config{
		StoreAddr: "127.0.0.1:20160",
		DBPath:    "/tmp/badger",
		LogLevel:  getLogLevel(),

		// Tests also use standalone mode
		Raft: false,
	}
}
