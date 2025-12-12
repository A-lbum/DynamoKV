# ------------------------------
# Stage 1: Build
# ------------------------------
FROM golang:1.24.2 AS builder
WORKDIR /app

# 复制整个 distributed_core 目录
COPY ./code/distributed_core ./

# 下载依赖
RUN go mod download

# 构建分布式节点可执行文件
RUN go build -o /dynamo-node ./cmd/distributed_main.go

# ------------------------------
# Stage 2: Final image
# ------------------------------
FROM debian:stable-slim
WORKDIR /app

# 复制可执行文件
COPY --from=builder /dynamo-node .

# 暴露端口
EXPOSE 8080

# 启动程序
ENTRYPOINT ["./dynamo-node"]