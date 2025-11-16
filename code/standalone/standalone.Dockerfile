FROM golang:1.22 AS builder
WORKDIR /app

# 拷贝 standalone 代码
COPY . .

RUN go mod download
RUN go build -o /standalone-server main.go

FROM debian:stable-slim
WORKDIR /app
COPY --from=builder /standalone-server .
ENTRYPOINT ["./standalone-server"]
