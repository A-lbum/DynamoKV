# ------------------------------
# Stage 1: Build
# ------------------------------
FROM golang:1.24.2 AS builder
WORKDIR /app

COPY . .

RUN go mod download

RUN go build -o /dynamo-node ./cmd

# ------------------------------
# Stage 2: Runtime
# ------------------------------
FROM debian:stable-slim
WORKDIR /app

COPY --from=builder /dynamo-node .

EXPOSE 8080

ENTRYPOINT ["./dynamo-node"]