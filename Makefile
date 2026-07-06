# Protobuf code generation
#
# Requires: buf (https://buf.build) or protoc + protoc-gen-go + protoc-gen-go-grpc
#
#   buf generate
#
# Or manually:
#   protoc -I api/proto \
#     --go_out=. --go_opt=module=ztm \
#     --go-grpc_out=. --go-grpc_opt=module=ztm \
#     api/proto/ztm/v1/*.proto

.PHONY: proto build test

GOEXE := $(shell go env GOEXE)

proto:
	buf generate

build:
	go build -o bin/ztm-node$(GOEXE) ./cmd/ztm-node
	go build -o bin/ztm-client$(GOEXE) ./cmd/ztm-client

test:
	go test ./...
