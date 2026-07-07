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
	protoc -I api/proto \
	  --go_out=api/proto --go_opt=paths=source_relative \
	  --go-grpc_out=api/proto --go-grpc_opt=paths=source_relative \
	  api/proto/ztm/v1/*.proto

build:
	go build -o bin/ztm-node$(GOEXE) ./cmd/ztm-node
	go build -o bin/ztm-client$(GOEXE) ./cmd/ztm-client

test:
	go test ./...
