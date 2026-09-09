PROTOC := protoc

PROTO_DIR := proto
GEN_DIR := gen

PROTO_FILES := $(shell find $(PROTO_DIR) -type f -name '*.proto')

.PHONY: proto proto-check test build tidy clean-proto

# 检查 protobuf 编译工具是否存在
proto-check:
	@command -v $(PROTOC) >/dev/null 2>&1 || \
		(echo "错误：未找到 protoc"; exit 1)
	@command -v protoc-gen-go >/dev/null 2>&1 || \
		(echo "错误：未找到 protoc-gen-go"; exit 1)
	@command -v protoc-gen-go-grpc >/dev/null 2>&1 || \
		(echo "错误：未找到 protoc-gen-go-grpc"; exit 1)
	@echo "protobuf 工具检查通过"
	@$(PROTOC) --version
	@protoc-gen-go --version
	@protoc-gen-go-grpc --version

# 编译所有 protobuf 文件
proto: proto-check
	@mkdir -p $(GEN_DIR)
	@$(PROTOC) \
		-I $(PROTO_DIR) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)
	@echo "protobuf 代码生成完成"

# 整理 Go 依赖
tidy:
	go mod tidy

# 运行全部测试
test:
	go test ./...

# 编译服务端和客户端
build:
	go build ./...

# 删除 protobuf 生成文件
clean-proto:
	find $(GEN_DIR) -type f \( \
		-name '*.pb.go' -o \
		-name '*_grpc.pb.go' \
	\) -delete
	@echo "protobuf 生成文件已删除"