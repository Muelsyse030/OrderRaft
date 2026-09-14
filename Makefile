PROTOC := protoc

PROTO_DIR := proto
GEN_DIR := gen

PROTO_FILES := $(shell find $(PROTO_DIR) -type f -name '*.proto')

# 把 go install 安装的 protoc 插件目录加入 PATH,使 make proto 无需手动配置环境变量
export PATH := $(shell go env GOPATH)/bin:$(PATH)

.PHONY: proto proto-check test test-race vet fmt fmt-check build tidy ci clean-proto

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

# 运行全部测试并开启竞态检测
test-race:
	go test -race -count=1 ./...

# 静态检查
vet:
	go vet ./...

# 格式化代码
fmt:
	gofmt -w .

# 校验代码格式
fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "以下文件未通过 gofmt："; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	@echo "gofmt 检查通过"

# 编译服务端和客户端
build:
	go build ./...

# 与 CI 一致的本地检查
ci: fmt-check vet test-race build

# 删除 protobuf 生成文件
clean-proto:
	find $(GEN_DIR) -type f \( \
		-name '*.pb.go' -o \
		-name '*_grpc.pb.go' \
	\) -delete
	@echo "protobuf 生成文件已删除"
