.PHONY: proto build test

# 生成proto代码
proto:
	@echo "生成proto代码..."
	@mkdir -p generated/mirapb
	@mkdir -p generated/datasource
	protoc --go_out=generated --go_opt=paths=source_relative \
	       --go-grpc_out=generated --go-grpc_opt=paths=source_relative \
	       -Iproto proto/mira_ida_access_service.proto proto/common.proto
	protoc --go_out=generated --go_opt=paths=source_relative \
	       --go-grpc_out=generated --go-grpc_opt=paths=source_relative \
	       -Iproto proto/data_source.proto
	@echo "Proto代码生成完成"

# 安装依赖
deps:
	@echo "安装依赖..."
	go mod download
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 生成 vendor 目录（将依赖保存到本地）
vendor:
	@echo "生成 vendor 目录..."
	go mod tidy
	go mod vendor
	@echo "依赖已保存到 vendor 目录"

# 构建（使用 vendor 依赖）
build: vendor
	@echo "构建项目..."
	go build -mod=vendor -o data-integrate-test main.go

# 构建（不使用 vendor，直接从网络下载）
build-without-vendor:
	@echo "构建项目（不使用 vendor）..."
	go build -o data-integrate-test main.go

# 运行测试
test:
	@echo "运行客户端连接测试..."
	go run cmd/test_clients/main.go

