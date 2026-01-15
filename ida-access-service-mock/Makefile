.PHONY: proto build run

# 生成proto代码
proto:
	@echo "生成proto代码..."
	@mkdir -p mirapb
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/*.proto

# 构建
build:
	@echo "构建服务..."
	go build -o ida-access-service-mock main.go

# 运行
run:
	@echo "运行服务..."
	go run main.go

# 安装依赖
deps:
	@echo "安装依赖..."
	go mod download
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

