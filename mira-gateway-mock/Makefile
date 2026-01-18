.PHONY: build run

# 构建
build:
	@echo "构建服务..."
	go build -o mira-gateway-mock main.go

# 运行
run:
	@echo "运行服务..."
	go run main.go

# 安装依赖
deps:
	@echo "安装依赖..."
	go mod download

# 生成 vendor 目录（将依赖保存到本地）
vendor:
	@echo "生成 vendor 目录..."
	go mod tidy
	go mod vendor
	@echo "依赖已保存到 vendor 目录"

# 构建（使用 vendor 依赖）
build: vendor
	@echo "构建服务..."
	go build -mod=vendor -o mira-gateway-mock main.go

# 构建（不使用 vendor，直接从网络下载）
build-without-vendor:
	@echo "构建服务（不使用 vendor）..."
	go build -o mira-gateway-mock main.go

