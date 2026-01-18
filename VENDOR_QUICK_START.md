# Vendor 快速使用指南

## 快速开始

### 1. 生成 Vendor 目录

在每个 Go 项目目录下运行：

```bash
# 方式1：直接使用 go 命令
go mod tidy
go mod vendor

# 方式2：使用 Makefile（推荐）
make vendor
```

### 2. 使用 Vendor 构建

```bash
# 使用 vendor 模式构建
go build -mod=vendor -o your-binary ./main.go

# 或使用 Makefile
make build
```

## 各项目 Vendor 命令

### mira-data-service-server

```bash
cd mira-data-service-server
make go_mod_tidy_vendor  # 生成 vendor
make build               # 使用 vendor 构建
```

### data-integrate-test

```bash
cd data-integrate-test
make vendor              # 生成 vendor
make build               # 使用 vendor 构建
```

### ida-access-service-mock

```bash
cd ida-access-service-mock
make vendor              # 生成 vendor
make build               # 使用 vendor 构建
```

### mira-gateway-mock

```bash
cd mira-gateway-mock
make vendor              # 生成 vendor
make build               # 使用 vendor 构建
```

## Docker 构建

在 Docker 构建前，确保已生成 vendor 目录：

```bash
# 1. 生成 vendor
make vendor

# 2. 构建 Docker 镜像
docker build -t your-image .
```

## 优势

✅ **加速构建**：避免每次从网络下载依赖  
✅ **离线构建**：无网络环境也能构建  
✅ **版本锁定**：确保依赖版本一致  
✅ **Docker 缓存**：利用 Docker 层缓存优化构建速度

## 注意事项

⚠️ 如果 vendor 目录不存在，Docker 构建会失败  
⚠️ 更新依赖后记得重新运行 `go mod vendor`  
⚠️ vendor 目录可能很大，建议添加到 `.gitignore`

