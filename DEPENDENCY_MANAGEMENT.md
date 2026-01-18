# Go 依赖管理指南

## 概述

本项目使用 Go Modules 进行依赖管理，并支持使用 `vendor` 目录将依赖保存到本地，避免每次构建时都从网络下载依赖。

## 为什么使用 Vendor？

1. **加速构建**：避免每次构建时都从网络下载依赖
2. **离线构建**：在没有网络的环境下也能构建项目
3. **版本锁定**：确保所有开发者使用相同版本的依赖
4. **Docker 缓存优化**：利用 Docker 层缓存，只在依赖变化时重新下载

## 如何使用 Vendor

### 1. 生成 Vendor 目录

在每个 Go 项目目录下运行：

```bash
# 方式1：使用 go mod vendor 命令
go mod tidy
go mod vendor

# 方式2：使用 Makefile（推荐）
make vendor
```

### 2. 使用 Vendor 构建

生成 vendor 目录后，可以使用以下方式构建：

```bash
# 使用 vendor 模式构建
go build -mod=vendor -o your-binary ./main.go

# 或者使用 Makefile
make build
```

### 3. 项目特定的 Vendor 命令

#### mira-data-service-server

```bash
cd mira-data-service-server
make go_mod_tidy_vendor  # 这会执行 go mod tidy 和 go mod vendor
make build               # 使用 vendor 构建
```

#### data-integrate-test

```bash
cd data-integrate-test
make vendor              # 生成 vendor 目录
make build               # 使用 vendor 构建
```

## Docker 构建优化

### 当前实现

项目中的 Dockerfile 已经优化为使用 vendor 目录：

1. **分层复制**：先复制 `go.mod` 和 `go.sum`，再复制 `vendor` 目录，最后复制源代码
2. **利用缓存**：如果依赖没有变化，Docker 会使用缓存的层，避免重新下载
3. **vendor 优先**：使用 `-mod=vendor` 标志，优先使用本地依赖

### Dockerfile 示例

```dockerfile
# 先复制依赖文件（利用缓存）
COPY go.mod go.sum ./
COPY vendor ./vendor

# 复制源代码
COPY . .

# 使用 vendor 模式构建
RUN go build -mod=vendor -o your-binary ./main.go
```

## 注意事项

1. **vendor 目录大小**：vendor 目录可能很大，建议添加到 `.gitignore` 中，或者使用 Git LFS
2. **版本同步**：确保 `go.mod`、`go.sum` 和 `vendor` 目录保持同步
3. **CI/CD**：在 CI/CD 流程中，可以在构建前运行 `go mod vendor` 来生成 vendor 目录

## 更新依赖

当需要更新依赖时：

```bash
# 1. 更新 go.mod
go get -u package-name
# 或
go mod tidy

# 2. 重新生成 vendor
go mod vendor

# 3. 提交 go.mod 和 go.sum（vendor 可选）
git add go.mod go.sum
# git add vendor  # 如果选择提交 vendor 目录
```

## 常见问题

### Q: vendor 目录应该提交到 Git 吗？

A: 这取决于团队策略：
- **提交 vendor**：优点是可以确保所有开发者使用相同依赖，缺点是会增大仓库体积
- **不提交 vendor**：优点是仓库更小，但需要在构建前运行 `go mod vendor`

本项目建议：**不提交 vendor 目录**，在构建脚本或 CI/CD 中自动生成。

### Q: 如何确保 vendor 目录是最新的？

A: 在构建前运行：
```bash
go mod tidy
go mod vendor
```

### Q: Docker 构建时找不到 vendor 目录？

A: 确保：
1. 构建前已运行 `go mod vendor`
2. Dockerfile 中的 COPY 路径正确
3. `.dockerignore` 没有排除 vendor 目录

## 相关文件

- `mira-data-service-server/Makefile` - 包含 `go_mod_tidy_vendor` 目标
- `data-integrate-test/Makefile` - 包含 `vendor` 目标
- `mira-data-service-server/Dockerfile` - 使用 vendor 的 Docker 构建示例
- `data-integrate-test/Dockerfile` - 使用 vendor 的 Docker 构建示例

