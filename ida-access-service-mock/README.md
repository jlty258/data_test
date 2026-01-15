# IDA Access Service Mock

IDA Access Service 的 Mock 实现，用于开发和测试。

## 功能

实现了以下 gRPC 接口：

- `GetPrivateDBConnInfo` - 获取数据库连接信息
- `GetPrivateAssetInfoByEnName` - 通过资产英文名称获取资产详情

## 本地运行

### 生成代码

首先需要生成 gRPC 代码：

```bash
# 安装必要的工具
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 生成代码
make proto
# 或手动执行
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/*.proto
```

### 运行服务

```bash
# 安装依赖
go mod download

# 运行服务（默认端口9091）
go run main.go

# 或指定端口
PORT=9092 go run main.go
```

## Docker 部署

### 构建镜像

```bash
docker build -t ida-access-service-mock:latest .
```

> 注意：Dockerfile 会自动生成 proto 代码，无需手动生成。

### 运行容器

```bash
docker run -d \
  --name ida-access-service-mock \
  -p 9091:9091 \
  -e PORT=9091 \
  ida-access-service-mock:latest
```

### 使用 Docker Compose

```bash
docker-compose up -d
```

## 环境变量

- `PORT`: 服务监听端口（默认: 9091）

## 使用

在 data-service 中设置环境变量：

```bash
export IDA_MANAGE_HOST=localhost
export IDA_MANAGE_PORT=9091
```

## Mock 数据说明

所有接口返回的都是 Mock 数据，可以根据需要修改 `main.go` 中的响应数据。

## 测试

可以使用 `grpcurl` 工具测试：

```bash
# 安装 grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# 列出服务
grpcurl -plaintext localhost:9091 list

# 调用接口
grpcurl -plaintext -d '{"requestId":"test-001","dbConnId":1}' \
  localhost:9091 mira.MiraIdaAccess/GetPrivateDBConnInfo
```
