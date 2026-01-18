# IDA Access Service Mock

IDA Access Service 的 Mock 实现，用于开发和测试。

## 功能

实现了以下 gRPC 接口：

- `GetPrivateDBConnInfo` - 获取数据库连接信息
- `GetPrivateAssetInfoByEnName` - 通过资产英文名称获取资产详情
- `GetPrivateAssetList` - 获取资产列表（支持分页和过滤）
- `GetPrivateAssetInfo` - 通过资产ID获取资产详情
- `CreateDataSource` - 创建数据源
- `CreateAsset` - 创建资产

## 存储

服务支持数据持久化存储，当前实现：

- **内存存储** (memory) - 默认存储方式，数据存储在内存中，服务重启后数据会丢失
- **MySQL 存储** (mysql) - 暂未实现

数据会持久化保存，包括：
- 数据源信息
- 资产信息
- 资产字段信息

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
- `STORAGE_TYPE`: 存储类型，可选值：`memory`（默认）、`mysql`（暂未实现）

## 使用

在 data-service 中设置环境变量：

```bash
export IDA_MANAGE_HOST=localhost
export IDA_MANAGE_PORT=9091
```

## 存储说明

服务使用存储层来管理数据：

- **内存存储**：数据存储在内存中，支持所有 CRUD 操作
- 创建的数据源和资产会持久化在存储中
- 查询操作会从存储中读取真实数据

### 使用内存存储（默认）

```bash
# 使用默认内存存储
go run main.go

# 或显式指定
STORAGE_TYPE=memory go run main.go
```

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
