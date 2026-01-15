# Mock 服务使用说明

本文档说明如何使用 `mira-gateway-mock` 和 `ida-access-service-mock` 两个 Mock 服务。

## 项目结构

```
freeland/
├── mira-data-service-server/    # 原始数据服务
├── mira-gateway-mock/           # Mira Gateway Mock (HTTP服务)
└── ida-access-service-mock/    # IDA Access Service Mock (gRPC服务)
```

## 1. Mira Gateway Mock

### 功能
提供 HTTP API Mock 服务，实现以下端点：
- `POST /v1/GetPrivateDBConnInfo` - 获取数据库连接信息
- `POST /v1/GetPrivateAssetInfoByEnName` - 通过资产英文名称获取资产详情
- `POST /v1/GetResultStorageConfig` - 获取结果存储配置
- `POST /v1/mira/async/notify` - 推送作业结果
- `GET /health` - 健康检查

### 启动

```bash
cd mira-gateway-mock

# 安装依赖
go mod download

# 运行服务（默认端口8080）
go run main.go

# 或指定端口
PORT=8081 go run main.go
```

### 配置 data-service

在 `mira-data-service-server` 中设置环境变量：

```bash
export MIRA_GATEWAY_HOST=localhost
export MIRA_GATEWAY_HOST_PORT=8080
```

## 2. IDA Access Service Mock

### 功能
提供 gRPC Mock 服务，实现以下接口：
- `GetPrivateDBConnInfo` - 获取数据库连接信息
- `GetPrivateAssetInfoByEnName` - 通过资产英文名称获取资产详情

### 生成代码

首先需要生成 gRPC 代码：

```bash
cd ida-access-service-mock

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

### 启动

```bash
cd ida-access-service-mock

# 安装依赖
go mod download

# 运行服务（默认端口9091）
go run main.go

# 或指定端口
PORT=9092 go run main.go
```

### 配置 data-service

在 `mira-data-service-server` 中设置环境变量：

```bash
export IDA_MANAGE_HOST=localhost
export IDA_MANAGE_PORT=9091
```

## 3. 完整启动流程

### 方式一：分别启动

```bash
# 终端1：启动 Mira Gateway Mock
cd mira-gateway-mock
go run main.go

# 终端2：启动 IDA Access Service Mock
cd ida-access-service-mock
make proto  # 首次运行需要生成代码
go run main.go

# 终端3：启动 data-service
cd mira-data-service-server
export MIRA_GATEWAY_HOST=localhost
export MIRA_GATEWAY_HOST_PORT=8080
export IDA_MANAGE_HOST=localhost
export IDA_MANAGE_PORT=9091
go run main.go
```

### 方式二：使用 Makefile

```bash
# 启动 Mira Gateway Mock
cd mira-gateway-mock
make run

# 启动 IDA Access Service Mock
cd ida-access-service-mock
make proto  # 首次运行需要
make run

# 启动 data-service
cd mira-data-service-server
# 设置环境变量后运行
```

## 4. 测试

### 测试 Mira Gateway Mock

```bash
# 健康检查
curl http://localhost:8080/health

# 测试获取数据库连接信息
curl -X POST http://localhost:8080/v1/GetPrivateDBConnInfo \
  -H "Content-Type: application/json" \
  -d '{"requestId":"test-001","dbConnId":1}'
```

### 测试 IDA Access Service Mock

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

## 5. Mock 数据说明

所有接口返回的都是 Mock 数据，可以根据需要修改对应项目中的 `main.go` 文件来调整返回数据。

### 修改 Mock 数据

- **Mira Gateway Mock**: 编辑 `mira-gateway-mock/main.go` 中的处理函数
- **IDA Access Service Mock**: 编辑 `ida-access-service-mock/main.go` 中的服务方法

## 6. 注意事项

1. **端口冲突**: 确保 Mock 服务使用的端口没有被其他服务占用
2. **Proto 代码生成**: IDA Access Service Mock 首次运行前必须生成 proto 代码
3. **环境变量**: data-service 需要正确设置环境变量才能连接到 Mock 服务
4. **数据格式**: Mock 服务返回的数据格式需要与真实服务保持一致

## 7. 故障排查

### 问题：IDA Access Service Mock 编译失败

**解决**: 确保已生成 proto 代码：
```bash
cd ida-access-service-mock
make proto
```

### 问题：data-service 无法连接到 Mock 服务

**解决**: 
1. 检查 Mock 服务是否正在运行
2. 检查环境变量是否正确设置
3. 检查端口是否被占用

### 问题：gRPC 连接失败

**解决**: 
1. 确保 IDA Access Service Mock 已启动
2. 检查端口是否正确（默认9091）
3. 检查防火墙设置

