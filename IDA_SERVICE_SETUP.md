# IDA Access Service Mock 设置指南

本文档说明如何启动 `ida-access-service-mock` 容器并提供注册方法。

## 1. 启动 ida-access-service-mock 容器

### 方法一：使用 docker-compose（推荐）

```bash
# 启动所有服务（包括 ida-access-service-mock）
docker-compose -f docker-compose.all.yml up -d ida-access-service-mock

# 或者只启动 ida-access-service-mock
cd ida-access-service-mock
docker-compose up -d
```

### 方法二：使用 Docker 命令

```bash
# 构建镜像
cd ida-access-service-mock
docker build -t ida-access-service-mock:latest .

# 运行容器
docker run -d \
  --name ida-access-service-mock \
  --network mock-services \
  -p 9091:9091 \
  -e PORT=9091 \
  ida-access-service-mock:latest
```

### 方法三：使用提供的脚本

**Linux/Mac:**
```bash
chmod +x start-ida-mock.sh
./start-ida-mock.sh
```

**Windows PowerShell:**
```powershell
.\start-ida-mock.ps1
```

## 2. 验证服务是否启动

```bash
# 检查容器状态
docker ps | grep ida-access-service-mock

# 检查服务日志
docker logs ida-access-service-mock

# 测试 gRPC 连接（需要 grpcurl）
grpcurl -plaintext localhost:9091 list
```

## 3. 配置 data-integrate-test

确保 `data-integrate-test/config/test_config.yaml` 中配置了正确的 IDA 服务地址：

```yaml
mock_services:
  ida:
    host: "localhost"  # 如果在容器中运行，使用 "ida-access-service-mock"
    port: 9091
```

## 4. 注册功能说明

### 4.1 已实现的功能

1. **CreateDataSource（创建数据源）**
   - 在 `ida-access-service-mock/main.go` 中实现
   - 接收数据源配置信息（名称、主机、端口、数据库类型等）
   - 返回模拟的数据源ID

2. **CreateAsset（创建资产）**
   - 在 `ida-access-service-mock/main.go` 中实现
   - 接收资产信息（名称、数据源ID、表名等）
   - 返回模拟的资产ID

3. **自动注册**
   - 在 `test_executor.go` 的 `setup` 方法中自动调用
   - 测试执行前会自动注册数据源和资产

### 4.2 注册流程

```
1. 生成测试数据
   ↓
2. 创建数据源（CreateDataSource）
   - 使用数据库连接信息
   - 返回 dataSourceId
   ↓
3. 创建资产（CreateAsset）
   - 使用数据源ID和表信息
   - 返回 assetId
   ↓
4. 执行测试
```

### 4.3 代码位置

- **Proto 定义**: `ida-access-service-mock/proto/mira_ida_access_service.proto`
- **Mock 实现**: `ida-access-service-mock/main.go`
- **客户端调用**: `data-integrate-test/clients/ida_service_client.go`
- **注册逻辑**: `data-integrate-test/testcases/test_executor.go` (registerToIDAService 方法)

## 5. 使用示例

### 5.1 运行测试（会自动注册）

```bash
# 在容器中运行
docker run --rm --network mock-services \
  -w /home/workspace \
  data-integrate-test:latest \
  sh -c "/home/workspace/bin/data-integrate-test -template=templates/mysql_1m_8fields.yaml"
```

### 5.2 查看注册日志

测试执行时，会输出以下日志：

```
数据源创建成功: ID=1000
资产创建成功: ID=2000, Name=test_mysql_1m_8fields_xxx_mysql_1m_8fields
```

## 6. 注意事项

1. **Proto 编译**: 当前实现使用了简化的请求/响应结构。如果需要完整的 proto 支持，需要：
   - 编译 proto 文件生成 Go 代码
   - 更新 `ida_service_client.go` 使用生成的 proto 类型

2. **网络配置**: 确保容器在同一个 Docker 网络中（`mock-services`）

3. **环境变量**: IDA 服务使用环境变量 `PORT` 指定端口（默认 9091）

4. **Mock 数据**: 当前返回固定的 Mock ID（数据源ID=1000，资产ID=2000）

## 7. 故障排查

### 问题：容器无法启动

```bash
# 查看日志
docker logs ida-access-service-mock

# 检查端口占用
netstat -an | grep 9091
```

### 问题：无法连接到 IDA 服务

```bash
# 检查网络
docker network inspect mock-services

# 测试连接
docker exec -it data-integrate-test ping ida-access-service-mock
```

### 问题：注册失败

- 检查 `test_config.yaml` 中的 IDA 服务配置
- 查看测试日志中的错误信息
- 确认 IDA 服务容器正在运行

## 8. 下一步改进

1. 编译 proto 文件，使用完整的类型定义
2. 实现真实的 ID 生成逻辑（而不是固定值）
3. 添加数据源和资产的查询接口
4. 支持数据源和资产的更新和删除
