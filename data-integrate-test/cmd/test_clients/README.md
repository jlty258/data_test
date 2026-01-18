# 客户端连接测试工具

这个工具用于测试 `data-integrate-test` 项目中的客户端是否可以正常连接到服务端。

## 功能

- 测试 IDA Service 客户端连接
- 测试 Data Service 客户端连接
- 验证客户端方法调用是否正常

## 使用方法

```bash
# 从项目根目录运行
go run cmd/test_clients/main.go

# 或指定配置文件路径
go run cmd/test_clients/main.go -config config/test_config.yaml
```

## 测试内容

### 1. IDA Service 客户端测试
- 连接测试
- `GetPrivateDBConnInfo` - 获取数据源连接信息
- `GetPrivateAssetList` - 获取资产列表
- `GetPrivateAssetInfo` - 获取资产详情

### 2. Data Service 客户端测试
- 连接测试
- `GetTableInfo` - 获取数据源表信息
- `ReadStreamingData` - 流式读取数据（需要有效的资产信息）
- `ReadInternalData` - 从内置数据库读取数据
- `Read` - 新版通用读接口
- `WriteInternalData` - 写入内部数据
- `Write` - 新版通用写接口

## 配置要求

确保 `config/test_config.yaml` 中配置了正确的服务地址：

```yaml
data_service:
  host: "localhost"
  port: 9090

mock_services:
  ida:
    host: "localhost"
    port: 9091
```

## 服务要求

在运行测试前，确保以下服务正在运行：

1. **ida-access-service-mock** - IDA 服务 Mock（端口 9091）
2. **mira-data-service-server** - 数据服务（端口 9090）

可以使用以下命令检查服务状态：

```bash
docker ps --filter "name=ida-access-service-mock" --filter "name=mira-data-service-server"
```

## 预期结果

如果一切正常，应该看到：

```
✓ 连接成功
✓ 调用成功
```

如果连接失败，会显示错误信息。

**注意**：某些测试（如 ReadStreamingData、WriteInternalData）可能需要：
- 有效的资产名称和链信息ID（从 IDA Service 获取）
- 已配置的数据库连接
- 相应的权限

如果这些资源不存在，测试会显示友好的提示信息，这是正常现象。

