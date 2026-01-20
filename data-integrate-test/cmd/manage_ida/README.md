# IDA Service 数据源和资产管理工具

这个工具用于管理 IDA Service 中的数据源和资产，包括创建、查询等操作。

## 功能

- ✅ 创建数据源
- ✅ 创建资产
- ✅ 查询数据源详情
- ✅ 查询资产列表（支持分页）
- ✅ 查询资产详情
- ✅ 查询所有（资产列表和关联的数据源）

## 使用方法

### 1. 执行全部操作（创建并查询）

```bash
go run cmd/manage_ida/main.go -action all
```

这会依次执行：
1. 创建数据源
2. 创建资产（使用刚创建的数据源ID）
3. 查询数据源详情
4. 查询资产详情

### 2. 创建数据源

```bash
go run cmd/manage_ida/main.go -action create-ds
```

### 3. 创建资产

```bash
# 需要指定数据源ID
go run cmd/manage_ida/main.go -action create-asset -ds-id 1000
```

### 4. 查询数据源

```bash
go run cmd/manage_ida/main.go -action query-ds -ds-id 1000
```

### 5. 查询资产列表

```bash
go run cmd/manage_ida/main.go -action query-asset
```

### 6. 查询资产详情

```bash
go run cmd/manage_ida/main.go -action query-asset -asset-id 2000
```

### 7. 查询所有（资产列表和关联的数据源）

```bash
# 使用默认分页（第1页，每页10条）
go run cmd/manage_ida/main.go -action query-all

# 指定分页参数
go run cmd/manage_ida/main.go -action query-all -page=1 -size=20
```

## 参数说明

- `-config`: 配置文件路径（默认: `config/test_config.yaml`）
- `-action`: 操作类型
  - `all`: 执行全部操作（创建数据源 -> 创建资产 -> 查询）
  - `create-ds`: 创建数据源
  - `create-asset`: 创建资产
  - `query-ds`: 查询数据源详情（需要 -ds-id）
  - `query-asset`: 查询资产（不指定 -asset-id 时查询列表，支持 -page 和 -size）
  - `query-all`: 查询所有（资产列表和关联的数据源，支持 -page 和 -size）
- `-ds-id`: 数据源ID（创建资产或查询数据源时使用）
- `-asset-id`: 资产ID（查询资产详情时使用）
- `-page`: 页码（查询资产列表时使用，默认: 1）
- `-size`: 每页条数（查询资产列表时使用，默认: 10）

## 示例输出

### 创建数据源
```
1. 创建数据源...
   ✓ 创建成功
     数据源ID: 1000
     消息: success
```

### 创建资产
```
2. 创建资产...
   ✓ 创建成功
     资产ID: 2000
     消息: success
```

### 查询数据源
```
3. 查询数据源 (ID: 1000)...
   ✓ 查询成功
     数据源ID: 1000
     连接名: 测试MySQL数据源
     主机: localhost:3306
     数据库类型: 1
     用户名: root
     数据库名: test_db
```

### 查询资产列表
```
4. 查询资产列表...
   ✓ 查询成功
     总记录数: 1
     当前页: 1
     每页条数: 10
     资产数量: 1
     资产[1]: ID=2000, 名称=测试数据资产, 英文名=test_address_001
```

### 查询资产详情
```
5. 查询资产详情 (ID: 2000)...
   ✓ 查询成功
     资产ID: 2000
     资产编号: ASSET_1768735694
     资产名称: 测试数据资产
     资产英文名: test_data_asset
     资产类型: 1
     数据规模: 1000
     表名: test_table
     数据源ID: 1000
```

## 配置要求

确保 `config/test_config.yaml` 中配置了正确的 IDA 服务地址：

```yaml
mock_services:
  ida:
    host: "localhost"
    port: 9091
```

## 服务要求

在运行工具前，确保 `ida-access-service-mock` 服务正在运行：

```bash
docker ps --filter "name=ida-access-service-mock"
```

## 注意事项

1. 创建资产前需要先创建数据源
2. 数据源ID和资产ID由服务端自动分配
3. 所有操作都使用真实的 gRPC 调用，不是 mock 数据

