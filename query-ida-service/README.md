# Query IDA Service

查询和测试 IDA Access Service Mock 的工具集。

## 项目说明

### 当前状态

**ida-access-service-mock 服务状态：**
- ✅ 服务已成功重启
- ✅ 使用内存存储 (memory)
- ✅ gRPC 服务运行在端口 9091
- ❌ **不支持 HTTP 接口**（只提供 gRPC 服务）

### 重要发现

1. **ida-access-service-mock 只提供 gRPC 服务**
   - 不支持 HTTP REST API
   - query.sh 和 query.ps1 中的 HTTP 请求无法直接使用
   - 需要通过 gRPC 客户端访问

2. **可以使用 gRPC 客户端查询**
   - 使用 `data-integrate-test/cmd/query_ida` 工具
   - 或使用 gRPC 客户端代码直接调用

## 使用方法

### 方法一：使用 data-integrate-test 工具

```bash
cd data-integrate-test
go build -o bin/query-ida ./cmd/query_ida

# 查询所有
./bin/query-ida -config=config/test_config.yaml -type=all

# 查询数据源
./bin/query-ida -config=config/test_config.yaml -type=datasource -ds-id=1000

# 查询资产列表
./bin/query-ida -config=config/test_config.yaml -type=asset

# 查询资产详情
./bin/query-ida -config=config/test_config.yaml -type=asset -asset-id=2000
```

### 方法二：使用 gRPC 客户端代码

参考 `data-integrate-test/clients/ida_service_client.go` 中的实现。

## CRUD 功能测试

### 当前支持的接口

1. **Create (创建)**
   - ✅ `CreateDataSource` - 创建数据源
   - ✅ `CreateAsset` - 创建资产

2. **Read (查询)**
   - ✅ `GetPrivateDBConnInfo` - 查询数据源
   - ✅ `GetPrivateAssetList` - 查询资产列表
   - ✅ `GetPrivateAssetInfo` - 通过ID查询资产详情
   - ✅ `GetPrivateAssetInfoByEnName` - 通过英文名查询资产详情

3. **Update (更新)**
   - ❌ 暂未实现

4. **Delete (删除)**
   - ❌ 暂未实现

## 测试 CRUD 功能

由于 ida-access-service-mock 只提供 gRPC 服务，需要使用 gRPC 客户端进行测试。

### 使用 data-integrate-test 测试

该工具已经实现了完整的查询功能，可以：
1. 查询资产列表
2. 查询数据源信息
3. 查询资产详情

### 创建测试数据

可以通过 `data-integrate-test` 的测试执行器自动创建数据源和资产，然后使用查询工具查看。

## 注意事项

1. **HTTP vs gRPC**
   - ida-access-service-mock 只提供 gRPC 服务
   - query.sh 和 query.ps1 中的 HTTP 请求无法使用
   - 需要使用 gRPC 客户端

2. **存储类型**
   - 当前使用内存存储
   - 数据在服务重启后会丢失
   - 可以通过环境变量 `STORAGE_TYPE=memory` 指定

3. **Proto 兼容性**
   - 已添加 `dataProductType` 字段
   - 与 mira-data-service-server 完全兼容

