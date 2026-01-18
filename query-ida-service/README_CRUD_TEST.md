# IDA Access Service Mock CRUD 测试指南

## 服务状态

✅ **ida-access-service-mock 已成功重启**
- 存储类型: memory（内存存储）
- 端口: 9091（gRPC）
- 状态: 运行中

## 测试方法

### 方法一：使用 data-integrate-test 查询工具（推荐）

这是最简单的方法，工具已经编译完成并可以使用。

```powershell
cd data-integrate-test
$config = (Get-Item "config\test_config.yaml").FullName

# 查询资产列表
.\bin\query-ida.exe -config=$config -type=asset

# 查询数据源
.\bin\query-ida.exe -config=$config -type=datasource -ds-id=1000

# 查询所有
.\bin\query-ida.exe -config=$config -type=all
```

### 方法二：使用 data-integrate-test 测试执行器创建数据

```powershell
cd data-integrate-test

# 编译主程序
go build -o bin\data-integrate-test.exe .

# 使用模板创建数据源和资产
.\bin\data-integrate-test.exe -template=templates\mysql\mysql_1k_8fields.yaml

# 然后查询创建的数据
$config = (Get-Item "config\test_config.yaml").FullName
.\bin\query-ida.exe -config=$config -type=all
```

## 当前支持的 CRUD 操作

### ✅ Create (创建)
- `CreateDataSource` - 创建数据源
- `CreateAsset` - 创建资产

### ✅ Read (查询)
- `GetPrivateDBConnInfo` - 查询数据源
- `GetPrivateAssetList` - 查询资产列表
- `GetPrivateAssetInfo` - 通过ID查询资产详情
- `GetPrivateAssetInfoByEnName` - 通过英文名查询资产详情

### ❌ Update (更新)
- 暂未实现

### ❌ Delete (删除)
- 暂未实现

## 重要说明

1. **只支持 gRPC 接口**
   - ida-access-service-mock 只提供 gRPC 服务
   - 不支持 HTTP REST API
   - 所有操作都需要通过 gRPC 客户端进行

2. **存储类型**
   - 当前使用内存存储（memory）
   - 数据在服务重启后会丢失
   - 可以通过环境变量 `STORAGE_TYPE=memory` 指定

3. **Proto 兼容性**
   - 已添加 `dataProductType` 字段
   - 与 mira-data-service-server 完全兼容

## 测试结果示例

### 查询资产列表
```
========== 查询资产列表 ==========

资产列表 (共 0 条，当前页 1):

  未找到资产
========== 查询完成 ==========
```

### 查询数据源
```
========== 查询数据源 ==========
数据源ID: 1000

  错误: 数据源不存在
========== 查询完成 ==========
```

## 下一步

要测试完整的 CRUD 功能，建议：

1. 使用 `data-integrate-test` 的测试执行器创建数据源和资产
2. 使用 `query-ida` 工具查询创建的数据
3. 验证数据的完整性和正确性

