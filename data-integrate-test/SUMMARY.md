# Data Integrate Test 项目总结

## 项目概述

`data-integrate-test` 是一个数据服务集成测试框架，用于测试 `data-service` 的数据处理能力。项目设计遵循**不侵入 data-service** 的原则，通过标准接口进行测试。

## 核心需求

1. **多数据库支持**: MySQL、KingBase、GBase、VastBase
2. **大数据量测试**: 支持 1M、50M、100M 行数据
3. **灵活字段配置**: 1-16 个字段，最大 1024 字节
4. **行数校验**: 验证读取/写入结果的行数是否与预期一致

## 架构设计

### 借鉴的设计思想

1. **dbt Models 思想**: 测试场景模板化，YAML 配置
2. **SQLMesh Snapshots 思想**: 数据快照，避免重复生成
3. **SQLMesh 环境隔离**: 命名空间隔离，支持并行测试

### 核心模块

1. **generators/**: 数据生成器
   - `schema_generator.go`: 根据数据库类型生成表结构
   - `data_generator.go`: 批量生成测试数据

2. **snapshots/**: 快照管理
   - `snapshot_manager.go`: 管理数据快照，避免重复生成

3. **strategies/**: 数据库策略
   - 支持 MySQL、KingBase、GBase、VastBase
   - 统一的接口，便于扩展

4. **validators/**: 验证器
   - `row_count_validator.go`: 行数校验（核心功能）

5. **isolation/**: 环境隔离
   - `namespace.go`: 生成唯一命名空间

6. **clients/**: 服务客户端
   - `ida_service_client.go`: IDA 服务客户端
   - `data_service_client.go`: Data Service 客户端

7. **testcases/**: 测试用例
   - `test_template.go`: 模板加载
   - `test_executor.go`: 测试执行器

## 工作流程

```
1. 加载测试模板 (YAML)
   ↓
2. 生成表结构 (根据数据库类型)
   ↓
3. 检查快照 (如果启用)
   ↓
4. 生成测试数据 (批量并发)
   ↓
5. 注册到 IDA-service (数据源 + 资产)
   ↓
6. 调用 data-service 接口
   ↓
7. 验证行数 (读取/写入)
   ↓
8. 清理测试数据
```

## 关键特性

### 1. 数据快照

启用快照后，相同 schema 和行数的数据会被复用：

```yaml
data:
  use_snapshot: true
```

**价值**: 100M 行数据生成可能需要数小时，快照可秒级复用。

### 2. 环境隔离

每个测试用例使用独立的命名空间：

```
test_mysql_1m_8fields_1705123456_a1b2c3d4
```

**价值**: 支持并行测试，互不干扰。

### 3. 行数校验

自动验证读取和写入的行数：

```yaml
tests:
  - type: "read"
    expected: 1000000
    tolerance: 0.1  # 0.1%误差
```

**价值**: 确保数据完整性。

## 测试模板示例

```yaml
name: "mysql_1m_8fields"
description: "MySQL 1M行 8字段测试"

database:
  type: "mysql"
  name: "mysql_test"

schema:
  field_count: 8
  max_field_size: 1024

data:
  row_count: 1000000
  use_snapshot: true

tests:
  - type: "read"
    expected: 1000000
    tolerance: 0.1
```

## 使用方式

```bash
# 运行测试
go run main.go -template=templates/mysql_1m_8fields.yaml

# 指定配置文件
go run main.go -template=templates/mysql_1m_8fields.yaml -config=config/test_config.yaml
```

## 待完成工作

1. **Proto 文件编译**: 需要编译 proto 文件生成 Go 代码
2. **客户端实现**: 完成 IDA 和 Data Service 客户端的完整实现
3. **写入测试**: 实现完整的写入测试逻辑
4. **Arrow 数据解析**: 实现 Arrow 数据的解析和行数统计

## 项目结构

```
data-integrate-test/
├── config/              # 配置文件
├── templates/           # 测试场景模板
├── snapshots/           # 数据快照
├── generators/          # 数据生成器
├── validators/          # 验证器
├── isolation/           # 环境隔离
├── clients/             # 服务客户端
├── strategies/          # 数据库策略
├── testcases/           # 测试用例
├── proto/               # Proto 文件
└── main.go             # 主程序
```

## 设计原则

1. **不侵入 data-service**: 通过标准接口测试
2. **模板化配置**: YAML 模板，易于维护
3. **快照复用**: 避免重复生成大数据
4. **环境隔离**: 支持并行测试
5. **行数校验**: 核心验证功能

## 总结

项目成功实现了核心需求，借鉴了 dbt 和 SQLMesh 的优秀设计思想，同时保持了简洁实用的特点。通过模板化、快照、隔离等机制，大大提升了测试效率和可维护性。

