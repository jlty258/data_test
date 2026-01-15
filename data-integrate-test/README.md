# Data Integrate Test

数据服务集成测试框架，用于测试 data-service 的数据处理能力。

## 功能特性

- ✅ **多数据库支持**: MySQL、KingBase、GBase、VastBase
- ✅ **大数据量测试**: 支持 1M、50M、100M 行数据
- ✅ **灵活字段配置**: 1-16 个字段，最大 1024 字节
- ✅ **数据快照**: 避免重复生成大数据，节省时间
- ✅ **环境隔离**: 支持并行测试，互不干扰
- ✅ **行数校验**: 自动验证读取/写入结果的行数

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
└── main.go             # 主程序
```

## 快速开始

### 1. 编译 proto 文件

```bash
# 需要先编译 proto 文件生成 Go 代码
cd proto
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       *.proto
```

### 2. 配置数据库

编辑 `config/test_config.yaml`，配置数据库连接信息。

### 3. 运行测试

```bash
go run main.go -template=templates/mysql_1m_8fields.yaml
```

## 测试模板

测试模板使用 YAML 格式，示例：

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
  use_snapshot: true  # 使用快照避免重复生成

tests:
  - type: "read"
    expected: 1000000
    tolerance: 0.1  # 允许0.1%误差
```

## 测试类型

- `read`: 测试读取功能
- `write`: 测试写入功能
- `read_write`: 先写后读

## 数据快照

启用快照后，相同 schema 和行数的数据会被复用，避免重复生成：

```yaml
data:
  use_snapshot: true
```

快照存储在 `snapshots/snapshots.db` 中。

## 环境隔离

每个测试用例使用独立的命名空间，支持并行执行：

```bash
go run main.go -template=templates/*.yaml -parallel=5
```

## 行数校验

自动验证读取和写入的行数是否与预期一致，支持误差范围配置：

```yaml
tests:
  - type: "read"
    expected: 1000000
    tolerance: 0.1  # 0.1%误差
```

## 依赖服务

- **data-service**: 被测试的数据服务
- **ida-access-service-mock**: IDA 服务 Mock
- **mira-gateway-mock**: Gateway 服务 Mock

## 注意事项

1. 需要先编译 proto 文件才能使用客户端
2. 大数据量测试（50M+）可能需要较长时间
3. 确保数据库有足够的存储空间
4. 快照功能可以显著减少测试时间

## 开发计划

- [ ] 完成 proto 文件编译
- [ ] 实现完整的客户端调用
- [ ] 支持写入测试
- [ ] 支持 OSS 数据测试
- [ ] 生成测试报告

