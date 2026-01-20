# 快照导出工具

这个工具用于导出根据模板创建的所有表的表结构和数据。

## 功能特性

- ✅ 自动扫描所有模板文件
- ✅ 导出表结构（CREATE TABLE 语句）
- ✅ 导出表数据为 CSV 格式
- ✅ 支持 MySQL、KingBase、GBase、VastBase 数据库
- ✅ 使用安全的 Unicode 分隔符（字段分隔符：\u0001，行分隔符：\u2028）

## 使用方法

### 导出所有模板对应的表

```bash
go run cmd/export_snapshots/main.go -templates=templates -config=config/test_config.yaml -output=snapshots/exported
```

### 导出单个模板对应的表

```bash
go run cmd/export_snapshots/main.go -template=templates/mysql/mysql_1k_all_interfaces.yaml -config=config/test_config.yaml -output=snapshots/exported
```

## 参数说明

- `-templates`: 模板文件目录（默认：`templates`）
- `-config`: 配置文件路径（默认：`config/test_config.yaml`）
- `-output`: 快照输出目录（默认：`snapshots/exported`）
- `-template`: 指定单个模板文件（可选，如果不指定则处理所有模板）

## 输出文件格式

对于每个表，会生成两个文件：

1. **表结构文件** (`{template_name}_{table_name}_schema.sql`)
   - 包含完整的 CREATE TABLE 语句
   - 可以直接用于重建表结构

2. **表数据文件** (`{template_name}_{table_name}_data.csv`)
   - CSV 格式，使用自定义分隔符
   - 字段分隔符：`\u0001` (U+0001, SOH)
   - 行分隔符：`\u2028` (U+2028, LINE SEPARATOR)
   - 第一行为列名

## 注意事项

1. 工具会自动查找匹配的表名。如果模板中指定了 `table_name`，会直接使用该表名；否则会查找包含模板名称的表。

2. 如果表不存在，工具会跳过并继续处理下一个模板。

3. 导出的 CSV 文件使用特殊的 Unicode 分隔符，这些字符在普通数据中几乎不会出现，可以安全使用。

4. 二进制数据（BLOB/BYTEA）会以十六进制格式导出（以 `0x` 开头）。

## 示例

```bash
# 导出所有模板的表
go run cmd/export_snapshots/main.go

# 导出指定模板的表
go run cmd/export_snapshots/main.go -template=templates/mysql/mysql_1k_all_interfaces.yaml

# 指定输出目录
go run cmd/export_snapshots/main.go -output=./my_snapshots
```
