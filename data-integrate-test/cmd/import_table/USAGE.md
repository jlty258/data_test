# 表导入工具使用指南

## 快速开始

### 编译工具（推荐）

```bash
# 进入项目目录
cd data-integrate-test

# 编译 import-table 工具
go build -ldflags '-w -s' -o import-table ./cmd/import_table/main.go

# 或直接使用镜像中已编译的二进制（在 Docker 中）
```

### 从本地文件系统导入

```bash
# 指定目录（自动查找 schema.sql 和 data.csv）
./import-table \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -input=./exports

# 指定文件路径（基础路径，会自动添加 _schema.sql 和 _data.csv）
./import-table \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -input=./exports/my_table
```

### 从 MinIO 导入

```bash
./import-table \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -input=minio://my-bucket/snapshots/my_table
```

## 在 Docker 中使用

### 方式 1: 从本地文件系统导入（挂载卷）

```bash
# 运行容器，从挂载的目录导入
docker run --rm \
  -v /host/exports:/exports \
  data-integrate-test:latest \
  /app/import-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -input=/exports
```

### 方式 2: 从 MinIO 导入

```bash
# 需要网络连接到 MinIO
docker run --rm \
  --network=host \
  data-integrate-test:latest \
  /app/import-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -input=minio://my-bucket/snapshots/my_table
```

## 完整示例

### 示例 1: 导入 MySQL 表

```bash
./import-table \
  -config=config/test_config.yaml \
  -db=mysql \
  -dbname=test_db \
  -table=users \
  -input=./exports/users
```

### 示例 2: 从 MinIO 导入

```bash
./import-table \
  -config=config/test_config.yaml \
  -db=mysql \
  -dbname=test_db \
  -table=products \
  -input=minio://data-exports/snapshots/products
```

### 示例 3: 导入 KingBase 表

```bash
./import-table \
  -db=kingbase \
  -dbname=test_db \
  -table=orders \
  -input=/tmp/exports/orders \
  -batch=10000
```

## 参数说明

| 参数 | 说明 | 必需 | 默认值 |
|------|------|------|--------|
| `-config` | 配置文件路径 | 否 | `config/test_config.yaml` |
| `-db` | 数据库类型 | **是** | - |
| `-dbname` | 数据库名称 | **是** | - |
| `-table` | 目标表名 | **是** | - |
| `-input` | 输入路径 | **是** | - |
| `-batch` | 批量插入大小 | 否 | 5000 |

## 输入路径格式

### 本地文件系统

支持以下格式：

1. **目录路径**：工具会自动查找 `*_schema.sql` 和 `*_data.csv` 文件
   ```bash
   -input=./exports
   ```

2. **基础文件路径**：工具会自动添加 `_schema.sql` 和 `_data.csv` 后缀
   ```bash
   -input=./exports/my_table
   # 会查找: ./exports/my_table_schema.sql 和 ./exports/my_table_data.csv
   ```

3. **完整文件路径**：如果指定了 `_schema.sql` 或 `_data.csv`，工具会自动查找对应的文件
   ```bash
   -input=./exports/my_table_schema.sql
   # 会自动查找: ./exports/my_table_data.csv
   ```

### MinIO 路径

格式：`minio://bucket/path/to/file`

示例：
- `minio://my-bucket/snapshots/table1`
- `minio://data-exports/2024/01/15/users`

**注意**：
- 不需要文件扩展名
- 工具会自动添加 `_schema.sql` 和 `_data.csv`
- 文件会被下载到临时目录，导入完成后自动清理

## 工作流程

### 从本地文件系统导入

```
1. 解析输入路径
   ↓
2. 查找 schema.sql 和 data.csv 文件
   ↓
3. 验证文件存在
   ↓
4. 连接数据库
   ↓
5. 导入表结构（CREATE TABLE）
   ↓
6. 导入表数据（使用原生工具或批量 INSERT）
   ↓
7. 完成
```

### 从 MinIO 导入

```
1. 解析 MinIO 路径
   ↓
2. 创建 MinIO 客户端
   ↓
3. 创建临时目录
   ↓
4. 下载 schema.sql 和 data.csv 到临时目录
   ↓
5. 连接数据库
   ↓
6. 导入表结构（CREATE TABLE）
   ↓
7. 导入表数据（使用原生工具或批量 INSERT）
   ↓
8. 清理临时文件
   ↓
9. 完成
```

## 性能优化

### 批量大小调整

根据数据量和数据库类型调整批量大小：

```bash
# 小批量（适合小表或网络较慢）
./import-table -batch=1000 ...

# 默认批量（适合大多数场景）
./import-table -batch=5000 ...

# 大批量（适合大表且网络较快）
./import-table -batch=10000 ...
```

### 性能对比

| 方式 | 速度 | 适用场景 |
|------|------|----------|
| 原生工具（LOAD DATA/COPY） | 100,000-500,000 行/秒 | 大数据量，推荐 |
| 批量 INSERT | 10,000-50,000 行/秒 | 原生工具失败时的回退方案 |

## 常见问题

### Q: 表已存在怎么办？

A: 导入前需要先删除表：

```sql
DROP TABLE IF EXISTS my_table;
```

或者使用不同的表名导入。

### Q: 如何导入多个表？

A: 可以编写脚本循环调用：

```bash
#!/bin/bash
tables=("table1" "table2" "table3")
for table in "${tables[@]}"; do
  ./import-table \
    -db=mysql \
    -dbname=test_db \
    -table=$table \
    -input=./exports/$table
done
```

### Q: MinIO 连接失败怎么办？

A: 检查：
1. MinIO 配置是否正确（endpoint, access_key, secret_key）
2. 网络是否可达
3. 权限是否足够
4. 对象是否存在

### Q: 导入速度慢怎么办？

A: 尝试：
1. 增加批量大小（`-batch=10000`）
2. 检查数据库性能
3. 检查网络延迟（如果从 MinIO 导入）
4. 确保使用原生导入工具（LOAD DATA/COPY）

### Q: 如何只导入表结构，不导入数据？

A: 当前版本不支持，可以：
1. 手动执行 schema.sql 文件
2. 或修改工具添加 `-schema-only` 参数

### Q: 支持哪些文件格式？

A: 当前版本支持：
- 表结构：`.sql` 文件（CREATE TABLE 语句）
- 表数据：`.csv` 文件（使用 `\u0001` 字段分隔符，`\u2028` 行分隔符）

## 与导出工具配合使用

### 完整工作流

```bash
# 1. 导出表
./export-table \
  -db=mysql \
  -dbname=source_db \
  -table=my_table \
  -output=minio://backup-bucket/snapshots/my_table

# 2. 导入表（到另一个数据库）
./import-table \
  -db=mysql \
  -dbname=target_db \
  -table=my_table \
  -input=minio://backup-bucket/snapshots/my_table
```

## 注意事项

1. **表名冲突**：如果目标表已存在，导入会失败
2. **数据类型兼容性**：确保源数据库和目标数据库的数据类型兼容
3. **字符集**：确保源和目标使用兼容的字符集
4. **权限要求**：需要 CREATE TABLE 和 INSERT 权限
5. **临时文件**：从 MinIO 导入时会在临时目录创建文件，导入完成后自动清理
