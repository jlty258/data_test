# 表导出工具使用指南

## 快速开始

### 编译工具（推荐）

```bash
# 进入项目目录
cd data-integrate-test

# 编译 export-table 工具
go build -ldflags '-w -s' -o export-table ./cmd/export_table/main.go

# 或直接使用镜像中已编译的二进制（在 Docker 中）
```

**为什么先编译？**
- ✅ 执行速度更快（无编译开销）
- ✅ 适合生产环境
- ✅ 可以分发二进制文件

### 导出到本地文件系统

```bash
# 使用编译后的二进制（推荐）
./export-table \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -output=./exports

# 或使用 go run（开发测试时）
go run cmd/export_table/main.go \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -output=./exports

# 指定完整路径
./export-table \
  -db=mysql \
  -dbname=test_db \
  -table=users \
  -output=/tmp/exports/users
```

**输出文件**：
- `./exports/my_table_schema.sql`
- `./exports/my_table_data.csv`

### 导出到 MinIO

```bash
./export-table \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -output=minio://my-bucket/snapshots/my_table
```

**MinIO 对象**：
- `my-bucket/snapshots/my_table_schema.sql`
- `my-bucket/snapshots/my_table_data.csv`

## 在 Docker 中使用

### 方式 1: 导出到容器内，然后复制出来

```bash
# 运行容器，导出到容器内的 /exports 目录
docker run --rm \
  -v /host/exports:/exports \
  data-integrate-test:latest \
  /app/export-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -output=/exports
```

### 方式 2: 直接导出到 MinIO

```bash
# 需要网络连接到 MinIO
docker run --rm \
  --network=host \
  data-integrate-test:latest \
  /app/export-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -output=minio://my-bucket/snapshots/my_table
```

### 方式 3: 使用环境变量覆盖配置

```bash
# 通过环境变量传递数据库连接信息（需要修改工具支持）
docker run --rm \
  -e DB_HOST=mysql-server \
  -e DB_PORT=3306 \
  -e DB_USER=root \
  -e DB_PASSWORD=password \
  -e DB_NAME=test_db \
  data-integrate-test:latest \
  /app/export-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -output=minio://my-bucket/snapshots/my_table
```

## 完整示例

### 示例 1: 导出 MySQL 表

```bash
./export-table \
  -config=config/test_config.yaml \
  -db=mysql \
  -dbname=test_db \
  -table=users \
  -output=./exports/users
```

### 示例 2: 导出到 MinIO（自动创建 bucket）

```bash
./export-table \
  -config=config/test_config.yaml \
  -db=mysql \
  -dbname=test_db \
  -table=products \
  -output=minio://data-exports/snapshots/products
```

如果 `data-exports` bucket 不存在，工具会自动创建。

### 示例 3: 导出 KingBase 表

```bash
./export-table \
  -db=kingbase \
  -dbname=test_db \
  -table=orders \
  -output=/tmp/exports/orders
```

## 参数说明

| 参数 | 说明 | 必需 | 默认值 |
|------|------|------|--------|
| `-config` | 配置文件路径 | 否 | `config/test_config.yaml` |
| `-db` | 数据库类型 | **是** | - |
| `-dbname` | 数据库名称 | **是** | - |
| `-table` | 表名 | **是** | - |
| `-output` | 输出路径 | **是** | - |

## 输出路径格式

### 本地文件系统

- 相对路径：`./exports`、`exports/my_table`
- 绝对路径：`/tmp/exports`、`/data/exports/my_table`

**注意**：如果指定的是文件路径（如 `./exports/my_table`），工具会：
- 创建目录：`./exports/`
- 生成文件：`./exports/my_table_schema.sql` 和 `./exports/my_table_data.csv`

### MinIO 路径

格式：`minio://bucket/path/to/file`

示例：
- `minio://my-bucket/snapshots/table1`
- `minio://data-exports/2024/01/15/users`

**注意**：
- 不需要文件扩展名
- 工具会自动添加 `_schema.sql` 和 `_data.csv`
- 如果 bucket 不存在，会自动创建

## 配置文件示例

`config/test_config.yaml`:

```yaml
databases:
  - type: mysql
    name: mysql_test
    host: localhost
    port: 3306
    user: root
    password: password
    database: test_db

minio:
  endpoint: localhost:9000
  access_key: minioadmin
  secret_key: minioadmin
  bucket: my-bucket
```

## 性能

- **处理速度**：100,000-200,000 行/秒
- **内存使用**：~150MB
- **1 亿行数据**：预计 15-30 分钟

## 常见问题

### Q: 如何导出多个表？

A: 可以编写脚本循环调用：

```bash
#!/bin/bash
tables=("table1" "table2" "table3")
for table in "${tables[@]}"; do
  ./export-table \
    -db=mysql \
    -dbname=test_db \
    -table=$table \
    -output=./exports/$table
done
```

### Q: MinIO 连接失败怎么办？

A: 检查：
1. MinIO 配置是否正确（endpoint, access_key, secret_key）
2. 网络是否可达
3. 权限是否足够

### Q: 如何只导出表结构，不导出数据？

A: 当前版本不支持，可以：
1. 导出完整快照
2. 删除 `_data.csv` 文件
3. 或修改工具添加 `-schema-only` 参数

### Q: 如何导出到其他对象存储（如 S3）？

A: 当前版本仅支持 MinIO。如需支持其他存储，可以：
1. 先导出到本地
2. 使用其他工具上传到目标存储
3. 或修改工具添加新的存储后端
