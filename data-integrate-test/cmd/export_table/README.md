# 表数据导出工具

一个简单易用的工具，用于将指定数据库的指定表导出到本地文件系统或 MinIO。

## 功能特性

- ✅ 直接指定数据库和表名，无需模板文件
- ✅ 支持导出到本地文件系统
- ✅ 支持导出到 MinIO（自动创建 bucket）
- ✅ 使用高性能导出引擎（支持上亿行数据）
- ✅ 支持 MySQL、KingBase、GBase、VastBase

## 使用方法

### 导出到本地文件系统

```bash
go run cmd/export_table/main.go \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -output=/path/to/output
```

**输出文件**：
- `/path/to/output/my_table_schema.sql` - 表结构
- `/path/to/output/my_table_data.csv` - 表数据

### 导出到 MinIO

```bash
go run cmd/export_table/main.go \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -output=minio://my-bucket/snapshots/my_table
```

**MinIO 对象**：
- `my-bucket/snapshots/my_table_schema.sql` - 表结构
- `my-bucket/snapshots/my_table_data.csv` - 表数据

## 参数说明

- `-config`: 配置文件路径（默认：`config/test_config.yaml`）
- `-db`: 数据库类型（mysql/kingbase/gbase/vastbase，**必需**）
- `-dbname`: 数据库名称（**必需**）
- `-table`: 表名（**必需**）
- `-output`: 输出路径（**必需**）
  - 本地路径：`/path/to/output` 或 `./output`
  - MinIO 路径：`minio://bucket/path/to/file`

## 配置要求

### 数据库配置

在 `config/test_config.yaml` 中配置数据库连接信息：

```yaml
databases:
  - type: mysql
    name: mysql_test
    host: localhost
    port: 3306
    user: root
    password: password
    database: test_db
```

### MinIO 配置（可选，仅导出到 MinIO 时需要）

```yaml
minio:
  endpoint: localhost:9000
  access_key: minioadmin
  secret_key: minioadmin
  bucket: my-bucket
```

## 使用示例

### 示例 1: 导出 MySQL 表到本地

```bash
go run cmd/export_table/main.go \
  -db=mysql \
  -dbname=test_db \
  -table=users \
  -output=./exports
```

**输出**：
```
exports/users_schema.sql
exports/users_data.csv
```

### 示例 2: 导出到 MinIO

```bash
go run cmd/export_table/main.go \
  -db=mysql \
  -dbname=test_db \
  -table=users \
  -output=minio://data-exports/snapshots/users
```

**MinIO 对象**：
```
data-exports/snapshots/users_schema.sql
data-exports/snapshots/users_data.csv
```

### 示例 3: 导出 KingBase 表

```bash
go run cmd/export_table/main.go \
  -db=kingbase \
  -dbname=test_db \
  -table=products \
  -output=/tmp/exports
```

## 编译和使用

### 方式对比

| 方式 | 效率 | 适用场景 |
|------|------|----------|
| **编译后运行** | ⭐⭐⭐⭐⭐ 最高 | 生产环境、频繁使用 |
| **go run** | ⭐⭐ 较低 | 开发测试、一次性使用 |

**为什么编译后运行更快？**
- `go run` 每次都要编译代码（1-5秒），然后执行
- 编译后的二进制文件直接执行，无编译开销
- 执行速度相同，但总时间更短

### 编译方式

```bash
# 编译 export-table 工具
cd data-integrate-test
go build -o export-table ./cmd/export_table/main.go

# 或使用优化编译（减小体积，提升性能）
go build -ldflags '-w -s' -o export-table ./cmd/export_table/main.go
```

### 使用编译后的二进制

```bash
# 直接运行编译后的二进制文件
./export-table \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -output=./exports
```

## 在 Docker 镜像中使用

### 构建镜像

```bash
cd data-integrate-test
docker build -t data-integrate-test:latest .
```

**注意**：镜像中已经包含了编译好的 `export-table` 二进制文件，无需使用 `go run`。

### 运行导出

```bash
# 导出到容器内的文件系统（推荐：使用编译后的二进制）
docker run --rm \
  -v /host/path:/exports \
  data-integrate-test:latest \
  /app/export-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -output=/exports

# 导出到 MinIO（需要网络连接）
docker run --rm \
  --network=host \
  data-integrate-test:latest \
  /app/export-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -output=minio://my-bucket/snapshots/my_table
```

**性能对比**：
- 使用 `/app/export-table`（编译后）：启动时间 < 0.1秒
- 使用 `go run`：启动时间 2-5秒（包含编译时间）

## 性能

- **处理速度**：100,000-200,000 行/秒
- **内存使用**：~150MB（稳定）
- **1 亿行数据**：预计 15-30 分钟

## 输出格式

### 表结构文件（.sql）

```sql
CREATE TABLE `my_table` (
  `id` bigint(20) NOT NULL,
  `col_1` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 表数据文件（.csv）

- 字段分隔符：`\u0001` (U+0001)
- 行分隔符：`\u2028` (U+2028)
- 第一行为列名

## 注意事项

1. **MinIO 路径格式**：
   - 必须以 `minio://` 开头
   - 格式：`minio://bucket/path/to/file`
   - 不需要文件扩展名，工具会自动添加 `_schema.sql` 和 `_data.csv`

2. **数据库连接**：
   - 确保数据库配置正确
   - 确保有读取表的权限

3. **MinIO 连接**：
   - 如果 bucket 不存在，工具会自动创建
   - 确保 MinIO 配置正确

## 故障排查

### 表不存在

```
错误: 表 my_table 不存在
解决: 检查表名是否正确，确保数据库名称正确
```

### MinIO 连接失败

```
错误: 创建 MinIO 客户端失败
解决: 检查 MinIO 配置（endpoint, access_key, secret_key）
```

### 权限问题

```
错误: 连接数据库失败
解决: 检查数据库用户权限
```
