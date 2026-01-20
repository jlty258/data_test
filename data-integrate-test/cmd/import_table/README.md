# 表数据导入工具

一个简单易用的工具，用于将本地文件系统或 MinIO 中的快照文件导入到指定数据库的指定表。

## 功能特性

- ✅ 直接指定数据库和表名，无需模板文件
- ✅ 支持从本地文件系统导入
- ✅ 支持从 MinIO 导入
- ✅ 使用高性能导入引擎（支持上亿行数据）
- ✅ 支持 MySQL、KingBase、GBase、VastBase
- ✅ 自动创建表（如果不存在）

## 使用方法

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

**输入文件**：
- `./exports/my_table_schema.sql` - 表结构
- `./exports/my_table_data.csv` - 表数据

### 从 MinIO 导入

```bash
./import-table \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -input=minio://my-bucket/snapshots/my_table
```

**MinIO 对象**：
- `my-bucket/snapshots/my_table_schema.sql` - 表结构
- `my-bucket/snapshots/my_table_data.csv` - 表数据

## 参数说明

- `-config`: 配置文件路径（默认：`config/test_config.yaml`）
- `-db`: 数据库类型（mysql/kingbase/gbase/vastbase，**必需**）
- `-dbname`: 数据库名称（**必需**）
- `-table`: 目标表名（**必需**）
- `-input`: 输入路径（**必需**）
  - 本地路径：`/path/to/files` 或 `./exports`
  - MinIO 路径：`minio://bucket/path/to/file`
- `-batch`: 批量插入大小（默认：5000行）

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

### MinIO 配置（可选，仅从 MinIO 导入时需要）

```yaml
minio:
  endpoint: localhost:9000
  access_key: minioadmin
  secret_key: minioadmin
  bucket: my-bucket
```

## 使用示例

### 示例 1: 从本地导入 MySQL 表

```bash
./import-table \
  -db=mysql \
  -dbname=test_db \
  -table=users \
  -input=./exports/users
```

### 示例 2: 从 MinIO 导入

```bash
./import-table \
  -db=mysql \
  -dbname=test_db \
  -table=users \
  -input=minio://data-exports/snapshots/users
```

### 示例 3: 导入 KingBase 表

```bash
./import-table \
  -db=kingbase \
  -dbname=test_db \
  -table=products \
  -input=/tmp/exports/products
```

## 编译和使用

### 编译方式

```bash
# 编译 import-table 工具
cd data-integrate-test
go build -ldflags '-w -s' -o import-table ./cmd/import_table/main.go
```

### 使用编译后的二进制

```bash
# 直接运行编译后的二进制文件
./import-table \
  -db=mysql \
  -dbname=test_db \
  -table=my_table \
  -input=./exports
```

## 在 Docker 镜像中使用

### 构建镜像

```bash
cd data-integrate-test
docker build -t data-integrate-test:latest .
```

**注意**：镜像中已经包含了编译好的 `import-table` 二进制文件。

### 运行导入

```bash
# 从本地文件系统导入（挂载卷）
docker run --rm \
  -v /host/exports:/exports \
  data-integrate-test:latest \
  /app/import-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -input=/exports

# 从 MinIO 导入（需要网络连接）
docker run --rm \
  --network=host \
  data-integrate-test:latest \
  /app/import-table \
    -db=mysql \
    -dbname=test_db \
    -table=my_table \
    -input=minio://my-bucket/snapshots/my_table
```

## 性能

- **处理速度**：使用原生工具时 100,000-500,000 行/秒
- **内存使用**：~150MB（稳定）
- **1 亿行数据**：预计 5-15 分钟（取决于数据库类型和网络）

## 输入文件格式

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
   - 确保有创建表和插入数据的权限

3. **表已存在**：
   - 如果表已存在，导入会失败
   - 需要先删除表或使用不同的表名

4. **MinIO 连接**：
   - 确保 MinIO 配置正确
   - 确保有读取对象的权限

## 故障排查

### 表已存在

```
错误: 执行 CREATE TABLE 失败: table already exists
解决: 删除现有表或使用不同的表名
```

### 文件不存在

```
错误: 表结构文件不存在
解决: 检查文件路径是否正确
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
