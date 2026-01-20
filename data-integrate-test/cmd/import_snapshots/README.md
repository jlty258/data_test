# 快照导入工具

这个工具用于快速导入之前导出的快照数据到数据库。

## 功能特性

- ✅ 使用数据库原生工具（LOAD DATA INFILE / COPY）实现最高性能
- ✅ 自动导入表结构（CREATE TABLE）
- ✅ 自动导入表数据（CSV 格式）
- ✅ 支持 MySQL、KingBase、GBase、VastBase 数据库
- ✅ 自动回退机制（原生工具失败时使用批量 INSERT）

## 性能优势

### 使用原生工具 vs 批量 INSERT

| 方式 | 1 亿行导入时间 | 处理速度 |
|------|--------------|---------|
| **批量 INSERT** | 2-4 小时 | 10K-20K 行/秒 |
| **LOAD DATA INFILE / COPY** | **5-15 分钟** | **100K-500K 行/秒** |
| **性能提升** | **10-50 倍** | **10-25 倍** |

## 使用方法

### 导入所有快照

```bash
go run cmd/import_snapshots/main.go -snapshots=snapshots/exported -db=mysql
```

### 导入单个快照

```bash
go run cmd/import_snapshots/main.go \
  -schema=snapshots/exported/mysql_1k_test_data_schema.sql \
  -data=snapshots/exported/mysql_1k_test_data_data.csv \
  -table=test_data \
  -db=mysql
```

## 参数说明

- `-snapshots`: 快照文件目录（默认：`snapshots/exported`）
- `-config`: 配置文件路径（默认：`config/test_config.yaml`）
- `-schema`: 表结构文件路径（.sql 文件，可选）
- `-data`: 表数据文件路径（.csv 文件，可选）
- `-table`: 目标表名（如果不指定，使用快照文件中的表名）
- `-db`: 数据库类型（mysql/kingbase/gbase/vastbase）
- `-batch`: 批量插入大小（仅用于回退方案，默认 5000 行）

## 工作原理

### 1. MySQL/GBase: LOAD DATA INFILE

```sql
LOAD DATA LOCAL INFILE '/path/to/file.csv'
INTO TABLE table_name
FIELDS TERMINATED BY 0x01      -- \u0001
LINES TERMINATED BY 0x0A       -- \n
IGNORE 1 LINES                 -- 跳过列头
(column1, column2, ...)
```

**特点**：
- 直接文件读取，无需网络传输数据
- 数据库内部批量处理
- 性能：100K-500K 行/秒

### 2. PostgreSQL/KingBase/VastBase: COPY

```sql
COPY table_name (column1, column2, ...)
FROM '/path/to/file.csv'
WITH (
    FORMAT csv,
    DELIMITER E'\x01',         -- \u0001
    NULL '',
    HEADER true                -- 跳过列头
)
```

**特点**：
- 直接文件读取
- 支持自定义分隔符
- 性能：100K-500K 行/秒

### 3. 回退方案：批量 INSERT

如果原生工具不可用（权限问题、驱动不支持），自动回退到批量 INSERT：

```sql
INSERT INTO table_name (col1, col2, ...)
VALUES (?,?), (?,?), ...  -- 5000 行一批
```

## 文件预处理

由于数据库原生工具可能不支持 `\u2028` 行分隔符，工具会自动：

1. 创建临时文件
2. 将 `\u2028` 替换为 `\n`
3. 使用临时文件导入
4. 导入完成后删除临时文件

## 注意事项

### MySQL LOAD DATA LOCAL INFILE

1. **需要启用 LOCAL**：
   - MySQL 客户端需要支持 `LOCAL INFILE`
   - 连接字符串需要包含 `allowLocalInfile=true`

2. **文件路径**：
   - 使用绝对路径
   - Windows 路径需要转换为 `/` 分隔符

3. **权限**：
   - 不需要 FILE 权限（使用 LOCAL）
   - 只需要 INSERT 权限

### PostgreSQL COPY

1. **权限要求**：
   - `COPY FROM file` 需要 superuser 权限
   - 如果失败，自动回退到批量 INSERT

2. **文件位置**：
   - `COPY FROM file` 需要文件在数据库服务器上
   - 如果文件在客户端，需要使用 `COPY FROM STDIN`（需要特殊驱动）

## 性能优化建议

### 对于 1 亿行数据

**推荐配置**：
- 使用原生工具（默认）
- 预计时间：5-15 分钟
- 处理速度：100K-500K 行/秒

### 如果原生工具不可用

**回退方案**：
- 批量 INSERT（5000 行/批）
- 预计时间：2-4 小时
- 处理速度：10K-20K 行/秒

## 示例

```bash
# 导入所有快照到 MySQL
go run cmd/import_snapshots/main.go -snapshots=snapshots/exported -db=mysql

# 导入单个快照到 KingBase
go run cmd/import_snapshots/main.go \
  -schema=snapshots/exported/test_schema.sql \
  -data=snapshots/exported/test_data.csv \
  -table=my_table \
  -db=kingbase

# 使用自定义批量大小（回退方案）
go run cmd/import_snapshots/main.go -batch=10000
```

## 故障排查

### LOAD DATA INFILE 失败

**错误**：`The used command is not allowed with this MySQL version`

**解决**：
- 检查 MySQL 客户端是否支持 LOCAL INFILE
- 连接字符串添加 `allowLocalInfile=true`

### COPY FROM file 失败

**错误**：`must be superuser to COPY to or from a file`

**解决**：
- 使用 superuser 权限
- 或自动回退到批量 INSERT（已实现）

### 文件路径问题

**错误**：`File not found`

**解决**：
- 使用绝对路径
- Windows 路径转换为 `/` 分隔符（已自动处理）
