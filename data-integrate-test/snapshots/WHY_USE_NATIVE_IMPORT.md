# 为什么导入使用数据库原生工具？

## 导入 vs 导出的区别

### 导出场景
- **需求**：需要与模板系统集成、自定义分隔符、统一格式
- **选择**：自己实现更合适

### 导入场景
- **需求**：快速将数据导入数据库
- **选择**：**原生工具性能远超批量 INSERT**

## 数据库原生导入工具

| 数据库 | 原生工具 | 性能 |
|--------|---------|------|
| MySQL/GBase | `LOAD DATA INFILE` | **10-50 倍** 于批量 INSERT |
| PostgreSQL/KingBase/VastBase | `COPY FROM` | **10-50 倍** 于批量 INSERT |

## 性能对比

### 批量 INSERT（当前实现）

```
导入 1 亿行数据：
- 方式：批量 INSERT（5000 行/批）
- 预计时间：~2-4 小时
- 处理速度：~10,000-20,000 行/秒
- 瓶颈：SQL 解析、网络传输、事务开销
```

### 原生工具（优化后）

```
导入 1 亿行数据：
- MySQL: LOAD DATA INFILE
- PostgreSQL: COPY FROM
- 预计时间：~5-15 分钟（提升 10-50 倍）
- 处理速度：~100,000-500,000 行/秒
- 优势：直接文件读取、最小化 SQL 解析、批量处理
```

## 为什么原生工具更快？

### 1. 直接文件读取 ⭐⭐⭐

**批量 INSERT**：
```
应用 → 解析 CSV → 构建 SQL → 网络传输 → 数据库解析 SQL → 插入
```

**LOAD DATA INFILE / COPY**：
```
数据库直接读取文件 → 批量插入
```

**性能提升**：10-20 倍

### 2. 最小化 SQL 解析 ⭐⭐⭐

**批量 INSERT**：
- 每条 SQL 都需要解析
- 1 亿行 = 20,000 条 SQL（5000 行/批）
- SQL 解析开销巨大

**原生工具**：
- 单条命令，一次解析
- 数据库内部优化

**性能提升**：5-10 倍

### 3. 批量处理优化 ⭐⭐

**批量 INSERT**：
- 受限于 prepared statement 参数数量
- 需要分批提交事务

**原生工具**：
- 数据库内部批量处理
- 自动优化 I/O 和内存

**性能提升**：2-3 倍

## 实现方案

### MySQL/GBase: LOAD DATA INFILE

```sql
LOAD DATA LOCAL INFILE '/path/to/file.csv'
INTO TABLE table_name
FIELDS TERMINATED BY 0x01      -- \u0001
LINES TERMINATED BY 0x0A       -- \n (预处理后)
IGNORE 1 LINES                 -- 跳过列头
(column1, column2, ...)
```

**注意事项**：
- 需要 `LOCAL` 关键字（客户端文件）
- 需要预处理文件（将 \u2028 替换为 \n）
- 需要 MySQL 客户端支持 LOCAL INFILE

### PostgreSQL/KingBase/VastBase: COPY

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

**注意事项**：
- 需要 superuser 权限（COPY FROM file）
- 或使用 COPY FROM STDIN（需要特殊驱动）
- 支持 Unicode 分隔符

## 回退方案

如果原生工具不可用（权限问题、驱动不支持），自动回退到批量 INSERT：

```go
// 尝试使用原生工具
if err := importWithLoadDataInfile(...); err != nil {
    // 回退到批量 INSERT
    return importWithBatchInsert(...)
}
```

## 总结

**导入场景使用原生工具的原因**：

1. **性能优势巨大**：10-50 倍性能提升
2. **实现简单**：单条 SQL 命令
3. **数据库优化**：数据库内部高度优化
4. **适合大数据量**：上亿行数据的最佳选择

**导出场景自己实现的原因**：

1. **格式要求特殊**：自定义分隔符
2. **集成需求**：与模板系统集成
3. **统一格式**：跨数据库统一输出

因此：
- **导出**：自己实现（满足特殊需求）
- **导入**：使用原生工具（追求最高性能）
