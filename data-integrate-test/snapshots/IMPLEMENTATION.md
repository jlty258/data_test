# Snapshots 功能实现原理

## 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    export_snapshots (main.go)                │
│  - 扫描模板目录                                              │
│  - 加载模板配置                                              │
│  - 连接数据库                                                │
│  - 匹配表名                                                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              SnapshotExporter (snapshot_exporter.go)          │
│  ┌────────────────────────┐  ┌──────────────────────────┐   │
│  │  exportTableSchema()   │  │   exportTableData()      │   │
│  │  导出表结构            │  │   导出表数据             │   │
│  └────────────────────────┘  └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│               DatabaseStrategy (策略模式)                     │
│  - MySQLStrategy                                             │
│  - KingBaseStrategy                                          │
│  - GBaseStrategy                                             │
│  - VastbaseStrategy                                          │
└─────────────────────────────────────────────────────────────┘
```

## 核心实现原理

### 1. 模板扫描与表匹配

#### 工作流程

```
1. 遍历模板目录 (filepath.Walk)
   ↓
2. 加载 YAML 模板文件 (testcases.LoadTemplate)
   ↓
3. 根据模板配置连接数据库
   ↓
4. 确定表名：
   - 如果模板指定了 table_name → 直接使用
   - 否则 → 查询所有表，匹配包含模板名称的表
   ↓
5. 检查表是否存在
   ↓
6. 导出快照
```

#### 表名匹配逻辑

```go
// 优先级：
1. 模板中明确指定 table_name
   → 直接使用该表名

2. 模板未指定，但表名包含模板名称
   → 例如：模板名 "mysql_1k_all_interfaces"
   → 匹配表名 "test_mysql_1k_all_interfaces_1234567890_abc123_test_data"

3. 如果都找不到，返回第一个表（容错处理）
```

### 2. 表结构导出原理

#### MySQL/GBase 实现

```sql
-- 使用 MySQL 内置命令直接获取 CREATE TABLE 语句
SHOW CREATE TABLE `table_name`;

-- 返回结果：
-- Table: table_name
-- Create Table: CREATE TABLE `table_name` (...)
```

**优点**：
- 简单直接，数据库原生支持
- 包含完整的表定义（索引、约束、字符集等）
- 无需手动拼接 SQL

#### PostgreSQL/KingBase/VastBase 实现

由于 PostgreSQL 系列数据库没有 `SHOW CREATE TABLE` 命令，需要从 `information_schema` 查询并手动构建：

```sql
-- 1. 查询列信息
SELECT 
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = current_schema()
AND table_name = $1
ORDER BY ordinal_position;

-- 2. 查询主键约束
SELECT column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = current_schema()
AND tc.table_name = $1
AND tc.constraint_type = 'PRIMARY KEY'
ORDER BY kcu.ordinal_position;

-- 3. 手动拼接 CREATE TABLE 语句
CREATE TABLE "table_name" (
  "id" BIGINT NOT NULL,
  "col_1" VARCHAR(255),
  ...
  PRIMARY KEY ("id")
);
```

**构建过程**：
1. 遍历所有列，构建列定义
2. 添加数据类型和长度信息
3. 添加 NULL/NOT NULL 约束
4. 添加默认值
5. 添加主键约束

### 3. 数据导出原理

#### CSV 格式设计

```
字段分隔符: \u0001 (U+0001, SOH - Start of Heading)
行分隔符:   \u2028 (U+2028, LINE SEPARATOR)
```

**为什么选择这些分隔符？**

1. **\u0001 (SOH)**：
   - 控制字符，在普通文本数据中几乎不会出现
   - ASCII 码 1，是第一个可打印字符之前的控制字符
   - 比逗号、制表符等更安全，避免与数据内容冲突

2. **\u2028 (LINE SEPARATOR)**：
   - Unicode 标准定义的行分隔符
   - 专门用于文本行分隔，不会与普通换行符混淆
   - 在大多数编程语言和文本处理工具中都能正确识别

#### 数据导出流程

```
1. 获取表的所有列名
   ↓
2. 构建 SELECT * FROM table 查询
   ↓
3. 执行查询，获取结果集
   ↓
4. 写入 CSV 文件：
   a. 写入列头（列名用 \u0001 分隔）
   b. 遍历每一行数据：
      - 扫描行数据到 values 数组
      - 格式化每个字段的值
      - 用 \u0001 连接所有字段
      - 用 \u2028 作为行结束符
      - 写入文件
   ↓
5. 每 10000 行输出一次进度
```

#### 数据类型处理

```go
formatValue() 函数处理不同类型：

[]byte    → "0x{hex}"        // 二进制数据转十六进制
string    → 直接返回          // 字符串（\u0001 和 \u2028 几乎不会出现）
int64     → "{数字}"
float64   → "{数字}"          // 使用 %g 格式，自动选择最紧凑表示
bool      → "true"/"false"
nil       → ""               // NULL 值转为空字符串
其他类型   → fmt.Sprintf("%v") // 通用转换
```

**为什么不需要转义？**

由于使用了极少出现的 Unicode 控制字符作为分隔符：
- `\u0001` 在普通文本中出现的概率 < 0.001%
- `\u2028` 是专门的行分隔符，普通数据中不会使用
- 因此不需要像标准 CSV 那样处理引号、转义等复杂情况

### 4. 数据库策略模式

使用策略模式支持多种数据库：

```go
DatabaseStrategy 接口：
  - Connect()              // 连接数据库
  - GetDB()               // 获取 *sql.DB
  - GetDBType()           // 获取数据库类型
  - GetConnectionInfo()    // 获取连接配置
  - TableExists()          // 检查表是否存在
  - GetRowCount()          // 获取行数
  - Cleanup()              // 清理表

实现类：
  - MySQLStrategy         // MySQL 和 GBase（使用 MySQL 协议）
  - KingBaseStrategy      // KingBase（PostgreSQL 协议）
  - VastbaseStrategy      // VastBase（PostgreSQL 协议）
```

**不同数据库的 SQL 差异处理**：

| 操作 | MySQL/GBase | KingBase/VastBase |
|------|------------|-------------------|
| 表名引用 | `` `table` `` | `"table"` |
| 参数占位符 | `?` | `$1, $2, ...` |
| 当前数据库 | `DATABASE()` | `current_schema()` |
| CREATE TABLE | `SHOW CREATE TABLE` | 查询 information_schema |

### 5. 文件输出格式

#### 文件命名规则

```
{template_name}_{table_name}_schema.sql  // 表结构
{template_name}_{table_name}_data.csv   // 表数据
```

**示例**：
```
mysql_1k_all_interfaces_test_data_schema.sql
mysql_1k_all_interfaces_test_data_data.csv
```

#### 文件内容示例

**schema.sql**:
```sql
CREATE TABLE `test_data` (
  `id` bigint(20) NOT NULL,
  `col_1` varchar(255) DEFAULT NULL,
  `col_2` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**data.csv** (使用 \u0001 和 \u2028，这里用可见字符表示):
```
id\u0001col_1\u0001col_2\u2028
1\u0001value1\u0001text1\u2028
2\u0001value2\u0001text2\u2028
```

### 6. 错误处理与容错

#### 多层容错机制

1. **模板级别**：
   - 单个模板处理失败不影响其他模板
   - 记录错误日志，继续处理

2. **表匹配级别**：
   - 表不存在 → 跳过，不中断
   - 表名匹配失败 → 记录警告，跳过

3. **数据导出级别**：
   - 查询失败 → 返回错误
   - 文件写入失败 → 返回错误
   - 数据类型转换失败 → 使用通用转换

### 7. 性能优化

1. **流式处理**：
   - 使用 `rows.Next()` 逐行读取，不一次性加载所有数据到内存
   - 适合大数据量导出

2. **进度反馈**：
   - 每 10000 行输出一次进度
   - 让用户了解导出进度

3. **连接管理**：
   - 每个模板处理完后关闭数据库连接
   - 避免连接泄漏

## 关键技术点总结

1. **策略模式**：统一接口，支持多种数据库
2. **Unicode 分隔符**：使用极少出现的控制字符，避免转义
3. **流式处理**：逐行读取，适合大数据量
4. **容错设计**：单个失败不影响整体流程
5. **元数据查询**：通过 information_schema 获取表结构信息

## 使用场景

- **数据备份**：导出表结构和数据用于备份
- **数据迁移**：在不同环境间迁移数据
- **测试数据管理**：保存测试数据快照，便于重复使用
- **数据归档**：将历史数据导出为文件格式
