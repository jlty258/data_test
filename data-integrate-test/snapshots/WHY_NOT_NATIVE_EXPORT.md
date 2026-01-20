# 为什么不用数据库原生导出功能？

## 数据库原生导出工具对比

### 原生工具示例

| 数据库 | 原生导出工具 | 命令示例 |
|--------|------------|---------|
| MySQL | `mysqldump` | `mysqldump -u user -p database table > dump.sql` |
| PostgreSQL | `pg_dump` | `pg_dump -U user -t table database > dump.sql` |
| KingBase | `pg_dump` | 同 PostgreSQL |
| GBase | `mysqldump` | 同 MySQL |

## 为什么选择自己实现？

### 1. **与现有框架深度集成** ⭐⭐⭐

**需求**：
- 需要根据**模板文件**自动找到对应的表
- 需要与 `testcases.LoadTemplate()` 集成
- 需要与 `DatabaseStrategy` 策略模式集成
- 需要统一的错误处理和日志

**原生工具的问题**：
```bash
# 需要手动指定表名，无法自动匹配
mysqldump -u user -p database test_mysql_1k_xxx_123456_test_table > dump.sql

# 无法知道模板和表的对应关系
# 无法批量处理多个模板
# 无法与 Go 代码集成
```

**我们的实现**：
```go
// 自动扫描模板 → 自动匹配表 → 自动导出
processAllTemplates() → findMatchingTable() → ExportTableSnapshot()
```

### 2. **统一的输出格式** ⭐⭐⭐

**需求**：
- 所有数据库导出为**相同的格式**（SQL + CSV）
- 文件命名规则统一：`{template_name}_{table_name}_schema.sql`
- CSV 使用**自定义分隔符**（`\u0001` 和 `\u2028`）

**原生工具的问题**：
```bash
# MySQL: 导出为 SQL INSERT 语句
mysqldump database table
# 输出: INSERT INTO table VALUES (...), (...);

# PostgreSQL: 导出为 COPY 格式
pg_dump -t table database
# 输出: COPY table FROM stdin;

# 格式不统一，无法统一处理
# 无法指定自定义分隔符
```

**我们的实现**：
```go
// 所有数据库统一输出：
// 1. schema.sql - CREATE TABLE 语句
// 2. data.csv - 统一使用 \u0001 和 \u2028 分隔符
```

### 3. **自定义分隔符需求** ⭐⭐

**需求**：
- 字段分隔符：`\u0001`（避免与数据冲突）
- 行分隔符：`\u2028`（Unicode 行分隔符）

**原生工具的限制**：
```bash
# mysqldump 只能导出为 SQL INSERT 或 CSV（逗号分隔）
mysqldump --tab=/path --fields-terminated-by=',' database table
# 无法使用 \u0001 这样的控制字符

# pg_dump 只能导出为 COPY 格式或 SQL
pg_dump -t table database
# 无法自定义分隔符
```

**我们的实现**：
```go
fieldSeparator := "\u0001"  // 完全自定义
lineSeparator := "\u2028"   // Unicode 行分隔符
```

### 4. **批量自动化处理** ⭐⭐⭐

**需求**：
- 扫描 `templates/` 目录下的所有 YAML 文件
- 自动匹配表名（支持命名空间前缀）
- 批量导出，单个失败不影响其他

**原生工具的问题**：
```bash
# 需要手动编写脚本
for template in templates/*.yaml; do
    # 解析 YAML 获取表名
    # 调用 mysqldump
    # 处理错误...
done

# 跨数据库需要不同的脚本
# 难以与 Go 代码集成
```

**我们的实现**：
```go
// 一行命令完成所有工作
go run cmd/export_snapshots/main.go -templates=templates
```

### 5. **表名智能匹配** ⭐⭐

**需求**：
- 模板可能指定了 `table_name`，也可能没有
- 表名可能包含命名空间前缀：`test_mysql_1k_xxx_123456_test_table`
- 需要智能匹配

**原生工具的问题**：
```bash
# 必须明确知道表名
mysqldump database exact_table_name

# 无法根据模板名称自动查找
# 无法处理命名空间前缀
```

**我们的实现**：
```go
// 智能匹配逻辑
if template.Schema.TableName != "" {
    tableName = template.Schema.TableName  // 直接使用
} else {
    // 查找包含模板名称的表
    tableName = findMatchingTable(allTables, template.Name)
}
```

### 6. **跨数据库统一接口** ⭐⭐⭐

**需求**：
- 支持 MySQL、KingBase、GBase、VastBase
- 统一的代码接口，无需为每个数据库写不同的脚本

**原生工具的问题**：
```bash
# MySQL
mysqldump -u user -p database table > mysql_dump.sql

# PostgreSQL/KingBase
pg_dump -U user -t table database > pg_dump.sql

# 需要不同的命令和参数
# 无法统一处理
```

**我们的实现**：
```go
// 统一的接口，自动适配不同数据库
strategy := strategyFactory.CreateStrategy(dbConfig)
exporter.ExportTableSnapshot(ctx, strategy, templateName, tableName)
```

### 7. **与测试框架集成** ⭐⭐

**需求**：
- 使用相同的配置系统（`config/test_config.yaml`）
- 使用相同的数据库连接策略
- 使用相同的模板加载逻辑

**原生工具的问题**：
```bash
# 需要单独维护配置
# 无法复用现有的连接池和策略
# 无法与测试框架共享代码
```

**我们的实现**：
```go
// 完全复用现有框架
cfg := config.LoadConfig(*configPath)
strategy := strategies.NewDatabaseStrategyFactory().CreateStrategy(dbConfig)
template := testcases.LoadTemplate(templatePath)
```

### 8. **错误处理和容错** ⭐

**需求**：
- 单个表导出失败不影响其他表
- 详细的错误日志
- 进度反馈

**原生工具的问题**：
```bash
# 如果表不存在，整个命令失败
mysqldump database non_existent_table
# ERROR 1146 (42S02): Table 'database.non_existent_table' doesn't exist

# 需要手动处理错误
# 难以批量处理时的容错
```

**我们的实现**：
```go
// 自动容错
if !exists {
    log.Printf("⚠️  表 %s 不存在，跳过", tableName)
    return nil  // 继续处理下一个
}
```

## 性能对比

### 原生工具（mysqldump/pg_dump）

**优点**：
- ✅ 经过充分优化，性能极佳
- ✅ 支持压缩、并行导出等高级功能
- ✅ 支持增量备份

**缺点**：
- ❌ 无法自定义格式
- ❌ 无法与代码集成
- ❌ 需要外部依赖

### 我们的实现

**优点**：
- ✅ 完全可控，可自定义格式
- ✅ 与框架深度集成
- ✅ 统一的接口和错误处理
- ✅ 智能表名匹配

**缺点**：
- ⚠️ 性能可能略低于原生工具（但对于测试数据量足够）
- ⚠️ 需要维护代码

## 总结

| 维度 | 原生工具 | 我们的实现 | 胜出 |
|------|---------|-----------|------|
| **集成度** | 低（外部工具） | 高（代码集成） | ✅ 我们的实现 |
| **格式统一** | 否（不同数据库不同格式） | 是（统一格式） | ✅ 我们的实现 |
| **自定义分隔符** | 否 | 是 | ✅ 我们的实现 |
| **批量自动化** | 需要脚本 | 内置支持 | ✅ 我们的实现 |
| **表名匹配** | 需要手动指定 | 智能匹配 | ✅ 我们的实现 |
| **性能** | 极高 | 良好 | ✅ 原生工具 |
| **维护成本** | 低（无需维护） | 中（需要维护代码） | ✅ 原生工具 |

## 结论

**选择自己实现的原因**：

1. **业务需求特殊**：需要与模板系统、测试框架深度集成
2. **格式要求特殊**：需要自定义分隔符和统一格式
3. **自动化需求**：需要批量处理、智能匹配表名
4. **集成需求**：需要与现有 Go 代码库无缝集成

**如果只是简单的数据备份**，使用 `mysqldump`/`pg_dump` 更合适。

**但对于测试数据快照管理**，我们的实现更符合项目需求。

## 可能的改进方向

如果未来需要更高性能，可以考虑：

1. **混合方案**：使用原生工具导出，然后转换为统一格式
2. **并行导出**：使用 goroutine 并行处理多个表
3. **流式压缩**：导出时直接压缩，节省空间

但目前的需求下，现有实现已经足够。
