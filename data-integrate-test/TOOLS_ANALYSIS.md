# 工具功能分析与优化建议

## 现有工具功能整理

### 1. 核心测试工具

| 工具 | 功能 | 使用场景 | 状态 |
|------|------|----------|------|
| `data-integrate-test` | 执行测试模板，创建表并写入数据 | 主测试流程 | ✅ 完善 |

**功能**：
- 加载 YAML 测试模板
- 创建数据源和资产
- 执行测试用例（创建表、写入数据、读取数据）
- 支持多种数据库（MySQL、KingBase、GBase、VastBase）

### 2. IDA 服务管理工具

| 工具 | 功能 | 使用场景 | 状态 |
|------|------|----------|------|
| `manage-ida` | IDA 服务管理（创建和查询数据源、资产） | IDA 服务操作 | ✅ 完善（已合并 query-ida） |

**功能**：
- 创建数据源
- 创建资产
- 查询数据源详情
- 查询资产列表/详情（支持分页）
- 查询所有（资产列表和关联的数据源）

**改进**：
- ✅ 已合并 `query-ida` 的所有功能
- ✅ 统一了参数格式
- ✅ 支持分页查询

### 3. 数据导入导出工具（单表）

| 工具 | 功能 | 使用场景 | 状态 |
|------|------|----------|------|
| `export-table` | 导出单个表到本地/MinIO | 单表导出 | ✅ 完善 |
| `import-table` | 从本地/MinIO 导入单个表 | 单表导入 | ✅ 完善 |

**功能**：
- 直接指定数据库和表名
- 支持本地文件系统和 MinIO
- 高性能导出/导入（支持上亿行）

**优势**：
- 简单易用，无需模板
- 适合生产环境

### 4. 快照批量工具（基于模板）

| 工具 | 功能 | 使用场景 | 状态 |
|------|------|----------|------|
| `export-snapshots` | 批量导出快照（基于模板） | 批量导出 | ✅ 完善 |
| `import-snapshots` | 批量导入快照 | 批量导入 | ✅ 完善 |

**功能**：
- 扫描模板目录
- 自动匹配表名
- 批量导出/导入所有表

**优势**：
- 适合批量操作
- 基于模板，自动化程度高

### 5. 测试客户端工具

| 工具 | 功能 | 使用场景 | 状态 |
|------|------|----------|------|
| `test-clients` | 测试客户端功能 | 客户端测试 | ✅ 完善 |

**功能**：
- 测试 Data Service 客户端
- 测试 IDA Service 客户端

## 功能对比分析

### 导出工具对比

| 特性 | export-table | export-snapshots |
|------|--------------|------------------|
| **使用方式** | 直接指定表名 | 基于模板扫描 |
| **适用场景** | 单表导出 | 批量导出 |
| **灵活性** | ⭐⭐⭐⭐⭐ 高 | ⭐⭐⭐ 中 |
| **自动化** | ⭐⭐ 低 | ⭐⭐⭐⭐⭐ 高 |
| **输出** | 本地/MinIO | 本地目录 |

**建议**：
- `export-table` 更适合生产环境单表操作
- `export-snapshots` 更适合批量测试数据导出

### 导入工具对比

| 特性 | import-table | import-snapshots |
|------|--------------|------------------|
| **使用方式** | 直接指定表名 | 扫描目录批量导入 |
| **适用场景** | 单表导入 | 批量导入 |
| **灵活性** | ⭐⭐⭐⭐⭐ 高 | ⭐⭐⭐ 中 |
| **自动化** | ⭐⭐ 低 | ⭐⭐⭐⭐⭐ 高 |
| **输入** | 本地/MinIO | 本地目录 |

## 可以增加的功能

### 1. 数据库管理工具 ⭐⭐⭐⭐⭐

**工具名**：`db-manager` 或 `db-tool`

**功能**：
- 列出所有数据库
- 列出数据库中的所有表
- 查看表结构
- 查看表行数
- 删除表
- 清空表数据
- 检查表是否存在

**使用场景**：
```bash
# 列出所有表
./db-manager -db=mysql -dbname=test_db -action=list-tables

# 查看表结构
./db-manager -db=mysql -dbname=test_db -table=users -action=describe

# 查看表行数
./db-manager -db=mysql -dbname=test_db -table=users -action=count

# 删除表
./db-manager -db=mysql -dbname=test_db -table=users -action=drop

# 清空表数据
./db-manager -db=mysql -dbname=test_db -table=users -action=truncate
```

**优先级**：高（常用操作）

### 2. 数据验证工具 ⭐⭐⭐⭐

**工具名**：`verify-data`

**功能**：
- 比较两个表的数据是否一致
- 比较快照文件与数据库表的数据
- 生成差异报告
- 验证数据完整性

**使用场景**：
```bash
# 比较两个表
./verify-data -db1=mysql -dbname1=source_db -table1=users \
              -db2=mysql -dbname2=target_db -table2=users

# 比较快照与表
./verify-data -snapshot=./exports/users -db=mysql -dbname=test_db -table=users
```

**优先级**：中高（数据迁移验证）

### 3. 数据转换工具 ⭐⭐⭐

**工具名**：`convert-data`

**功能**：
- 转换 CSV 格式（分隔符转换）
- 转换字符编码
- 压缩/解压缩快照文件
- 格式转换（CSV -> Parquet, Arrow 等）

**使用场景**：
```bash
# 转换分隔符
./convert-data -input=./exports/users_data.csv \
               -output=./exports/users_standard.csv \
               -from-sep=\u0001 -to-sep=,

# 压缩快照
./convert-data -input=./exports/users -output=./exports/users.tar.gz -compress
```

**优先级**：中（按需）

### 4. 批量操作工具 ⭐⭐⭐⭐

**工具名**：`batch-operations`

**功能**：
- 批量导出多个表
- 批量导入多个表
- 批量删除表
- 批量清空表
- 支持 CSV 文件指定表列表

**使用场景**：
```bash
# 批量导出（从文件读取表列表）
./batch-operations -action=export \
                   -tables-file=tables.txt \
                   -db=mysql -dbname=test_db \
                   -output=./exports

# 批量导入
./batch-operations -action=import \
                   -tables-file=tables.txt \
                   -db=mysql -dbname=test_db \
                   -input=./exports
```

**优先级**：中高（提高效率）

### 5. 性能测试工具 ⭐⭐⭐

**工具名**：`benchmark`

**功能**：
- 测试导出性能
- 测试导入性能
- 生成性能报告
- 对比不同配置的性能

**使用场景**：
```bash
# 测试导出性能
./benchmark -action=export -db=mysql -dbname=test_db -table=large_table -iterations=5

# 测试导入性能
./benchmark -action=import -input=./exports/large_table -db=mysql -dbname=test_db -table=large_table
```

**优先级**：低（开发调试用）

### 6. 数据同步工具 ⭐⭐⭐⭐⭐

**工具名**：`sync-data`

**功能**：
- 同步两个数据库之间的表数据
- 增量同步（基于时间戳或主键）
- 双向同步
- 冲突解决策略

**使用场景**：
```bash
# 全量同步
./sync-data -source-db=mysql -source-dbname=source_db -source-table=users \
            -target-db=mysql -target-dbname=target_db -target-table=users

# 增量同步
./sync-data -source-db=mysql -source-dbname=source_db -source-table=users \
            -target-db=mysql -target-dbname=target_db -target-table=users \
            -incremental -key=id -last-sync-time=2024-01-01T00:00:00Z
```

**优先级**：高（数据同步需求）

### 7. 备份恢复工具 ⭐⭐⭐⭐

**工具名**：`backup-restore`

**功能**：
- 备份整个数据库
- 备份指定表
- 定时备份
- 恢复备份
- 备份压缩和加密

**使用场景**：
```bash
# 备份数据库
./backup-restore -action=backup -db=mysql -dbname=test_db -output=./backups/test_db_20240101

# 恢复数据库
./backup-restore -action=restore -db=mysql -dbname=test_db -input=./backups/test_db_20240101
```

**优先级**：中高（数据安全）

### 8. 数据统计工具 ⭐⭐⭐

**工具名**：`stats`

**功能**：
- 统计表大小
- 统计行数
- 统计列信息
- 生成数据库报告

**使用场景**：
```bash
# 统计表信息
./stats -db=mysql -dbname=test_db -table=users

# 统计整个数据库
./stats -db=mysql -dbname=test_db -all
```

**优先级**：低（辅助工具）

## Dockerfile 优化建议

### 1. 编译优化 ⭐⭐⭐⭐⭐

**当前问题**：
- 所有工具在一个 RUN 命令中编译，任何一个失败都会导致整个构建失败
- 没有并行编译
- 没有缓存优化

**优化方案**：

```dockerfile
# 方案 1: 分离编译（更好的缓存）
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o data-integrate-test ./main.go
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o manage-ida ./cmd/manage_ida/main.go
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o query-ida ./cmd/query_ida/main.go
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o test-clients ./cmd/test_clients/main.go
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o export-table ./cmd/export_table/main.go
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o import-table ./cmd/import_table/main.go
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o export-snapshots ./cmd/export_snapshots/main.go
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o import-snapshots ./cmd/import_snapshots/main.go

# 方案 2: 使用 Makefile（推荐）
COPY Makefile ./
RUN make build-all

# 方案 3: 使用构建脚本
COPY build.sh ./
RUN chmod +x build.sh && ./build.sh
```

**推荐**：方案 2（Makefile），更清晰、可维护

### 2. 版本信息注入 ⭐⭐⭐⭐

**优化**：在编译时注入版本、构建时间等信息

```dockerfile
# 获取版本信息
ARG VERSION=latest
ARG BUILD_TIME
ARG GIT_COMMIT

# 编译时注入版本信息
RUN CGO_ENABLED=0 go build \
    -ldflags "-w -s -X main.Version=${VERSION} -X main.BuildTime=${BUILD_TIME} -X main.GitCommit=${GIT_COMMIT}" \
    -o data-integrate-test ./main.go
```

### 3. 多阶段优化 ⭐⭐⭐

**当前**：已经使用多阶段构建 ✅

**可优化**：
- 使用更小的基础镜像（如 `alpine` 或 `distroless`）
- 只复制必要的文件

```dockerfile
# 使用 distroless（更安全、更小）
FROM gcr.io/distroless/static-debian11:nonroot
COPY --from=builder /build/data-integrate-test /app/
```

### 4. 工具分类组织 ⭐⭐⭐

**建议**：按功能分类组织工具

```dockerfile
# 创建工具目录
RUN mkdir -p /app/tools/{ida,import-export,snapshots,test}

# 复制工具到对应目录
COPY --from=builder /build/manage-ida /app/tools/ida/
COPY --from=builder /build/query-ida /app/tools/ida/
COPY --from=builder /build/export-table /app/tools/import-export/
COPY --from=builder /build/import-table /app/tools/import-export/
# ...
```

### 5. 添加工具帮助脚本 ⭐⭐⭐

**建议**：创建一个统一的工具入口脚本

```bash
#!/bin/bash
# /app/tools.sh

case "$1" in
  export-table)
    shift
    /app/export-table "$@"
    ;;
  import-table)
    shift
    /app/import-table "$@"
    ;;
  *)
    echo "Usage: tools.sh {tool-name} [args...]"
    echo "Available tools:"
    echo "  export-table, import-table, export-snapshots, import-snapshots"
    echo "  manage-ida, query-ida, test-clients"
    exit 1
    ;;
esac
```

## 工具命名规范建议

### 当前命名

- ✅ `export-table` / `import-table` - 清晰
- ✅ `export-snapshots` / `import-snapshots` - 清晰
- ⚠️ `manage-ida` / `query-ida` - 功能有重叠
- ✅ `test-clients` - 清晰

### 建议规范

1. **统一前缀**（可选）：
   - `dit-export-table` (data-integrate-test)
   - 或保持简洁：`export-table`

2. **功能分组**：
   - 导入导出：`export-*`, `import-*`
   - IDA 管理：`ida-*` (如 `ida-manage`, `ida-query`)
   - 数据库管理：`db-*` (如 `db-list`, `db-describe`)
   - 工具类：`verify-*`, `convert-*`, `sync-*`

## 优先级总结

### 高优先级（建议立即实现）

1. ✅ **数据库管理工具** (`db-manager`) - 常用操作
2. ✅ **数据同步工具** (`sync-data`) - 数据迁移需求
3. ✅ **Dockerfile 编译优化** - 提升构建效率

### 中优先级（按需实现）

4. ⚠️ **数据验证工具** (`verify-data`) - 数据迁移验证
5. ⚠️ **批量操作工具** (`batch-operations`) - 提高效率
6. ⚠️ **备份恢复工具** (`backup-restore`) - 数据安全

### 低优先级（可选）

7. ⚠️ **数据转换工具** (`convert-data`) - 按需
8. ⚠️ **性能测试工具** (`benchmark`) - 开发调试
9. ⚠️ **数据统计工具** (`stats`) - 辅助工具

## 最佳实践建议

### 1. 工具组织

```
cmd/
├── db/              # 数据库管理工具
│   ├── manager/     # 数据库管理
│   └── stats/       # 统计工具
├── sync/            # 数据同步工具
│   └── sync-data/
├── verify/          # 验证工具
│   └── verify-data/
├── batch/           # 批量操作
│   └── batch-operations/
└── ...              # 现有工具
```

### 2. 统一接口

所有工具应该：
- 统一的参数格式（`-db`, `-dbname`, `-table`）
- 统一的错误处理
- 统一的日志格式
- 统一的配置加载

### 3. 文档完善

每个工具应该有：
- `README.md` - 功能说明
- `USAGE.md` - 使用指南（可选）
- 命令行帮助（`-h`）

### 4. 测试覆盖

- 单元测试
- 集成测试
- 性能测试（关键工具）

## 总结

**现有工具状态**：✅ 功能完善，覆盖主要使用场景

**主要改进方向**：
1. 增加数据库管理工具（高优先级）
2. 增加数据同步工具（高优先级）
3. 优化 Dockerfile 构建（提升效率）
4. 统一工具接口和命名规范（提升易用性）

**建议**：先实现高优先级工具，再根据实际需求逐步增加其他功能。
