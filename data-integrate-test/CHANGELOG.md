# 更新日志

## 2024-01-XX - 工具合并与构建优化

### 合并工具

- ✅ **合并 `manage-ida` 和 `query-ida`**
  - 将 `query-ida` 的所有功能合并到 `manage-ida`
  - 新增 `query-all` 操作（查询所有资产和数据源）
  - 统一参数格式，支持分页查询（`-page`, `-size`）
  - 删除 `query-ida` 工具（功能已完全整合）

### 构建优化

- ✅ **引入 Makefile**
  - 统一管理所有工具的编译
  - 更好的 Docker 缓存利用
  - 支持单独编译工具
  - 提供 `make clean`, `make test`, `make fmt`, `make vet` 等命令

- ✅ **优化 Dockerfile**
  - 使用 Makefile 编译，替代多个 RUN 命令
  - 更好的缓存层利用
  - 编译产物输出到 `bin/` 目录

### 工具更新

**manage-ida** 新增功能：
- `-action=query-all`: 查询所有（资产列表和关联的数据源）
- `-page`: 页码参数（默认: 1）
- `-size`: 每页条数参数（默认: 10）
- `-action=help`: 显示帮助信息

### 文件变更

- ✅ 更新 `cmd/manage_ida/main.go` - 合并所有功能
- ✅ 更新 `cmd/manage_ida/README.md` - 更新文档
- ✅ 创建 `Makefile` - 统一编译管理
- ✅ 创建 `Makefile.README.md` - Makefile 使用说明
- ✅ 更新 `Dockerfile` - 使用 Makefile 编译
- ✅ 更新 `.gitignore` - 添加 `bin/` 目录
- ✅ 更新 `TOOLS_ANALYSIS.md` - 更新工具分析文档
- ⚠️ `cmd/query_ida/` - 已废弃，可以删除

### 使用方式变更

**之前**：
```bash
# 使用 query-ida 查询
./query-ida -type=asset -page=1 -size=20

# 使用 manage-ida 管理
./manage-ida -action=create-ds
```

**现在**：
```bash
# 统一使用 manage-ida
./manage-ida -action=query-asset -page=1 -size=20
./manage-ida -action=create-ds
./manage-ida -action=query-all -page=1 -size=20
```

### 迁移指南

如果之前使用 `query-ida`，需要更新命令：

| 旧命令 | 新命令 |
|--------|--------|
| `query-ida -type=all` | `manage-ida -action=query-all` |
| `query-ida -type=datasource -ds-id=1000` | `manage-ida -action=query-ds -ds-id=1000` |
| `query-ida -type=asset` | `manage-ida -action=query-asset` |
| `query-ida -type=asset -asset-id=2000` | `manage-ida -action=query-asset -asset-id=2000` |

### 构建方式变更

**之前**：
```bash
# Dockerfile 中直接编译
RUN CGO_ENABLED=0 go build ... && \
    CGO_ENABLED=0 go build ... && \
    ...
```

**现在**：
```bash
# 使用 Makefile
RUN make all
```

**本地开发**：
```bash
# 编译所有工具
make

# 或编译单个工具
make bin/export-table
```
