# Makefile 使用说明

## 概述

项目使用 Makefile 统一管理所有工具的编译，提供更好的缓存和并行编译支持。

## 使用方法

### 编译所有工具

```bash
make
# 或
make all
```

这会编译所有工具到 `bin/` 目录：
- `bin/data-integrate-test`
- `bin/manage-ida`
- `bin/test-clients`
- `bin/export-table`
- `bin/import-table`
- `bin/export-snapshots`
- `bin/import-snapshots`

### 编译单个工具

```bash
make bin/data-integrate-test
make bin/manage-ida
make bin/export-table
# ...
```

### 清理编译产物

```bash
make clean
```

### 其他命令

```bash
# 运行测试
make test

# 格式化代码
make fmt

# 检查代码
make vet

# 显示帮助
make help
```

## 优势

### 1. 更好的缓存

每个工具单独编译，Docker 构建时可以更好地利用缓存层：

```dockerfile
# 如果只修改了 export-table，其他工具的编译层会被缓存
COPY Makefile ./
RUN make all
```

### 2. 并行编译（可选）

可以轻松添加并行编译支持：

```makefile
.PHONY: all
all: $(BINARIES)
	@echo "✅ 所有工具编译完成！"

# 并行编译
.PHONY: all-parallel
all-parallel:
	@$(MAKE) -j$(shell nproc) $(BINARIES)
```

### 3. 统一管理

- 统一的编译参数
- 统一的输出目录
- 易于维护和扩展

### 4. 易于扩展

添加新工具只需在 Makefile 中添加一行：

```makefile
$(BIN_DIR)/new-tool: $(BIN_DIR) cmd/new_tool/main.go
	@echo "编译 new-tool..."
	$(BUILD_FLAGS) -o $@ ./cmd/new_tool/main.go
```

## 编译参数

当前使用的编译参数：

- `CGO_ENABLED=0`: 禁用 CGO，纯 Go 编译
- `-ldflags '-w -s'`: 去除调试信息和符号表，减小二进制体积

## 在 Docker 中使用

Dockerfile 已经配置为使用 Makefile：

```dockerfile
COPY Makefile ./
RUN make all
```

编译后的二进制文件位于 `bin/` 目录，Dockerfile 会复制到最终镜像。

## 与直接编译对比

### 直接编译（旧方式）

```dockerfile
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o data-integrate-test ./main.go && \
    CGO_ENABLED=0 go build -ldflags '-w -s' -o manage-ida ./cmd/manage_ida/main.go && \
    ...
```

**问题**：
- 所有工具在一个 RUN 命令中，缓存不友好
- 任何一个失败都会导致整个构建失败
- 难以并行编译

### 使用 Makefile（新方式）

```dockerfile
COPY Makefile ./
RUN make all
```

**优势**：
- 更好的缓存利用
- 清晰的依赖关系
- 易于维护和扩展
- 支持并行编译（可选）

## 最佳实践

1. **本地开发**：使用 `make` 编译所有工具
2. **CI/CD**：使用 `make all` 确保所有工具编译成功
3. **Docker 构建**：使用 `make all`，利用 Docker 缓存层
4. **添加新工具**：在 Makefile 中添加对应的编译规则
