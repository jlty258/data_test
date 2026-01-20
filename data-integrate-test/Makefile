# Makefile for data-integrate-test
# 编译所有工具

# Go 编译参数
GO := go
CGO_ENABLED := 0
LDFLAGS := -w -s
BUILD_FLAGS := CGO_ENABLED=$(CGO_ENABLED) $(GO) build -ldflags '$(LDFLAGS)'

# 输出目录（在 Docker 中会输出到 /build/bin，本地会输出到 ./bin）
BIN_DIR := bin
BINARIES := \
	$(BIN_DIR)/data-integrate-test \
	$(BIN_DIR)/manage-ida \
	$(BIN_DIR)/test-clients \
	$(BIN_DIR)/export-table \
	$(BIN_DIR)/import-table \
	$(BIN_DIR)/export-snapshots \
	$(BIN_DIR)/import-snapshots

# 默认目标
.PHONY: all
all: $(BINARIES)
	@echo "✅ 所有工具编译完成！"

# 创建输出目录
$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

# 编译主程序
$(BIN_DIR)/data-integrate-test: $(BIN_DIR) main.go
	@echo "编译 data-integrate-test..."
	$(BUILD_FLAGS) -o $@ ./main.go

# 编译 manage-ida（合并了 manage-ida 和 query-ida）
$(BIN_DIR)/manage-ida: $(BIN_DIR) cmd/manage_ida/main.go
	@echo "编译 manage-ida..."
	$(BUILD_FLAGS) -o $@ ./cmd/manage_ida/main.go

# 编译 test-clients
$(BIN_DIR)/test-clients: $(BIN_DIR) cmd/test_clients/main.go
	@echo "编译 test-clients..."
	$(BUILD_FLAGS) -o $@ ./cmd/test_clients/main.go

# 编译 export-table
$(BIN_DIR)/export-table: $(BIN_DIR) cmd/export_table/main.go
	@echo "编译 export-table..."
	$(BUILD_FLAGS) -o $@ ./cmd/export_table/main.go

# 编译 import-table
$(BIN_DIR)/import-table: $(BIN_DIR) cmd/import_table/main.go
	@echo "编译 import-table..."
	$(BUILD_FLAGS) -o $@ ./cmd/import_table/main.go

# 编译 export-snapshots
$(BIN_DIR)/export-snapshots: $(BIN_DIR) cmd/export_snapshots/main.go
	@echo "编译 export-snapshots..."
	$(BUILD_FLAGS) -o $@ ./cmd/export_snapshots/main.go

# 编译 import-snapshots
$(BIN_DIR)/import-snapshots: $(BIN_DIR) cmd/import_snapshots/main.go
	@echo "编译 import-snapshots..."
	$(BUILD_FLAGS) -o $@ ./cmd/import_snapshots/main.go

# 清理编译产物
.PHONY: clean
clean:
	@echo "清理编译产物..."
	@rm -rf $(BIN_DIR)
	@echo "✅ 清理完成！"

# 测试
.PHONY: test
test:
	@echo "运行测试..."
	$(GO) test ./... -v

# 测试覆盖率
.PHONY: test-coverage
test-coverage:
	@echo "运行测试并生成覆盖率报告..."
	@mkdir -p test-results
	$(GO) test ./... -v -coverprofile=coverage.out
	$(GO) tool cover -html=coverage.out -o coverage.html
	@cp coverage.html coverage.out test-results/ 2>/dev/null || true
	@echo "✅ 覆盖率报告已生成: coverage.html"

# 测试覆盖率（仅显示百分比）
.PHONY: test-coverage-short
test-coverage-short:
	@echo "运行测试并显示覆盖率..."
	$(GO) test ./... -cover

# 格式化代码
.PHONY: fmt
fmt:
	@echo "格式化代码..."
	$(GO) fmt ./...

# 检查代码
.PHONY: vet
vet:
	@echo "检查代码..."
	$(GO) vet ./...

# 显示帮助
.PHONY: help
help:
	@echo "可用目标:"
	@echo "  make all          - 编译所有工具（默认）"
	@echo "  make clean        - 清理编译产物"
	@echo "  make test         - 运行测试"
	@echo "  make test-coverage - 运行测试并生成覆盖率报告"
	@echo "  make test-coverage-short - 运行测试并显示覆盖率"
	@echo "  make fmt          - 格式化代码"
	@echo "  make vet          - 检查代码"
	@echo "  make help         - 显示此帮助信息"
	@echo ""
	@echo "单独编译:"
	@echo "  make $(BIN_DIR)/data-integrate-test"
	@echo "  make $(BIN_DIR)/manage-ida"
	@echo "  make $(BIN_DIR)/test-clients"
	@echo "  make $(BIN_DIR)/export-table"
	@echo "  make $(BIN_DIR)/import-table"
	@echo "  make $(BIN_DIR)/export-snapshots"
	@echo "  make $(BIN_DIR)/import-snapshots"
