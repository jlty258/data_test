# 测试改进建议

## 📊 当前测试状态分析

### 覆盖率现状

| 包名 | 当前覆盖率 | 目标覆盖率 | 差距 | 优先级 |
|------|-----------|-----------|------|--------|
| `config` | 100.0% | 80% | ✅ 超出 | - |
| `strategies` | 19.3% | 80% | -60.7% | 🔴 高 |
| `snapshots` | 4.8% | 70% | -65.2% | 🔴 高 |
| `cmd/*` | 0.0% | 60% | -60% | 🟡 中 |

---

## 🔴 高优先级改进（建议立即实施）

### 1. 添加集成测试 ⭐⭐⭐⭐⭐

**问题**：
- snapshots 包覆盖率仅 4.8%
- 需要真实数据库的测试已跳过
- 无法验证实际的导出/导入功能

**解决方案**：

#### 1.1 创建集成测试框架

```go
// snapshots/snapshot_exporter_integration_test.go
// +build integration

package snapshots

import (
    "context"
    "testing"
    "data-integrate-test/config"
    "data-integrate-test/strategies"
)

func TestExportTableSnapshot_Integration(t *testing.T) {
    if testing.Short() {
        t.Skip("跳过集成测试")
    }
    
    // 使用真实数据库配置
    cfg, _ := config.LoadConfig("config/test_config.yaml")
    dbConfig, _ := cfg.GetDatabaseConfig("mysql")
    
    strategyFactory := strategies.NewDatabaseStrategyFactory()
    strategy, _ := strategyFactory.CreateStrategy(dbConfig)
    
    ctx := context.Background()
    if err := strategy.Connect(ctx); err != nil {
        t.Fatalf("连接数据库失败: %v", err)
    }
    defer strategy.GetDB().Close()
    
    // 执行实际导出测试
    exporter := NewSnapshotExporter("/tmp/test-export")
    err := exporter.ExportTableSnapshot(ctx, strategy, "test_template", "test_table")
    
    if err != nil {
        t.Errorf("导出失败: %v", err)
    }
}
```

#### 1.2 使用 Docker Compose 启动测试数据库

```yaml
# docker-compose.test.yml
version: '3.8'
services:
  mysql-test:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: testpass
      MYSQL_DATABASE: testdb
    ports:
      - "3307:3306"
  
  test-runner:
    build:
      context: .
      dockerfile: Dockerfile.test
    depends_on:
      - mysql-test
    environment:
      - MYSQL_HOST=mysql-test
      - MYSQL_PORT=3306
    command: go test ./... -tags=integration -v
```

**实施步骤**：
1. 创建 `integration_test.go` 文件（使用 build tags）
2. 配置测试数据库环境
3. 添加测试数据准备和清理逻辑
4. 在 CI/CD 中运行集成测试

**预期效果**：
- snapshots 包覆盖率提升到 > 50%
- 验证实际的导出/导入功能
- 发现潜在的数据处理问题

---

### 2. 提升 strategies 包覆盖率 ⭐⭐⭐⭐

**问题**：
- 当前覆盖率仅 19.3%
- 只测试了工厂创建，未测试策略方法
- 未测试数据库连接、查询等功能

**改进方案**：

#### 2.1 添加策略方法测试

```go
// strategies/mysql_strategy_test.go (扩展)

func TestMySQLStrategy_Connect(t *testing.T) {
    // 使用测试数据库或 mock
}

func TestMySQLStrategy_TableExists(t *testing.T) {
    // 测试表存在性检查
}

func TestMySQLStrategy_GetRowCount(t *testing.T) {
    // 测试行数查询
}

func TestMySQLStrategy_Cleanup(t *testing.T) {
    // 测试数据清理
}
```

#### 2.2 使用 SQL Mock

```go
// 使用 sqlmock 库
import "github.com/DATA-DOG/go-sqlmock"

func TestMySQLStrategy_GetRowCount(t *testing.T) {
    db, mock, err := sqlmock.New()
    if err != nil {
        t.Fatalf("创建 mock 失败: %v", err)
    }
    defer db.Close()
    
    // 设置 mock 期望
    rows := sqlmock.NewRows([]string{"count"}).AddRow(100)
    mock.ExpectQuery("SELECT COUNT").WillReturnRows(rows)
    
    strategy := &MySQLStrategy{db: db}
    count, err := strategy.GetRowCount(context.Background(), "test_table")
    
    if err != nil {
        t.Errorf("GetRowCount() error = %v", err)
    }
    if count != 100 {
        t.Errorf("GetRowCount() = %v, want 100", count)
    }
}
```

**实施步骤**：
1. 添加 `github.com/DATA-DOG/go-sqlmock` 依赖
2. 为每个策略方法添加测试
3. 测试错误处理场景
4. 测试边界条件

**预期效果**：
- strategies 包覆盖率提升到 > 60%
- 验证所有策略方法的功能
- 发现潜在的数据库操作问题

---

### 3. 测试工具的实际执行逻辑 ⭐⭐⭐⭐

**问题**：
- cmd/* 工具仅测试参数解析（0% 覆盖率）
- 未测试实际的导出/导入逻辑
- 未测试 MinIO 集成

**改进方案**：

#### 3.1 提取可测试的函数

```go
// cmd/export_table/main.go
// 将 main 函数逻辑提取为可测试的函数

func runExport(ctx context.Context, cfg *config.TestConfig, dbType, dbName, tableName, output string) error {
    // 实际的导出逻辑
    // 可以被测试调用
}

func main() {
    // 参数解析
    // 调用 runExport
}
```

#### 3.2 添加功能测试

```go
// cmd/export_table/main_test.go (扩展)

func TestRunExport_LocalFileSystem(t *testing.T) {
    // 测试本地文件系统导出
}

func TestRunExport_MinIO(t *testing.T) {
    // 测试 MinIO 导出（使用 MinIO 测试容器）
}

func TestRunExport_ErrorHandling(t *testing.T) {
    // 测试各种错误场景
}
```

**实施步骤**：
1. 重构 main 函数，提取业务逻辑
2. 添加功能测试（使用测试数据库）
3. 添加 MinIO 集成测试（使用 MinIO 测试容器）
4. 测试错误处理和边界条件

**预期效果**：
- cmd/* 工具覆盖率提升到 > 40%
- 验证工具的实际功能
- 发现工具使用中的问题

---

## 🟡 中优先级改进（按需实施）

### 4. 使用专业的 Mock 框架 ⭐⭐⭐

**当前问题**：
- 使用手写的 mock 策略
- mock 功能有限，难以模拟复杂场景

**改进方案**：

```go
// 使用 testify/mock
import "github.com/stretchr/testify/mock"

type MockDatabaseStrategy struct {
    mock.Mock
}

func (m *MockDatabaseStrategy) GetDB() *sql.DB {
    args := m.Called()
    return args.Get(0).(*sql.DB)
}

func (m *MockDatabaseStrategy) GetDBType() string {
    args := m.Called()
    return args.String(0)
}
```

**优势**：
- 更强大的 mock 功能
- 自动验证调用次数和参数
- 更好的测试可读性

---

### 5. 添加性能基准测试 ⭐⭐⭐

**改进方案**：

```go
// snapshots/snapshot_exporter_bench_test.go

func BenchmarkExportTableSnapshot(b *testing.B) {
    // 准备测试数据
    // 运行基准测试
    for i := 0; i < b.N; i++ {
        exporter.ExportTableSnapshot(ctx, strategy, "template", "table")
    }
}

func BenchmarkExportLargeTable(b *testing.B) {
    // 测试大数据量导出性能
}
```

**用途**：
- 监控性能回归
- 优化关键路径
- 验证性能改进

---

### 6. 添加测试辅助工具 ⭐⭐

**改进方案**：

```go
// testutils/db_helper.go
package testutils

func CreateTestDatabase(t *testing.T) (*sql.DB, func()) {
    // 创建测试数据库
    // 返回清理函数
}

func LoadTestData(t *testing.T, db *sql.DB, tableName string, rows int) {
    // 加载测试数据
}
```

**优势**：
- 简化测试代码
- 提高测试可维护性
- 统一测试数据管理

---

## 🟢 低优先级改进（可选）

### 7. 测试覆盖率目标管理 ⭐⭐

**改进方案**：

```makefile
# Makefile
.PHONY: test-coverage-check
test-coverage-check:
	@echo "检查覆盖率..."
	@go test ./... -coverprofile=coverage.out
	@go tool cover -func=coverage.out | \
		awk '/config/ {if ($$3+0 < 80) exit 1}' || \
		(echo "❌ config 包覆盖率低于 80%" && exit 1)
	@echo "✅ 覆盖率检查通过"
```

---

### 8. 测试报告自动化 ⭐⭐

**改进方案**：

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run tests
        run: |
          docker build -f Dockerfile.test -t test-image .
          docker run --rm test-image make test-coverage
      - name: Upload coverage
        uses: codecov/codecov-action@v2
        with:
          file: ./coverage.out
```

---

## 📋 改进优先级总结

### 🔴 立即实施（1-2周）

1. **添加集成测试** - 提升 snapshots 覆盖率到 > 50%
2. **提升 strategies 覆盖率** - 使用 sqlmock，目标 > 60%
3. **测试工具执行逻辑** - 提取可测试函数，目标 > 40%

### 🟡 按需实施（2-4周）

4. **Mock 框架集成** - 使用 testify/mock
5. **性能基准测试** - 添加关键功能的基准测试
6. **测试辅助工具** - 创建测试工具库

### 🟢 长期改进（1-2月）

7. **覆盖率目标管理** - 自动化覆盖率检查
8. **CI/CD 集成** - 自动化测试和报告

---

## 🎯 具体实施建议

### 第一步：集成测试（最重要）

**时间**: 3-5 天

1. 创建 `integration_test.go` 文件
2. 配置测试数据库（Docker Compose）
3. 添加 3-5 个关键功能的集成测试
4. 预期覆盖率提升：snapshots 4.8% → 50%+

### 第二步：strategies 测试扩展

**时间**: 2-3 天

1. 添加 `sqlmock` 依赖
2. 为每个策略方法添加测试
3. 测试错误处理和边界条件
4. 预期覆盖率提升：strategies 19.3% → 60%+

### 第三步：工具功能测试

**时间**: 3-4 天

1. 重构 main 函数，提取业务逻辑
2. 添加功能测试
3. 添加 MinIO 集成测试
4. 预期覆盖率提升：cmd/* 0% → 40%+

---

## 📊 预期改进效果

### 覆盖率提升预测

| 包名 | 当前 | 改进后 | 提升 |
|------|------|--------|------|
| `config` | 100.0% | 100.0% | - |
| `strategies` | 19.3% | 60%+ | +40% |
| `snapshots` | 4.8% | 50%+ | +45% |
| `cmd/*` | 0.0% | 40%+ | +40% |
| **总体** | **0.6%** | **25%+** | **+24%** |

### 测试质量提升

1. ✅ **功能验证** - 集成测试验证实际功能
2. ✅ **错误处理** - 更全面的错误场景测试
3. ✅ **性能监控** - 基准测试监控性能
4. ✅ **可维护性** - 测试辅助工具提高效率

---

## 💡 实施建议

### 推荐实施顺序

1. **第1周**：集成测试框架 + snapshots 集成测试
2. **第2周**：strategies 测试扩展 + sqlmock
3. **第3周**：工具功能测试 + MinIO 测试
4. **第4周**：性能测试 + 测试工具库

### 快速开始

**最小可行改进**（1-2天）：
1. 添加 2-3 个关键功能的集成测试
2. 使用 sqlmock 测试 strategies 的 2-3 个方法
3. 预期覆盖率提升：总体 0.6% → 15%+

---

## 📚 参考资料

- [Go Testing Best Practices](https://github.com/golang/go/wiki/TestComments)
- [sqlmock 使用指南](https://github.com/DATA-DOG/go-sqlmock)
- [testify/mock 文档](https://github.com/stretchr/testify#mock-package)
- [Integration Testing in Go](https://www.alexedwards.net/blog/organising-database-access)

---

**总结**：当前测试基础良好，核心功能（config）已完美覆盖。主要改进方向是添加集成测试和提升业务逻辑覆盖率，预计可以将总体覆盖率从 0.6% 提升到 25%+。
