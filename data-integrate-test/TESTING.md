# 测试文档

## 概述

本项目使用 Go 标准测试框架进行单元测试和集成测试。测试文件遵循 Go 约定，以 `_test.go` 结尾。

## 测试类型

### 1. 单元测试

测试各个包的核心功能：
- `config/config_test.go` - 配置加载和验证测试
- `strategies/database_strategy_test.go` - 数据库策略工厂测试
- `snapshots/snapshot_exporter_test.go` - 快照导出器测试
- `snapshots/snapshot_importer_test.go` - 快照导入器测试

### 2. 命令行工具测试

测试编译后的工具命令：
- `cmd/export_table/main_test.go` - export-table 工具测试
- `cmd/import_table/main_test.go` - import-table 工具测试
- `cmd/manage_ida/main_test.go` - manage-ida 工具测试

这些测试验证：
- 命令行参数解析
- 参数验证（必需参数、参数格式）
- 配置文件加载
- 路径格式验证（本地/MinIO）

## 运行测试

### 运行所有测试

```bash
make test
# 或
go test ./... -v
```

### 运行特定包的测试

```bash
go test ./config -v
go test ./strategies -v
go test ./snapshots -v
go test ./cmd/export_table -v
go test ./cmd/import_table -v
go test ./cmd/manage_ida -v
```

### 测试覆盖率

#### 生成覆盖率报告

```bash
make test-coverage
```

这会：
1. 运行所有测试
2. 生成 `coverage.out` 文件
3. 生成 HTML 覆盖率报告 `coverage.html`

#### 查看覆盖率百分比

```bash
make test-coverage-short
# 或
go test ./... -cover
```

## 测试命令行工具

### 测试参数解析

命令行工具测试主要验证：

1. **必需参数验证**
   ```go
   // 测试缺少必需参数时是否正确报错
   args := []string{"-db=mysql", "-table=test"} // 缺少 -dbname 和 -output
   // 应该返回错误
   ```

2. **参数格式验证**
   ```go
   // 测试 MinIO 路径格式
   args := []string{"-output=minio://bucket/path"}
   // 应该正确识别为 MinIO 路径
   ```

3. **默认值验证**
   ```go
   // 测试默认参数值
   args := []string{"-db=mysql", "-dbname=testdb", "-table=test", "-output=/tmp"}
   // -config 应该使用默认值 "config/test_config.yaml"
   ```

### 测试工具执行

对于需要实际执行的测试（集成测试），可以使用 `os/exec` 执行编译后的二进制文件：

```go
func TestExportTable_Execution(t *testing.T) {
    cmd := exec.Command("./bin/export-table", 
        "-db=mysql", 
        "-dbname=testdb", 
        "-table=test_table",
        "-output=/tmp/export")
    
    err := cmd.Run()
    // 验证执行结果
}
```

## 测试结构

### 表驱动测试

使用表驱动测试模式，提高测试可读性和可维护性：

```go
func TestLoadConfig(t *testing.T) {
    tests := []struct {
        name      string
        configYAML string
        wantErr   bool
        validate  func(*testing.T, *TestConfig)
    }{
        {
            name: "有效的配置文件",
            configYAML: `...`,
            wantErr: false,
            validate: func(t *testing.T, cfg *TestConfig) {
                // 验证配置内容
            },
        },
        // ...
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // 测试逻辑
        })
    }
}
```

## 测试覆盖目标

### 当前覆盖率

运行 `make test-coverage` 查看当前测试覆盖率。

### 覆盖率目标

- **核心模块**（config, strategies）：目标 > 80%
- **业务逻辑**（snapshots）：目标 > 70%
- **命令行工具**（cmd/*）：目标 > 60%

## Mock 和测试辅助

### Mock 策略

对于需要数据库连接的测试，我们使用 mock 策略：

```go
type mockDatabaseStrategy struct {
    dbType   string
    dbConfig *config.DatabaseConfig
}

func (m *mockDatabaseStrategy) GetDBType() string {
    return m.dbType
}
```

### 临时文件

使用 `t.TempDir()` 创建临时目录，测试结束后自动清理：

```go
func TestSomething(t *testing.T) {
    tmpDir := t.TempDir()
    configPath := filepath.Join(tmpDir, "test_config.yaml")
    // ...
}
```

## 集成测试

### 数据库集成测试

对于需要真实数据库连接的集成测试，需要：

1. 启动测试数据库（MySQL、KingBase、GBase、VastBase）
2. 配置 `config/test_config.yaml`
3. 运行集成测试

```bash
# 使用 Docker Compose 启动测试环境
docker-compose up -d

# 运行集成测试
go test ./... -tags=integration -v
```

### 集成测试标记

集成测试使用 build tags 区分：

```go
// +build integration

package snapshots

import (
    "testing"
)

func TestExportTableSnapshot_Integration(t *testing.T) {
    // 需要真实数据库的测试
}
```

运行集成测试：
```bash
go test ./... -tags=integration -v
```

## 测试最佳实践

### 1. 测试隔离

每个测试应该是独立的，不依赖其他测试的状态：

```go
func TestSomething(t *testing.T) {
    // 使用临时目录
    tmpDir := t.TempDir()
    
    // 每个测试创建新的实例
    exporter := NewSnapshotExporter(tmpDir)
    // ...
}
```

### 2. 错误测试

不仅要测试成功情况，还要测试错误情况：

```go
func TestLoadConfig_FileNotFound(t *testing.T) {
    _, err := LoadConfig("nonexistent.yaml")
    if err == nil {
        t.Error("LoadConfig() should return error for nonexistent file")
    }
}
```

### 3. 边界条件测试

测试边界条件和极端情况：

```go
func TestNewSnapshotImporter(t *testing.T) {
    tests := []struct {
        batchSize int
        wantSize  int
    }{
        {batchSize: 0, wantSize: 5000},      // 零值
        {batchSize: -1, wantSize: 5000},     // 负值
        {batchSize: 1000, wantSize: 1000},  // 正常值
    }
    // ...
}
```

### 4. 命令行工具测试

测试命令行工具时：
- 重置 flag 状态
- 测试参数解析
- 测试参数验证
- 测试默认值

```go
func TestTool_FlagParsing(t *testing.T) {
    // 重置 flag
    flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
    
    // 解析参数
    var dbType = flag.String("db", "", "数据库类型")
    // ...
    
    // 验证参数
    if *dbType == "" {
        t.Error("db should be required")
    }
}
```

## CI/CD 集成

### GitHub Actions 示例

```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-go@v2
        with:
          go-version: '1.21'
      - run: make test
      - run: make test-coverage-short
```

## 常见问题

### Q: 如何测试需要数据库连接的函数？

A: 使用 mock 策略或集成测试。对于单元测试，使用 mock；对于需要真实数据库的测试，使用集成测试标记。

### Q: 如何测试命令行工具？

A: 
1. 测试参数解析和验证（单元测试）
2. 使用 `os/exec` 执行编译后的二进制文件（集成测试）
3. 使用 `flag` 包的测试功能验证参数解析

### Q: 如何测试私有方法？

A: 
1. 通过公开方法间接测试
2. 将私有方法提取到单独的包（internal）
3. 使用集成测试验证功能

### Q: 测试覆盖率不够怎么办？

A: 
1. 识别未覆盖的代码路径
2. 添加相应的测试用例
3. 使用 `go test -coverprofile=coverage.out` 分析未覆盖的代码

## 参考资料

- [Go Testing Package](https://pkg.go.dev/testing)
- [Go Test Coverage](https://go.dev/blog/cover)
- [Table-Driven Tests](https://github.com/golang/go/wiki/TableDrivenTests)
- [Testing Command-Line Tools in Go](https://blog.golang.org/subtests)
