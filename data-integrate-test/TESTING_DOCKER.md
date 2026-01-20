# 在 Docker 容器中运行测试

## 概述

本项目提供了多种方式在 Docker 容器中运行测试，确保测试环境的一致性和可重复性。

## 方法 1: 使用测试脚本（推荐）

### Linux/macOS

#### 运行所有测试

```bash
./scripts/run-tests.sh
```

#### 运行测试并生成覆盖率报告

```bash
./scripts/run-tests-coverage.sh
```

### Windows (PowerShell)

#### 运行所有测试

```powershell
.\scripts\run-tests.ps1
```

#### 运行测试并生成覆盖率报告

```powershell
.\scripts\run-tests-coverage.ps1
```

## 方法 2: 使用 Docker Compose

### 运行测试

```bash
docker-compose -f docker-compose.test.yml up --build
```

### 运行测试并生成覆盖率报告

修改 `docker-compose.test.yml` 中的 `command` 为 `make test-coverage`，然后运行：

```bash
docker-compose -f docker-compose.test.yml up --build
```

## 方法 3: 直接使用 Docker 命令

### 构建测试镜像

```bash
docker build -f Dockerfile.test -t data-integrate-test:test .
```

### 运行测试

```bash
docker run --rm \
    -v $(pwd):/build \
    data-integrate-test:test \
    make test
```

### 运行测试并生成覆盖率报告

```bash
docker run --rm \
    -v $(pwd):/build \
    data-integrate-test:test \
    make test-coverage
```

### 查看覆盖率报告

覆盖率报告会生成在项目根目录：
- `coverage.html` - HTML 格式的覆盖率报告
- `coverage.out` - 原始覆盖率数据

在浏览器中打开 `coverage.html` 查看详细报告。

## 方法 4: 使用 Makefile（在容器内）

如果已经在容器内，可以直接使用 Makefile：

```bash
# 进入容器
docker run -it --rm \
    -v $(pwd):/build \
    data-integrate-test:test \
    /bin/bash

# 在容器内运行
make test
# 或
make test-coverage
```

## 测试结果

测试结果会保存在以下位置：

- **测试输出日志**: `test-results/test-output.log`
- **覆盖率输出日志**: `test-results/test-coverage-output.log`
- **覆盖率报告**: `coverage.html` 和 `coverage.out`

## 环境要求

- Docker 已安装并运行
- 至少 2GB 可用磁盘空间（用于 Docker 镜像和测试结果）

## 常见问题

### Q: 测试失败，如何查看详细错误信息？

A: 查看 `test-results/test-output.log` 文件，其中包含完整的测试输出。

### Q: 如何只运行特定包的测试？

A: 修改 Dockerfile.test 中的 CMD，或使用 docker run 覆盖命令：

```bash
docker run --rm \
    -v $(pwd):/build \
    data-integrate-test:test \
    go test ./config -v
```

### Q: 如何清理测试结果？

A: 删除 `test-results` 目录和覆盖率文件：

```bash
rm -rf test-results coverage.html coverage.out
```

### Q: 测试镜像很大，如何优化？

A: 测试镜像基于 `golang:latest`，包含了完整的 Go 工具链。如果需要更小的镜像，可以使用多阶段构建，但会增加构建复杂度。

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
      
      - name: Run tests in Docker
        run: |
          docker build -f Dockerfile.test -t data-integrate-test:test .
          docker run --rm data-integrate-test:test make test
      
      - name: Generate coverage report
        run: |
          docker run --rm \
            -v ${{ github.workspace }}:/build \
            data-integrate-test:test \
            make test-coverage
      
      - name: Upload coverage
        uses: codecov/codecov-action@v2
        with:
          file: ./coverage.out
```

## 优势

使用 Docker 运行测试的优势：

1. **环境一致性** - 所有开发者使用相同的测试环境
2. **隔离性** - 测试不会影响本地环境
3. **可重复性** - 测试结果可重复
4. **CI/CD 友好** - 易于集成到 CI/CD 流程
5. **依赖管理** - 不需要在本地安装所有依赖

## 注意事项

1. **卷挂载** - 确保项目目录有正确的权限
2. **网络** - 如果需要访问外部服务，可能需要配置网络
3. **资源限制** - 大量测试可能需要更多内存和 CPU
4. **缓存** - Docker 层缓存可以加速后续构建
