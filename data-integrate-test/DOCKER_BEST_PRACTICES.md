# Docker 镜像最佳实践指南

## 问题分析

当前项目包含多个命令行工具（CLI tools），都是**任务型工具**（Job/Task），而非常驻服务：

| 工具 | 类型 | 执行方式 |
|------|------|----------|
| `data-integrate-test` | 测试工具 | 一次性执行，完成后退出 |
| `export-table` | 数据导出工具 | 一次性执行，完成后退出 |
| `import-table` | 数据导入工具 | 一次性执行，完成后退出 |
| `export-snapshots` | 批量导出工具 | 一次性执行，完成后退出 |
| `import-snapshots` | 批量导入工具 | 一次性执行，完成后退出 |
| `manage-ida` | 管理工具 | 一次性执行，完成后退出 |
| `test-clients` | 测试客户端 | 一次性执行，完成后退出 |

## 两种方案对比

### 方案一：单镜像多工具（当前方案）

**特点**：
- 一个镜像包含所有工具
- 通过 `docker run` 时指定不同的入口命令
- 镜像体积较大，但共享基础层

**优点**：
- ✅ 镜像管理简单，只需维护一个 Dockerfile
- ✅ 共享依赖和基础层，节省存储空间
- ✅ 版本统一，所有工具版本一致
- ✅ 适合工具之间有共享代码的场景
- ✅ 构建时间相对较短（一次构建）

**缺点**：
- ❌ 镜像体积较大（包含所有工具）
- ❌ 无法单独更新某个工具
- ❌ 运行时需要指定完整命令路径
- ❌ 不适合 Kubernetes Job 的细粒度管理

**适用场景**：
- 工具数量较少（< 10个）
- 工具之间有共享代码/依赖
- 需要统一版本管理
- 主要用于 CI/CD 或本地开发

### 方案二：多镜像单工具（微服务化）

**特点**：
- 每个工具独立镜像
- 每个镜像只包含一个工具
- 镜像体积小，职责单一

**优点**：
- ✅ 镜像体积小，拉取速度快
- ✅ 可以独立更新和版本管理
- ✅ 适合 Kubernetes Job/CronJob
- ✅ 职责单一，符合单一职责原则
- ✅ 可以针对不同工具优化镜像

**缺点**：
- ❌ 镜像数量多，管理复杂
- ❌ 需要维护多个 Dockerfile
- ❌ 构建时间较长（需要多次构建）
- ❌ 可能存在重复的基础层

**适用场景**：
- 工具数量较多（> 10个）
- 需要独立部署和版本管理
- 使用 Kubernetes 等编排平台
- 不同工具的使用频率差异大

## 推荐方案：混合方案（最佳实践）

根据你的项目情况，推荐使用**混合方案**：

### 核心思路

1. **保留单镜像多工具**作为基础镜像（用于开发、测试）
2. **按需创建单工具镜像**（用于生产环境、Kubernetes）

### 具体实施

#### 1. 基础镜像（单镜像多工具）

保持当前的 `Dockerfile`，作为**开发/测试镜像**：

```dockerfile
# Dockerfile - 包含所有工具
FROM golang:latest AS builder
# ... 构建所有工具 ...

FROM ubuntu:22.04
# ... 复制所有工具 ...
```

**使用方式**：
```bash
# 开发测试
docker run --rm data-integrate-test:latest /app/export-table -db=mysql -dbname=test -table=users -output=/tmp

# CI/CD
docker run --rm data-integrate-test:latest /app/data-integrate-test -template=templates/mysql_1m_8fields.yaml
```

#### 2. 单工具镜像（按需创建）

为生产环境或 Kubernetes 创建单工具镜像，使用**多阶段构建**：

```dockerfile
# Dockerfile.export-table - 仅包含 export-table 工具
FROM golang:latest AS builder
# ... 只构建 export-table ...

FROM ubuntu:22.04
# ... 只复制 export-table ...
ENTRYPOINT ["/app/export-table"]
```

**使用方式**：
```bash
# Kubernetes Job
apiVersion: batch/v1
kind: Job
metadata:
  name: export-table-job
spec:
  template:
    spec:
      containers:
      - name: export-table
        image: data-integrate-test-export-table:latest
        args: ["-db=mysql", "-dbname=test", "-table=users", "-output=/tmp"]
```

#### 3. 使用 Docker Compose 管理

创建 `docker-compose.tools.yml` 来统一管理：

```yaml
services:
  # 基础镜像服务（用于开发）
  data-integrate-test-base:
    build:
      context: .
      dockerfile: Dockerfile
    image: data-integrate-test:latest
    
  # 单工具镜像（可选，按需构建）
  export-table:
    build:
      context: .
      dockerfile: Dockerfile.export-table
    image: data-integrate-test-export-table:latest
    
  import-table:
    build:
      context: .
      dockerfile: Dockerfile.import-table
    image: data-integrate-test-import-table:latest
```

## 最佳实践建议

### 1. 镜像分层策略

**推荐**：使用多阶段构建，最小化最终镜像

```dockerfile
# 阶段1：构建
FROM golang:latest AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make all

# 阶段2：运行（最小化）
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/bin/* /app/
```

### 2. 入口点设计

**单镜像多工具**：使用脚本作为入口点

```dockerfile
# entrypoint.sh
#!/bin/bash
set -e

# 如果没有指定命令，显示帮助
if [ $# -eq 0 ]; then
    echo "可用工具："
    ls -1 /app/
    exit 1
fi

# 执行指定的工具
exec "$@"
```

```dockerfile
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
```

**使用**：
```bash
docker run --rm data-integrate-test:latest /app/export-table -db=mysql ...
```

### 3. 标签策略

使用语义化版本和标签：

```bash
# 基础镜像
data-integrate-test:latest
data-integrate-test:v1.0.0

# 单工具镜像
data-integrate-test-export-table:v1.0.0
data-integrate-test-import-table:v1.0.0
```

### 4. 构建优化

使用 `.dockerignore` 减少构建上下文：

```dockerignore
# .dockerignore
.git
.gitignore
*.md
test-results/
coverage.out
coverage.html
.vscode/
.idea/
```

### 5. 运行时配置

**环境变量**：用于配置，而非命令参数

```dockerfile
ENV CONFIG_PATH=/app/config
ENV LOG_LEVEL=info
```

**卷挂载**：配置文件、数据目录

```bash
docker run -v $(pwd)/config:/app/config:ro \
           -v $(pwd)/data:/app/data \
           data-integrate-test:latest /app/export-table ...
```

## 针对你的项目的具体建议

### 当前阶段（推荐）

**保持单镜像多工具方案**，原因：
1. ✅ 工具数量适中（7个）
2. ✅ 工具之间有共享代码（config、clients等）
3. ✅ 主要用于测试和开发
4. ✅ 当前 Dockerfile 已经优化得很好

### 优化建议

1. **改进入口点**：添加帮助脚本
2. **添加工具别名**：创建符号链接或脚本
3. **优化镜像体积**：使用 distroless 或 alpine（如果兼容）

### 未来扩展

如果未来需要：
- 部署到 Kubernetes
- 独立版本管理
- 更细粒度的权限控制

可以按需创建单工具镜像。

## 实际使用示例

### 单镜像多工具使用方式

#### 方式1：直接指定工具路径（推荐）

```bash
# 导出表
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/data:/app/data \
  data-integrate-test:latest \
  /app/export-table -db=mysql -dbname=test -table=users -output=/app/data/export

# 导入表
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/data:/app/data \
  data-integrate-test:latest \
  /app/import-table -db=mysql -dbname=test -table=users -input=/app/data/export

# 管理 IDA
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  data-integrate-test:latest \
  /app/manage-ida -action=all

# 执行测试
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/templates:/app/templates:ro \
  data-integrate-test:latest \
  /app/data-integrate-test -template=templates/mysql/mysql_1m_8fields.yaml
```

#### 方式2：使用工具名（通过入口点脚本）

```bash
# 入口点脚本会自动查找工具
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  data-integrate-test:latest \
  export-table -db=mysql -dbname=test -table=users -output=/tmp
```

#### 方式3：显示帮助信息

```bash
# 不传参数，显示所有可用工具
docker run --rm data-integrate-test:latest
```

### 单工具镜像使用方式（Kubernetes Job 示例）

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: export-table-job
spec:
  template:
    spec:
      containers:
      - name: export-table
        image: data-integrate-test-export-table:latest
        args:
          - "-db=mysql"
          - "-dbname=production"
          - "-table=users"
          - "-output=minio://bucket/exports/users"
        volumeMounts:
        - name: config
          mountPath: /app/config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: data-integrate-config
      restartPolicy: Never
  backoffLimit: 3
```

### Docker Compose 使用示例

```yaml
# docker-compose.tools.yml
version: '3.8'

services:
  # 导出任务
  export-job:
    image: data-integrate-test:latest
    volumes:
      - ./config:/app/config:ro
      - ./exports:/app/exports
    command: ["/app/export-table", "-db=mysql", "-dbname=test", "-table=users", "-output=/app/exports"]
    profiles:
      - export
    
  # 导入任务
  import-job:
    image: data-integrate-test:latest
    volumes:
      - ./config:/app/config:ro
      - ./exports:/app/exports
    command: ["/app/import-table", "-db=mysql", "-dbname=test", "-table=users", "-input=/app/exports"]
    profiles:
      - import
```

使用：
```bash
# 运行导出任务
docker-compose -f docker-compose.tools.yml --profile export run --rm export-job

# 运行导入任务
docker-compose -f docker-compose.tools.yml --profile import run --rm import-job
```

## 总结

| 场景 | 推荐方案 | 原因 |
|------|----------|------|
| **开发/测试** | 单镜像多工具 | 简单、统一、易用 |
| **CI/CD** | 单镜像多工具 | 构建快、版本统一 |
| **生产环境（少量工具）** | 单镜像多工具 | 管理简单 |
| **生产环境（Kubernetes）** | 多镜像单工具 | 细粒度管理 |
| **工具数量 > 10** | 多镜像单工具 | 避免镜像过大 |

**最终建议**：当前项目**保持单镜像多工具**，这是最适合的方案。如果未来有特定需求（如 Kubernetes 部署），再按需创建单工具镜像。

## 已实施的优化

1. ✅ 创建了 `entrypoint.sh` 入口点脚本，支持多种使用方式
2. ✅ 更新了 `Dockerfile`，使用新的入口点脚本
3. ✅ 创建了 `Dockerfile.export-table.example` 作为单工具镜像的参考模板
4. ✅ 创建了最佳实践文档 `DOCKER_BEST_PRACTICES.md`

## 下一步建议

1. **测试新的入口点脚本**：确保所有工具都能正常工作
2. **按需创建单工具镜像**：如果需要在 Kubernetes 中部署，参考示例 Dockerfile
3. **优化镜像体积**：可以考虑使用 `distroless` 或 `alpine` 基础镜像（需要测试兼容性）
4. **添加健康检查**：对于可能需要长时间运行的工具，添加健康检查机制
