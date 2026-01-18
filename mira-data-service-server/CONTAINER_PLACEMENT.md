# test_import_data.go 容器放置建议

## 容器环境分析

### 当前运行的容器

1. **mira-data-service-server**
   - 服务端口: `9090` (gRPC)
   - Go 环境: ❌ **无**（运行的是编译后的二进制文件）
   - 生成代码: ❌ **无**（容器内不包含源代码和生成的 protobuf 代码）
   - 网络: `freeland_mock-services`
   - 内部地址: `localhost:9090`
   - 容器间访问: `mira-data-service-server:9090`

2. **ida-access-service-mock**
   - 服务端口: `9091` (gRPC), `8081` (HTTP)
   - Go 环境: ❓ 未知（可能是编译后的二进制）
   - 网络: `freeland_mock-services`

3. **data-integrate-test** (如果运行)
   - 用途: 测试容器
   - Go 环境: ❓ 可能有（用于运行测试）
   - 网络: `freeland_mock-services`

## 推荐方案

### ⭐ 方案 1: 使用 Docker Go 镜像临时运行（推荐）

**优点**:
- 不依赖容器内环境
- 可以访问所有容器网络
- 灵活性高
- 不需要修改容器配置

**使用方法**:
```bash
# 方法 A: 使用官方 Go 镜像，挂载代码目录
docker run --rm \
  --network freeland_mock-services \
  -v /root/freeland/mira-data-service-server:/workspace \
  -w /workspace \
  golang:1.23 sh -c "go run test_import_data.go"

# 方法 B: 修改脚本中的连接地址为容器服务名
# 将 localhost:9090 改为 mira-data-service-server:9090
docker run --rm \
  --network freeland_mock-services \
  -v /root/freeland/mira-data-service-server:/workspace \
  -w /workspace \
  -e GOPROXY=https://goproxy.cn,direct \
  golang:1.23 sh -c "cd /workspace && go run test_import_data.go"
```

**需要修改脚本中的地址**:
```go
// 原代码（容器内访问）
conn, err := grpc.NewClient("localhost:9090", ...)

// 改为（容器间访问）
conn, err := grpc.NewClient("mira-data-service-server:9090", ...)
```

---

### ⭐ 方案 2: 放在 data-integrate-test 容器（如果存在且有 Go 环境）

**优点**:
- 专门的测试容器
- 已经有测试框架和依赖
- 可以复用测试配置

**使用方法**:
```bash
# 复制脚本到容器
docker cp test_import_data.go data-integrate-test:/tmp/

# 在容器内运行
docker exec -w /tmp data-integrate-test go run test_import_data.go
```

**前提条件**:
- 容器需要有 Go 环境
- 需要安装依赖包
- 需要修改连接地址为 `mira-data-service-server:9090`

---

### 方案 3: 修改 mira-data-service-server 容器（不推荐）

**缺点**:
- 需要修改 Dockerfile 添加 Go 环境
- 增加镜像大小
- 运行容器不应该包含开发工具

**如果必须这样做**:
```dockerfile
# 在 Dockerfile 中添加开发工具（仅开发环境）
RUN apk add --no-cache go
```

---

### 方案 4: 创建专用测试容器（适合长期使用）

**优点**:
- 独立的测试环境
- 可以包含完整的开发工具
- 可以复用

**docker-compose.yml 添加**:
```yaml
  import-test:
    image: golang:1.23
    container_name: import-test
    volumes:
      - ./mira-data-service-server:/workspace
    working_dir: /workspace
    networks:
      - mock-services
    command: tail -f /dev/null  # 保持运行
    profiles:
      - test
```

**使用方法**:
```bash
# 启动测试容器
docker-compose --profile test up -d import-test

# 运行测试
docker exec import-test go run test_import_data.go
```

---

## 网络连接说明

### 在容器内连接 mira-data-service-server

```go
// 选项 1: 使用服务名（推荐，容器间通信）
conn, err := grpc.NewClient("mira-data-service-server:9090", ...)

// 选项 2: 使用 localhost（仅在 mira-data-service-server 容器内）
conn, err := grpc.NewClient("localhost:9090", ...)

// 选项 3: 使用容器 IP（不推荐，IP 可能变化）
conn, err := grpc.NewClient("172.20.0.4:9090", ...)
```

---

## 最终推荐

### 短期测试：方案 1（Docker Go 镜像）

```bash
# 修改脚本中的地址
sed -i 's/localhost:9090/mira-data-service-server:9090/g' test_import_data.go

# 运行测试
docker run --rm \
  --network freeland_mock-services \
  -v /root/freeland/mira-data-service-server:/workspace \
  -w /workspace \
  -e GOPROXY=https://goproxy.cn,direct \
  golang:1.23 sh -c "go mod download && go run test_import_data.go"
```

### 长期使用：方案 4（专用测试容器）

在 `docker-compose.yml` 中添加测试容器，方便重复使用。

---

## 检查步骤

1. 检查容器网络连接:
```bash
docker exec mira-data-service-server ping -c 1 mira-data-service-server
```

2. 检查端口是否开放:
```bash
docker exec <test-container> nc -zv mira-data-service-server 9090
```

3. 验证 Go 环境（如果在容器内）:
```bash
docker exec <container> go version
```
