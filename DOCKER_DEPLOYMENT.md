# Docker 部署指南

本文档说明如何使用 Docker 部署 Mock 服务。

## 快速开始

### 方式一：使用 Docker Compose 一键启动所有服务

```bash
# 在项目根目录执行
docker-compose -f docker-compose.all.yml up -d

# 查看服务状态
docker-compose -f docker-compose.all.yml ps

# 查看日志
docker-compose -f docker-compose.all.yml logs -f

# 停止服务
docker-compose -f docker-compose.all.yml down
```

### 方式二：分别构建和运行

#### 1. Mira Gateway Mock

```bash
cd mira-gateway-mock

# 构建镜像
docker build -t mira-gateway-mock:latest .

# 运行容器
docker run -d \
  --name mira-gateway-mock \
  -p 8080:8080 \
  -e PORT=8080 \
  mira-gateway-mock:latest

# 或使用 docker-compose
docker-compose up -d
```

#### 2. IDA Access Service Mock

```bash
cd ida-access-service-mock

# 构建镜像
docker build -t ida-access-service-mock:latest .

# 运行容器
docker run -d \
  --name ida-access-service-mock \
  -p 9091:9091 \
  -e PORT=9091 \
  ida-access-service-mock:latest

# 或使用 docker-compose
docker-compose up -d
```

## 镜像特性

### 多阶段构建
- 使用多阶段构建减小镜像大小
- 编译阶段使用 `golang:1.23.0`
- 运行阶段使用 `alpine:latest`（体积小）

### 安全特性
- 使用非 root 用户运行（appuser）
- 最小化运行时依赖
- 只暴露必要的端口

### 健康检查
- Mira Gateway Mock 包含 HTTP 健康检查
- 可通过 `/health` 端点检查服务状态

## 端口映射

| 服务 | 容器端口 | 主机端口 | 说明 |
|------|---------|---------|------|
| mira-gateway-mock | 8080 | 8080 | HTTP API |
| ida-access-service-mock | 9091 | 9091 | gRPC 服务 |

## 环境变量

### Mira Gateway Mock
- `PORT`: 服务监听端口（默认: 8080）

### IDA Access Service Mock
- `PORT`: 服务监听端口（默认: 9091）

## 验证部署

### 检查 Mira Gateway Mock

```bash
# 健康检查
curl http://localhost:8080/health

# 测试API
curl -X POST http://localhost:8080/v1/GetPrivateDBConnInfo \
  -H "Content-Type: application/json" \
  -d '{"requestId":"test-001","dbConnId":1}'
```

### 检查 IDA Access Service Mock

```bash
# 使用 grpcurl 测试（需要先安装）
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# 列出服务
grpcurl -plaintext localhost:9091 list

# 调用接口
grpcurl -plaintext -d '{"requestId":"test-001","dbConnId":1}' \
  localhost:9091 mira.MiraIdaAccess/GetPrivateDBConnInfo
```

## 与 data-service 集成

在运行 data-service 时，设置以下环境变量：

```bash
export MIRA_GATEWAY_HOST=localhost
export MIRA_GATEWAY_HOST_PORT=8080
export IDA_MANAGE_HOST=localhost
export IDA_MANAGE_PORT=9091
```

如果 data-service 也在 Docker 中运行，可以使用 Docker 网络：

```yaml
# 在 docker-compose 中添加
networks:
  - mock-services
```

然后在 data-service 的环境变量中使用服务名：

```bash
export MIRA_GATEWAY_HOST=mira-gateway-mock
export IDA_MANAGE_HOST=ida-access-service-mock
```

## 镜像大小优化

- 使用 Alpine Linux 作为基础镜像（约 5MB）
- 多阶段构建，只复制必要的二进制文件
- 使用 `.dockerignore` 排除不必要的文件

## 故障排查

### 问题：构建失败

**检查**：
1. 确保 Docker 版本 >= 20.10
2. 检查网络连接（需要下载依赖）
3. 查看构建日志：`docker build --progress=plain .`

### 问题：容器无法启动

**检查**：
1. 查看容器日志：`docker logs <container-name>`
2. 检查端口是否被占用：`netstat -an | grep <port>`
3. 检查环境变量是否正确设置

### 问题：健康检查失败

**检查**：
1. 确保服务已启动：`docker ps`
2. 检查端口映射：`docker port <container-name>`
3. 手动测试健康检查端点

## 生产环境建议

1. **使用固定版本标签**：不要使用 `latest` 标签
   ```bash
   docker build -t mira-gateway-mock:v1.0.0 .
   ```

2. **资源限制**：设置 CPU 和内存限制
   ```yaml
   deploy:
     resources:
       limits:
         cpus: '0.5'
         memory: 128M
   ```

3. **日志管理**：配置日志驱动和轮转
   ```yaml
   logging:
     driver: "json-file"
     options:
       max-size: "10m"
       max-file: "3"
   ```

4. **监控**：集成 Prometheus 或其他监控系统

5. **安全扫描**：定期扫描镜像漏洞
   ```bash
   docker scan mira-gateway-mock:latest
   ```

