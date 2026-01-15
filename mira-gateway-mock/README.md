# Mira Gateway Mock

Mira Gateway 服务的 Mock 实现，用于开发和测试。

## 功能

实现了以下 HTTP API 端点：

- `POST /v1/GetPrivateDBConnInfo` - 获取数据库连接信息
- `POST /v1/GetPrivateAssetInfoByEnName` - 通过资产英文名称获取资产详情
- `POST /v1/GetResultStorageConfig` - 获取结果存储配置
- `POST /v1/mira/async/notify` - 推送作业结果
- `GET /health` - 健康检查

## 本地运行

```bash
# 安装依赖
go mod download

# 运行服务（默认端口8080）
go run main.go

# 或指定端口
PORT=8081 go run main.go
```

## Docker 部署

### 构建镜像

```bash
docker build -t mira-gateway-mock:latest .
```

### 运行容器

```bash
docker run -d \
  --name mira-gateway-mock \
  -p 8080:8080 \
  -e PORT=8080 \
  mira-gateway-mock:latest
```

### 使用 Docker Compose

```bash
docker-compose up -d
```

## 环境变量

- `PORT`: 服务监听端口（默认: 8080）

## 使用

在 data-service 中设置环境变量：

```bash
export MIRA_GATEWAY_HOST=localhost
export MIRA_GATEWAY_HOST_PORT=8080
```

## Mock 数据说明

所有接口返回的都是 Mock 数据，可以根据需要修改 `main.go` 中的响应数据。

## 健康检查

```bash
curl http://localhost:8080/health
```
