# IDA Access Service Mock 状态检查

## 当前状态

### 容器状态
- **状态**: ✅ 运行中 (Running)
- **运行时长**: 3 天
- **容器ID**: 0222ea457cf8
- **镜像**: ida-access-service-mock:latest
- **端口映射**: 0.0.0.0:9091->9091/tcp

### 服务状态
- **端口监听**: ✅ 9091 端口正常监听
- **进程状态**: ✅ 服务进程正常运行
- **启动时间**: 2026-01-15 18:38:22

### 服务日志
从日志可以看到：
- ✅ 服务已成功启动在端口 9091
- ✅ 已收到创建数据源请求（CreateDataSource）
- ✅ 已收到创建资产请求（CreateAsset）
- ⚠️ 日志中没有显示存储类型信息（可能是旧版本镜像）

### 功能验证
- **gRPC 服务**: ✅ 运行中（端口 9091）
- **反射 API**: ❌ 未启用（grpcurl 无法列出服务，但不影响功能）
- **HTTP 接口**: ❌ 未实现（ida-access-service-mock 只提供 gRPC 服务）

## 注意事项

1. **镜像版本**: 容器使用的是 3 天前的镜像，可能不包含最新的存储层实现和 `dataProductType` 字段
2. **需要重新构建**: 如果要使用最新的功能（存储层、dataProductType），需要重新构建并重启容器

## 建议操作

### 重新构建并重启（使用最新代码）
```bash
# 重新构建镜像
docker-compose -f docker-compose.all.yml build ida-access-service-mock

# 重启服务
docker-compose -f docker-compose.all.yml up -d ida-access-service-mock

# 查看日志
docker logs -f ida-access-service-mock
```

### 测试服务
由于服务不支持反射 API，需要使用具体的接口进行测试：
- 使用 gRPC 客户端（如 data-integrate-test 中的客户端）
- 或使用 grpcurl 直接调用具体接口

