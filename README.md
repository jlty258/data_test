# Freeland Data Service Integration Test

数据服务集成测试框架和相关 Mock 服务。

## 项目结构

- **data-integrate-test**: 数据服务集成测试框架
- **ida-access-service-mock**: IDA Access Service Mock 服务
- **mira-gateway-mock**: Mira Gateway Mock 服务
- **mira-data-service-server**: 数据服务主服务（独立仓库）

## 快速开始

### 使用 Docker Compose 启动所有服务

```bash
# 启动所有 Mock 服务
docker-compose -f docker-compose.all.yml up -d

# 运行测试
docker-compose -f docker-compose.all.yml --profile test run --rm data-integrate-test \
  -template=templates/mysql_1m_8fields.yaml
```

### 本地开发

```bash
# 运行测试
cd data-integrate-test
go run main.go -template=templates/mysql_1m_8fields.yaml
```

## 详细文档

- [Docker 部署指南](DOCKER_DEPLOYMENT.md)
- [Mock 服务说明](MOCK_SERVICES_README.md)
- [Data Integrate Test 文档](data-integrate-test/README.md)

## License

[添加许可证信息]
