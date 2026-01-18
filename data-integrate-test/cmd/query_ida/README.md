# IDA Service 查询工具

这个工具用于查询 ida-service 中注册的数据源和资产。

## 使用方法

### 编译

```bash
cd data-integrate-test
go build -o bin/query-ida ./cmd/query_ida
```

### 运行

#### 1. 查询所有（资产列表和关联的数据源）

```bash
./bin/query-ida -type=all
```

或者指定分页参数：

```bash
./bin/query-ida -type=all -page=1 -size=20
```

#### 2. 查询数据源

查询指定ID的数据源：

```bash
./bin/query-ida -type=datasource -ds-id=1000
```

#### 3. 查询资产

查询资产列表：

```bash
./bin/query-ida -type=asset
```

查询指定ID的资产详情：

```bash
./bin/query-ida -type=asset -asset-id=2000
```

### 参数说明

- `-config`: 配置文件路径（默认: `../../config/test_config.yaml`）
- `-type`: 查询类型
  - `all`: 查询所有（资产列表和关联的数据源）
  - `datasource`: 查询数据源
  - `asset`: 查询资产
- `-ds-id`: 数据源ID（查询单个数据源时使用）
- `-asset-id`: 资产ID（查询单个资产时使用）
- `-page`: 页码（查询资产列表时使用，默认: 1）
- `-size`: 每页条数（查询资产列表时使用，默认: 10）

## 配置

工具使用 `config/test_config.yaml` 配置文件中的 IDA 服务配置：

```yaml
mock_services:
  ida:
    host: "localhost"
    port: 9091
```

## 注意事项

1. 当前实现使用的是简化的请求/响应结构，返回的是 Mock 数据
2. 如果需要调用真实的 gRPC 服务，需要：
   - 编译 proto 文件生成 Go 代码
   - 更新 `ida_service_client.go` 使用生成的 proto 类型
3. 确保 IDA Service 服务正在运行

