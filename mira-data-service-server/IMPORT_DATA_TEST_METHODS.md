# ImportData 接口测试方式说明

`ImportData` 是一个 gRPC 接口，用于从外部数据资产导入数据到 Doris 数据库。以下是几种测试方式：

## 测试方式概览

### 方式 1: Go 客户端脚本（推荐）

**文件**: `test_import_data.go`

**优点**:
- 类型安全，直接使用生成的 protobuf 代码
- 易于调试和维护
- 可以访问完整的响应信息

**使用方法**:
```bash
# 在有 Go 环境的机器上
cd /root/freeland/mira-data-service-server
go run test_import_data.go
```

**适用场景**: 
- 本地开发环境有 Go 环境
- 需要详细的响应信息
- 需要集成到自动化测试中

---

### 方式 2: grpcurl 命令行工具

**优点**:
- 不需要编译，直接使用命令行
- 适合快速测试和调试
- 支持反射服务发现

**安装**:
```bash
# 安装 grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
# 或使用 Docker
docker pull fullstorydev/grpcurl
```

**使用方法**:
```bash
# 列出服务（如果支持反射）
grpcurl -plaintext localhost:9090 list

# 调用 ImportData
grpcurl -plaintext -d '{
  "targets": [{
    "external": {
      "assetName": "test_10k_16f_asset",
      "chainInfoId": "",
      "alias": ""
    },
    "targetTableName": "test_10k_16f_imported",
    "dbName": "test_import_db",
    "columns": [],
    "keys": []
  }]
}' localhost:9090 datasource.DataSourceService/ImportData
```

**使用 proto 文件**（如果不支持反射）:
```bash
grpcurl -plaintext -proto proto/data_source.proto \
  -d @request.json localhost:9090 datasource.DataSourceService/ImportData
```

**适用场景**:
- 快速测试和验证
- 调试接口调用
- 手动测试场景

---

### 方式 3: Python gRPC 客户端

**优点**:
- Python 环境常见，易于使用
- 适合脚本化测试
- 可以集成到 Python 测试框架

**安装依赖**:
```bash
pip install grpcio grpcio-tools
```

**生成 Python 代码**:
```bash
python -m grpc_tools.protoc -I./proto \
  --python_out=. --grpc_python_out=. \
  proto/data_source.proto
```

**示例代码** (test_import_data.py):
```python
import grpc
import data_source_pb2
import data_source_pb2_grpc

def test_import_data():
    # 连接服务器
    channel = grpc.insecure_channel('localhost:9090')
    stub = data_source_pb2_grpc.DataSourceServiceStub(channel)
    
    # 创建请求
    request = data_source_pb2.ImportDataRequest(
        targets=[data_source_pb2.ImportTarget(
            external=data_source_pb2.ExternalDataSource(
                assetName="test_10k_16f_asset",
                chainInfoId="",
                alias=""
            ),
            targetTableName="test_10k_16f_imported",
            dbName="test_import_db",
            columns=[],
            keys=[]
        )]
    )
    
    # 调用接口
    response = stub.ImportData(request)
    
    # 打印结果
    print(f"Success: {response.success}")
    print(f"Message: {response.message}")
    for result in response.results:
        print(f"Result: {result}")

if __name__ == "__main__":
    test_import_data()
```

**适用场景**:
- Python 开发环境
- Python 自动化测试
- 脚本化测试场景

---

### 方式 4: 在服务代码中直接调用（集成测试）

**优点**:
- 不依赖网络连接
- 可以测试内部逻辑
- 适合单元测试和集成测试

**示例代码** (server_test.go):
```go
func TestImportData(t *testing.T) {
    ctx := context.Background()
    server := &Server{}  // 初始化服务器实例
    
    request := &pb.ImportDataRequest{
        Targets: []*pb.ImportTarget{
            {
                External: &pb.ExternalDataSource{
                    AssetName:   "test_10k_16f_asset",
                    ChainInfoId: "",
                    Alias:       "",
                },
                TargetTableName: "test_10k_16f_imported",
                DbName:          "test_import_db",
                Columns:         []string{},
                Keys:            []*pb.TableKey{},
            },
        },
    }
    
    response, err := server.ImportData(ctx, request)
    assert.NoError(t, err)
    assert.True(t, response.Success)
}
```

**适用场景**:
- 单元测试
- 集成测试
- CI/CD 自动化测试

---

### 方式 5: 使用 Docker 容器内运行 Go 脚本

**优点**:
- 可以在没有本地 Go 环境的情况下运行
- 环境隔离

**方法 A: 临时容器运行**
```bash
# 复制脚本和依赖到容器
docker cp test_import_data.go mira-data-service-server:/tmp/
docker cp go.mod mira-data-service-server:/tmp/
docker cp go.sum mira-data-service-server:/tmp/

# 在容器内运行（如果容器有 Go 环境）
docker exec -w /tmp mira-data-service-server go run test_import_data.go
```

**方法 B: 构建包含 Go 的临时镜像**
```bash
# 使用官方 Go 镜像
docker run --rm --network host \
  -v /root/freeland/mira-data-service-server:/workspace \
  -w /workspace golang:1.23 go run test_import_data.go
```

**适用场景**:
- 容器化环境
- 没有本地 Go 环境
- 临时测试场景

---

### 方式 6: 使用 gRPC 客户端库（其他语言）

**其他语言支持**:
- **Java**: 使用 grpc-java
- **Node.js**: 使用 @grpc/grpc-js
- **C#**: 使用 Grpc.Net.Client
- **Ruby**: 使用 grpc gem

---

## 当前环境推荐方式

根据当前环境分析：

1. ✅ **方式 1 (Go 脚本)**: 已有脚本 `test_import_data.go`，需要 Go 环境
2. ❌ **方式 2 (grpcurl)**: 未安装，需要安装
3. ❌ **方式 3 (Python)**: Python grpc 模块未安装
4. ✅ **方式 4 (集成测试)**: 可以编写，适合持续测试
5. ✅ **方式 5 (Docker)**: 可以使用，但需要临时 Go 镜像

**最快速的方法**: 安装 grpcurl 或使用 Docker Go 镜像运行脚本

---

## 请求参数说明

```json
{
  "targets": [
    {
      "external": {
        "assetName": "test_10k_16f_asset",  // 资产名称（必填）
        "chainInfoId": "",                   // 链信息ID（可选）
        "alias": ""                          // 别名（可选）
      },
      "targetTableName": "test_10k_16f_imported",  // 目标表名（必填）
      "dbName": "test_import_db",                    // 数据库名（必填）
      "columns": [],                                // 导入列（空数组表示全部）
      "keys": []                                    // 表键信息（可选）
    }
  ]
}
```

## 响应结构说明

```json
{
  "success": true,
  "message": "Import completed",
  "results": [
    {
      "success": true,
      "sourceTableName": "test_10k_16f_asset",
      "targetTableName": "test_10k_16f_imported",
      "sourceDatabase": "test_10k_16f_asset",
      "targetDatabase": "test_import_db",
      "affectedRows": 10000,
      "errorMessage": ""
    }
  ]
}
```
