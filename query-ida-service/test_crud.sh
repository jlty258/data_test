#!/bin/bash

# CRUD 测试脚本：测试 ida-access-service-mock 的完整 CRUD 功能

IDA_HOST=${1:-localhost}
IDA_PORT=${2:-9091}
BASE_URL="http://${IDA_HOST}:${IDA_PORT}/v1"
REQUEST_ID="test_crud_$(date +%s)"

echo "========== IDA Access Service Mock CRUD 测试 =========="
echo "IDA Service 地址: ${BASE_URL}"
echo "注意: ida-access-service-mock 只提供 gRPC 服务，不支持 HTTP"
echo "此脚本演示如何使用 gRPC 客户端进行测试"
echo ""

# 由于 ida-access-service-mock 只提供 gRPC 服务，我们需要使用 gRPC 客户端
# 这里提供测试步骤说明

echo "========== 测试步骤 =========="
echo ""
echo "1. 创建数据源 (CreateDataSource)"
echo "   使用 gRPC 客户端调用 CreateDataSource 接口"
echo ""
echo "2. 查询数据源 (GetPrivateDBConnInfo)"
echo "   使用创建的数据源ID查询数据源信息"
echo ""
echo "3. 创建资产 (CreateAsset)"
echo "   使用创建的数据源ID创建资产"
echo ""
echo "4. 查询资产列表 (GetPrivateAssetList)"
echo "   查询所有已创建的资产"
echo ""
echo "5. 查询资产详情 (GetPrivateAssetInfo)"
echo "   使用资产ID查询资产详情"
echo ""
echo "6. 通过资产英文名查询 (GetPrivateAssetInfoByEnName)"
echo "   使用资产英文名查询资产详情"
echo ""

echo "========== 使用 data-integrate-test 工具测试 =========="
echo ""
echo "可以使用 data-integrate-test/cmd/query_ida 工具进行测试："
echo ""
echo "cd data-integrate-test"
echo "go build -o bin/query-ida ./cmd/query_ida"
echo "./bin/query-ida -type=all"
echo ""

echo "========== 使用 grpcurl 测试（需要启用反射） =========="
echo ""
echo "注意: 当前服务未启用 gRPC 反射，无法使用 grpcurl list"
echo "但可以直接调用接口："
echo ""
echo "# 创建数据源示例（需要构建正确的请求）"
echo "grpcurl -plaintext -d '{\"baseRequest\":{\"requestId\":\"test\"},\"name\":\"test_ds\",\"host\":\"localhost\",\"port\":3306,\"dbType\":1}' \\"
echo "  localhost:9091 mira.MiraIdaAccess/CreateDataSource"
echo ""

echo "========== 建议使用 Go 客户端测试 =========="
echo ""
echo "推荐使用 data-integrate-test 项目中的客户端进行完整测试"
echo ""

