#!/bin/bash

# 查询 IDA Service 中已注册的数据源和资产信息

IDA_HOST=${1:-localhost}
IDA_PORT=${2:-9091}
BASE_URL="http://${IDA_HOST}:${IDA_PORT}/v1"
REQUEST_ID="query_$(date +%s)"

echo "========== 查询 IDA Service 数据源和资产信息 =========="
echo "IDA Service 地址: ${BASE_URL}"
echo ""

# 1. 查询资产列表
echo "1. 查询资产列表..."
echo "   URL: ${BASE_URL}/GetPrivateAssetList"
echo ""

ASSET_LIST_RESPONSE=$(curl -s -X POST "${BASE_URL}/GetPrivateAssetList" \
  -H "Content-Type: application/json" \
  -d "{
    \"baseRequest\": {
      \"requestId\": \"${REQUEST_ID}\"
    },
    \"pageNumber\": 1,
    \"pageSize\": 100,
    \"filters\": []
  }")

if [ $? -eq 0 ]; then
  echo "$ASSET_LIST_RESPONSE" | jq '.' 2>/dev/null || echo "$ASSET_LIST_RESPONSE"
  echo ""
  
  # 提取数据源ID
  DS_IDS=$(echo "$ASSET_LIST_RESPONSE" | jq -r '.data.list[].dataInfo.dataSourceId' 2>/dev/null | sort -u)
  
  if [ -n "$DS_IDS" ]; then
    echo "2. 查询数据源信息..."
    echo ""
    
    for DS_ID in $DS_IDS; do
      if [ "$DS_ID" != "null" ] && [ -n "$DS_ID" ]; then
        echo "   数据源 ID: $DS_ID"
        DS_RESPONSE=$(curl -s -X POST "${BASE_URL}/GetPrivateDBConnInfo" \
          -H "Content-Type: application/json" \
          -d "{
            \"requestId\": \"${REQUEST_ID}_ds_${DS_ID}\",
            \"dbConnId\": ${DS_ID}
          }")
        
        echo "$DS_RESPONSE" | jq '.' 2>/dev/null || echo "$DS_RESPONSE"
        echo ""
      fi
    done
  fi
else
  echo "   查询失败，请检查 IDA Service 是否运行"
fi

echo "========== 查询完成 =========="

