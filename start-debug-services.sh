#!/bin/bash

# 启动调试服务脚本
# 用于启动 ida-access-service-mock, mira-gateway-mock, mira-data-service-server

set -e

echo "=========================================="
echo "启动调试服务"
echo "=========================================="

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ 错误: Docker 未运行，请先启动 Docker"
    exit 1
fi

# 检查 docker-compose 是否可用
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ 错误: docker-compose 未安装"
    exit 1
fi

# 使用 docker-compose 或 docker compose
COMPOSE_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker compose"
fi

echo ""
echo "1. 停止现有服务（如果存在）..."
$COMPOSE_CMD -f docker-compose.debug.yml down

echo ""
echo "2. 构建镜像..."
$COMPOSE_CMD -f docker-compose.debug.yml build

echo ""
echo "3. 启动服务..."
$COMPOSE_CMD -f docker-compose.debug.yml up -d

echo ""
echo "4. 等待服务启动..."
sleep 5

echo ""
echo "5. 检查服务状态..."
$COMPOSE_CMD -f docker-compose.debug.yml ps

echo ""
echo "=========================================="
echo "服务启动完成！"
echo "=========================================="
echo ""
echo "服务地址："
echo "  - Mira Gateway Mock:     http://localhost:8080"
echo "  - IDA Access Service:    localhost:9091 (gRPC)"
echo "  - Mira Data Service:     localhost:9090 (gRPC), http://localhost:8080 (HTTP)"
echo ""
echo "查看日志："
echo "  $COMPOSE_CMD -f docker-compose.debug.yml logs -f [服务名]"
echo ""
echo "停止服务："
echo "  $COMPOSE_CMD -f docker-compose.debug.yml down"
echo ""
echo "=========================================="

# 检查服务健康状态
echo ""
echo "检查服务健康状态..."
echo ""

# 检查 Mira Gateway Mock
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ Mira Gateway Mock: 健康"
else
    echo "⚠️  Mira Gateway Mock: 未响应（可能还在启动中）"
fi

# 检查 IDA Access Service Mock (使用 grpcurl 如果可用)
if command -v grpcurl &> /dev/null; then
    if grpcurl -plaintext localhost:9091 list > /dev/null 2>&1; then
        echo "✅ IDA Access Service Mock: 健康"
    else
        echo "⚠️  IDA Access Service Mock: 未响应（可能还在启动中）"
    fi
else
    echo "ℹ️  IDA Access Service Mock: 跳过检查（需要安装 grpcurl）"
fi

# 检查 Mira Data Service Server
if timeout 2 bash -c "echo > /dev/tcp/localhost/9090" 2>/dev/null; then
    echo "✅ Mira Data Service Server: 端口 9090 已监听"
else
    echo "⚠️  Mira Data Service Server: 端口 9090 未监听（可能还在启动中）"
fi

echo ""
echo "提示: 如果服务未就绪，请稍等片刻后再次检查"
echo "      查看日志: $COMPOSE_CMD -f docker-compose.debug.yml logs -f"

