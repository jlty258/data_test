#!/bin/bash
# 运行数据集成测试的便捷脚本

set -e

echo "========== Data Integrate Test 运行脚本 =========="
echo ""

# 检查 docker-compose 文件
if [ ! -f "docker-compose.debug.yml" ]; then
    echo "错误: 找不到 docker-compose.debug.yml"
    exit 1
fi

# 检查测试模板
if [ ! -f "data-integrate-test/templates/mysql_1k_8fields.yaml" ]; then
    echo "错误: 找不到测试模板 mysql_1k_8fields.yaml"
    exit 1
fi

echo "1. 构建测试镜像..."
cd data-integrate-test
docker build -t data-integrate-test:latest .
cd ..

echo ""
echo "2. 启动依赖服务..."
docker-compose -f docker-compose.debug.yml up -d mira-gateway-mock ida-access-service-mock minio doris-fe doris-be

echo ""
echo "等待服务就绪..."
sleep 10

echo ""
echo "3. 运行测试..."
docker-compose -f docker-compose.debug.yml --profile test run --rm data-integrate-test \
    /home/workspace/bin/data-integrate-test \
    -template templates/mysql_1k_8fields.yaml \
    -config config/test_config.docker.yaml

echo ""
echo "========== 测试完成 =========="
echo ""
echo "提示: 要清理测试环境，运行:"
echo "  docker-compose -f docker-compose.debug.yml down"

