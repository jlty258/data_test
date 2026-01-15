#!/bin/bash

# Data Integrate Test 运行脚本
# 使用方法: ./run-test.sh <template_name>
# 例如: ./run-test.sh mysql_1m_8fields

if [ -z "$1" ]; then
    echo "用法: ./run-test.sh <template_name>"
    echo "例如: ./run-test.sh mysql_1m_8fields"
    echo ""
    echo "可用的模板:"
    ls -1 data-integrate-test/templates/*.yaml 2>/dev/null | sed 's/.*\///;s/\.yaml$//' || echo "  未找到模板文件"
    exit 1
fi

TEMPLATE_NAME=$1
TEMPLATE_PATH="templates/${TEMPLATE_NAME}.yaml"

echo "运行测试: ${TEMPLATE_NAME}"
echo "模板路径: ${TEMPLATE_PATH}"
echo ""

docker-compose --profile test run --rm data-integrate-test \
    -template=${TEMPLATE_PATH} \
    "$@"

