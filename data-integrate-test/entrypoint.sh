#!/bin/bash
# 多工具镜像入口点脚本
# 支持直接执行工具或显示帮助信息

set -e

# 如果没有参数，显示帮助信息
if [ $# -eq 0 ]; then
    echo "=========================================="
    echo "Data Integrate Test Tools"
    echo "=========================================="
    echo ""
    echo "可用工具："
    echo "  /app/data-integrate-test    - 执行测试模板"
    echo "  /app/manage-ida             - IDA 服务管理"
    echo "  /app/export-table           - 导出单个表"
    echo "  /app/import-table           - 导入单个表"
    echo "  /app/export-snapshots       - 批量导出快照"
    echo "  /app/import-snapshots       - 批量导入快照"
    echo "  /app/test-clients           - 测试客户端"
    echo ""
    echo "使用示例："
    echo "  docker run --rm <image> /app/export-table -db=mysql -dbname=test -table=users -output=/tmp"
    echo "  docker run --rm <image> /app/manage-ida -action=all"
    echo ""
    echo "或者直接执行工具（如果作为 ENTRYPOINT）："
    echo "  docker run --rm <image> export-table -db=mysql -dbname=test -table=users -output=/tmp"
    exit 0
fi

# 如果第一个参数是工具名（不带路径），尝试查找
if [ "${1#/app/}" = "$1" ] && [ "${1#-}" = "$1" ]; then
    TOOL_PATH="/app/$1"
    if [ -f "$TOOL_PATH" ] && [ -x "$TOOL_PATH" ]; then
        # 工具存在且可执行，执行它
        exec "$TOOL_PATH" "${@:2}"
    else
        # 工具不存在，作为参数传递给默认工具
        exec /app/data-integrate-test "$@"
    fi
else
    # 直接执行传入的命令
    exec "$@"
fi
