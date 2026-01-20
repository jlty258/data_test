#!/bin/bash
# 在 Docker 容器中运行测试的脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

echo "=========================================="
echo "在 Docker 容器中运行测试"
echo "=========================================="
echo ""

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ 错误: Docker 未运行，请先启动 Docker"
    exit 1
fi

# 构建测试镜像
echo "📦 构建测试镜像..."
docker build -f Dockerfile.test -t data-integrate-test:test .

if [ $? -ne 0 ]; then
    echo "❌ 构建测试镜像失败"
    exit 1
fi

echo ""
echo "✅ 测试镜像构建成功"
echo ""

# 运行测试
echo "🧪 运行测试..."
echo ""

# 创建测试结果目录
mkdir -p test-results

# 运行测试并保存输出
docker run --rm \
    -v "$PROJECT_DIR:/build" \
    -v "$PROJECT_DIR/test-results:/build/test-results" \
    data-integrate-test:test \
    make test 2>&1 | tee test-results/test-output.log

TEST_EXIT_CODE=${PIPESTATUS[0]}

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ 所有测试通过！"
else
    echo "❌ 测试失败，退出码: $TEST_EXIT_CODE"
fi

echo ""
echo "测试输出已保存到: test-results/test-output.log"
echo ""

exit $TEST_EXIT_CODE
