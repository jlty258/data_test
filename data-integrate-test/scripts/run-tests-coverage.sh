#!/bin/bash
# 在 Docker 容器中运行测试并生成覆盖率报告的脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

echo "=========================================="
echo "在 Docker 容器中运行测试并生成覆盖率报告"
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

# 运行测试并生成覆盖率报告
echo "🧪 运行测试并生成覆盖率报告..."
echo ""

# 创建测试结果目录
mkdir -p test-results

# 运行测试并生成覆盖率报告
docker run --rm \
    -v "$PROJECT_DIR:/build" \
    -v "$PROJECT_DIR/test-results:/build/test-results" \
    data-integrate-test:test \
    make test-coverage 2>&1 | tee test-results/test-coverage-output.log

TEST_EXIT_CODE=${PIPESTATUS[0]}

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ 所有测试通过！"
    echo ""
    
    # 检查覆盖率文件是否生成
    if [ -f "coverage.html" ]; then
        echo "📊 覆盖率报告已生成:"
        echo "   - coverage.html (HTML 报告)"
        echo "   - coverage.out (原始数据)"
        echo ""
        echo "💡 提示: 在浏览器中打开 coverage.html 查看详细覆盖率报告"
    else
        echo "⚠️  警告: 覆盖率报告文件未生成"
    fi
    
    if [ -f "test-results/coverage.html" ]; then
        echo "   - test-results/coverage.html (已复制到测试结果目录)"
    fi
else
    echo "❌ 测试失败，退出码: $TEST_EXIT_CODE"
fi

echo ""
echo "测试输出已保存到: test-results/test-coverage-output.log"
echo ""

exit $TEST_EXIT_CODE
