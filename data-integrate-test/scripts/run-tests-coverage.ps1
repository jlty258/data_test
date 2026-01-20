# PowerShell 脚本：在 Docker 容器中运行测试并生成覆盖率报告

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir

Set-Location $ProjectDir

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "在 Docker 容器中运行测试并生成覆盖率报告" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# 检查 Docker 是否运行
try {
    docker info | Out-Null
} catch {
    Write-Host "❌ 错误: Docker 未运行，请先启动 Docker" -ForegroundColor Red
    exit 1
}

# 构建测试镜像
Write-Host "📦 构建测试镜像..." -ForegroundColor Yellow
docker build -f Dockerfile.test -t data-integrate-test:test .

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ 构建测试镜像失败" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅ 测试镜像构建成功" -ForegroundColor Green
Write-Host ""

# 运行测试并生成覆盖率报告
Write-Host "🧪 运行测试并生成覆盖率报告..." -ForegroundColor Yellow
Write-Host ""

# 创建测试结果目录
New-Item -ItemType Directory -Force -Path "test-results" | Out-Null

# 运行测试并生成覆盖率报告
$testOutput = docker run --rm `
    -v "${ProjectDir}:/build" `
    -v "${ProjectDir}/test-results:/build/test-results" `
    data-integrate-test:test `
    make test-coverage 2>&1

$testOutput | Tee-Object -FilePath "test-results/test-coverage-output.log"

$testExitCode = $LASTEXITCODE

Write-Host ""
if ($testExitCode -eq 0) {
    Write-Host "✅ 所有测试通过！" -ForegroundColor Green
    Write-Host ""
    
    # 检查覆盖率文件是否生成
    if (Test-Path "coverage.html") {
        Write-Host "📊 覆盖率报告已生成:" -ForegroundColor Cyan
        Write-Host "   - coverage.html (HTML 报告)" -ForegroundColor Cyan
        Write-Host "   - coverage.out (原始数据)" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "💡 提示: 在浏览器中打开 coverage.html 查看详细覆盖率报告" -ForegroundColor Yellow
    } else {
        Write-Host "⚠️  警告: 覆盖率报告文件未生成" -ForegroundColor Yellow
    }
    
    if (Test-Path "test-results/coverage.html") {
        Write-Host "   - test-results/coverage.html (已复制到测试结果目录)" -ForegroundColor Cyan
    }
} else {
    Write-Host "❌ 测试失败，退出码: $testExitCode" -ForegroundColor Red
}

Write-Host ""
Write-Host "测试输出已保存到: test-results/test-coverage-output.log" -ForegroundColor Cyan
Write-Host ""

exit $testExitCode
