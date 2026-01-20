# PowerShell 脚本：在 Docker 容器中运行测试

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir

Set-Location $ProjectDir

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "在 Docker 容器中运行测试" -ForegroundColor Cyan
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

# 运行测试
Write-Host "🧪 运行测试..." -ForegroundColor Yellow
Write-Host ""

# 创建测试结果目录
New-Item -ItemType Directory -Force -Path "test-results" | Out-Null

# 运行测试并保存输出
$testOutput = docker run --rm `
    -v "${ProjectDir}:/build" `
    -v "${ProjectDir}/test-results:/build/test-results" `
    data-integrate-test:test `
    make test 2>&1

$testOutput | Tee-Object -FilePath "test-results/test-output.log"

$testExitCode = $LASTEXITCODE

Write-Host ""
if ($testExitCode -eq 0) {
    Write-Host "✅ 所有测试通过！" -ForegroundColor Green
} else {
    Write-Host "❌ 测试失败，退出码: $testExitCode" -ForegroundColor Red
}

Write-Host ""
Write-Host "测试输出已保存到: test-results/test-output.log" -ForegroundColor Cyan
Write-Host ""

exit $testExitCode
