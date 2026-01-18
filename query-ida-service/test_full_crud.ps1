# PowerShell 脚本：完整的 CRUD 测试
# 使用 data-integrate-test 工具测试 ida-access-service-mock 的完整 CRUD 功能

$ErrorActionPreference = "Stop"

Write-Host "========== IDA Access Service Mock 完整 CRUD 测试 ==========" -ForegroundColor Green
Write-Host ""

# 检查服务状态
Write-Host "1. 检查服务状态..." -ForegroundColor Yellow
$containerStatus = docker ps --filter "name=ida-access-service-mock" --format "{{.Status}}"
if ($containerStatus) {
    Write-Host "   ✅ 服务运行中: $containerStatus" -ForegroundColor Green
} else {
    Write-Host "   ❌ 服务未运行，请先启动服务" -ForegroundColor Red
    exit 1
}
Write-Host ""

# 检查查询工具
Write-Host "2. 检查查询工具..." -ForegroundColor Yellow
$queryTool = "d:\freeland20260106\freeland\data-integrate-test\bin\query-ida.exe"
$configPath = "d:\freeland20260106\freeland\data-integrate-test\config\test_config.yaml"

if (-not (Test-Path $queryTool)) {
    Write-Host "   正在编译 query-ida 工具..." -ForegroundColor Cyan
    Push-Location "d:\freeland20260106\freeland\data-integrate-test"
    go build -o bin\query-ida.exe .\cmd\query_ida
    if ($LASTEXITCODE -ne 0) {
        Write-Host "   ❌ 编译失败" -ForegroundColor Red
        Pop-Location
        exit 1
    }
    Pop-Location
    Write-Host "   ✅ 工具编译成功" -ForegroundColor Green
} else {
    Write-Host "   ✅ 工具已存在" -ForegroundColor Green
}
Write-Host ""

# 3. 查询当前资产列表（Read）
Write-Host "3. 查询资产列表 (Read)..." -ForegroundColor Yellow
& $queryTool -config=$configPath -type=asset
Write-Host ""

# 4. 查询数据源（Read）
Write-Host "4. 查询数据源 (Read) - 示例ID=1000..." -ForegroundColor Yellow
& $queryTool -config=$configPath -type=datasource -ds-id=1000
Write-Host ""

Write-Host "========== 测试说明 ==========" -ForegroundColor Cyan
Write-Host ""
Write-Host "当前测试了 Read 操作（查询功能）。"
Write-Host ""
Write-Host "要测试完整的 CRUD（包括 Create），可以使用以下方式："
Write-Host ""
Write-Host "1. 使用 data-integrate-test 的测试执行器自动创建数据源和资产："
Write-Host "   cd data-integrate-test"
Write-Host "   go build -o bin/data-integrate-test.exe ."
Write-Host "   .\bin\data-integrate-test.exe -template=templates\mysql\mysql_1k_8fields.yaml"
Write-Host ""
Write-Host "2. 然后使用查询工具查看创建的数据："
Write-Host "   .\bin\query-ida.exe -config=config\test_config.yaml -type=all"
Write-Host ""
Write-Host "注意：ida-access-service-mock 只提供 gRPC 服务，不支持 HTTP 接口。"
Write-Host "所有操作都需要通过 gRPC 客户端进行。"
Write-Host ""

