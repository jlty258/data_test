# PowerShell 脚本：测试 IDA Access Service Mock 的 CRUD 功能
# 注意：由于服务只提供 gRPC 接口，此脚本使用 data-integrate-test 工具进行测试

param(
    [string]$IdaHost = "localhost",
    [int]$IdaPort = 9091
)

Write-Host "========== IDA Access Service Mock CRUD 测试 ==========" -ForegroundColor Green
Write-Host "IDA Service 地址: ${IdaHost}:${IdaPort}" -ForegroundColor Cyan
Write-Host ""

# 检查 data-integrate-test 工具是否存在
$queryTool = "d:\freeland20260106\freeland\data-integrate-test\bin\query-ida.exe"
if (-not (Test-Path $queryTool)) {
    Write-Host "正在编译 query-ida 工具..." -ForegroundColor Yellow
    Push-Location "d:\freeland20260106\freeland\data-integrate-test"
    go build -o bin\query-ida.exe .\cmd\query_ida
    if ($LASTEXITCODE -ne 0) {
        Write-Host "编译失败，请检查错误信息" -ForegroundColor Red
        exit 1
    }
    Pop-Location
}

Write-Host "========== 测试步骤 ==========" -ForegroundColor Yellow
Write-Host ""

Write-Host "1. 查询资产列表..." -ForegroundColor Cyan
& $queryTool -config="d:\freeland20260106\freeland\data-integrate-test\config\test_config.yaml" -type=asset
Write-Host ""

Write-Host "2. 查询数据源（示例ID=1000）..." -ForegroundColor Cyan
& $queryTool -config="d:\freeland20260106\freeland\data-integrate-test\config\test_config.yaml" -type=datasource -ds-id=1000
Write-Host ""

Write-Host "========== 测试说明 ==========" -ForegroundColor Yellow
Write-Host ""
Write-Host "由于 ida-access-service-mock 只提供 gRPC 服务，无法直接使用 HTTP 脚本测试。"
Write-Host "建议使用以下方式测试 CRUD 功能："
Write-Host ""
Write-Host "1. 使用 data-integrate-test 工具："
Write-Host "   cd data-integrate-test"
Write-Host "   go build -o bin/query-ida ./cmd/query_ida"
Write-Host "   ./bin/query-ida -config=config/test_config.yaml -type=all"
Write-Host ""
Write-Host "2. 使用 data-integrate-test 的测试执行器自动创建数据源和资产："
Write-Host "   ./bin/data-integrate-test -template=templates/mysql_1k_8fields.yaml"
Write-Host "   这会自动创建数据源和资产，然后可以使用查询工具查看"
Write-Host ""

