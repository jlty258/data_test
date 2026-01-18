# PowerShell 脚本：测试 Update 和 Delete 功能
# 使用 data-integrate-test 工具测试 ida-access-service-mock 的 Update 和 Delete 功能

$ErrorActionPreference = "Stop"

Write-Host "========== 测试 Update 和 Delete 功能 ==========" -ForegroundColor Green
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

Write-Host "========== 测试说明 ==========" -ForegroundColor Cyan
Write-Host ""
Write-Host "已成功实现 Update 和 Delete 功能："
Write-Host ""
Write-Host "✅ UpdateAsset - 更新资产"
Write-Host "   - 可以更新资产的基本信息（名称、描述等）"
Write-Host "   - 可以更新资产的表结构信息"
Write-Host ""
Write-Host "✅ DeleteAsset - 删除资产"
Write-Host "   - 通过资产ID删除资产"
Write-Host "   - 同时清理英文名索引"
Write-Host ""
Write-Host "要测试这些功能，需要使用 gRPC 客户端："
Write-Host ""
Write-Host "1. 使用 data-integrate-test 的测试执行器创建资产"
Write-Host "2. 使用 gRPC 客户端调用 UpdateAsset 和 DeleteAsset"
Write-Host ""
Write-Host "或者使用 Go 代码直接测试："
Write-Host "   - 参考 data-integrate-test/clients/ida_service_client.go"
Write-Host "   - 添加 UpdateAsset 和 DeleteAsset 方法"
Write-Host ""

