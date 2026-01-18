# PowerShell 脚本：查询 IDA Service 中已注册的数据源和资产信息

param(
    [string]$IdaHost = "localhost",
    [int]$IdaPort = 9091
)

$baseUrl = "http://${IdaHost}:${IdaPort}/v1"
$requestId = "query_$(Get-Date -Format 'yyyyMMddHHmmss')"

Write-Host "========== 查询 IDA Service 数据源和资产信息 ==========" -ForegroundColor Green
Write-Host "IDA Service 地址: $baseUrl" -ForegroundColor Cyan
Write-Host ""

# 1. 查询资产列表
Write-Host "1. 查询资产列表..." -ForegroundColor Yellow
Write-Host "   URL: ${baseUrl}/GetPrivateAssetList" -ForegroundColor Gray
Write-Host ""

$assetListBody = @{
    baseRequest = @{
        requestId = $requestId
    }
    pageNumber = 1
    pageSize = 100
    filters = @()
} | ConvertTo-Json -Depth 10

try {
    $assetListResponse = Invoke-RestMethod -Uri "${baseUrl}/GetPrivateAssetList" `
        -Method Post `
        -ContentType "application/json" `
        -Body $assetListBody
    
    Write-Host "资产列表响应:" -ForegroundColor Cyan
    $assetListResponse | ConvertTo-Json -Depth 10
    Write-Host ""
    
    # 提取数据源ID
    $dataSourceIds = $assetListResponse.data.list | 
        Where-Object { $_.dataInfo -ne $null } | 
        ForEach-Object { $_.dataInfo.dataSourceId } | 
        Sort-Object -Unique
    
    if ($dataSourceIds) {
        Write-Host "2. 查询数据源信息..." -ForegroundColor Yellow
        Write-Host ""
        
        foreach ($dsId in $dataSourceIds) {
            if ($dsId -ne $null -and $dsId -ne "") {
                Write-Host "   数据源 ID: $dsId" -ForegroundColor Cyan
                
                $dsBody = @{
                    requestId = "${requestId}_ds_${dsId}"
                    dbConnId = $dsId
                } | ConvertTo-Json
                
                try {
                    $dsResponse = Invoke-RestMethod -Uri "${baseUrl}/GetPrivateDBConnInfo" `
                        -Method Post `
                        -ContentType "application/json" `
                        -Body $dsBody
                    
                    $dsResponse | ConvertTo-Json -Depth 10
                    Write-Host ""
                } catch {
                    Write-Host "   查询数据源 $dsId 失败: $_" -ForegroundColor Red
                }
            }
        }
    } else {
        Write-Host "   未找到数据源ID" -ForegroundColor Yellow
    }
} catch {
    Write-Host "查询失败: $_" -ForegroundColor Red
    Write-Host "请检查 IDA Service 是否运行在 ${IdaHost}:${IdaPort}" -ForegroundColor Yellow
}

Write-Host "========== 查询完成 ==========" -ForegroundColor Green

