# PowerShell 脚本：启动 ida-access-service-mock 容器

Write-Host "启动 ida-access-service-mock 容器..." -ForegroundColor Green

# 使用 docker-compose 启动
docker-compose -f docker-compose.all.yml up -d ida-access-service-mock

# 等待服务启动
Write-Host "等待服务启动..." -ForegroundColor Yellow
Start-Sleep -Seconds 3

# 检查服务状态
docker ps | Select-String "ida-access-service-mock"

Write-Host "ida-access-service-mock 容器已启动" -ForegroundColor Green
Write-Host "服务地址: localhost:9091" -ForegroundColor Cyan
