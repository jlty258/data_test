# PowerShell 脚本：根据 data-integrate-test/config/test_config.yaml 配置启动 MySQL
# 配置信息：
#   - 用户名: root
#   - 密码: password
#   - 数据库: test_db
#   - 端口: 3306

Write-Host "启动 MySQL 数据库..." -ForegroundColor Green
Write-Host "配置信息：" -ForegroundColor Yellow
Write-Host "  - 用户名: root"
Write-Host "  - 密码: password"
Write-Host "  - 数据库: test_db"
Write-Host "  - 端口: 3306"
Write-Host ""

docker-compose -f docker-compose.mysql.yml up -d

Write-Host ""
Write-Host "等待 MySQL 启动..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# 检查容器状态
$containerRunning = docker ps | Select-String -Pattern "mysql-test"
if ($containerRunning) {
    Write-Host "✅ MySQL 启动成功！" -ForegroundColor Green
    Write-Host ""
    Write-Host "连接信息：" -ForegroundColor Cyan
    Write-Host "  - 主机: localhost"
    Write-Host "  - 端口: 3306"
    Write-Host "  - 用户名: root"
    Write-Host "  - 密码: password"
    Write-Host "  - 数据库: test_db"
    Write-Host ""
    Write-Host "测试连接：" -ForegroundColor Cyan
    Write-Host "  mysql -h localhost -P 3306 -u root -ppassword test_db"
    Write-Host ""
    Write-Host "查看日志：" -ForegroundColor Cyan
    Write-Host "  docker-compose -f docker-compose.mysql.yml logs -f"
    Write-Host ""
    Write-Host "停止服务：" -ForegroundColor Cyan
    Write-Host "  docker-compose -f docker-compose.mysql.yml down"
} else {
    Write-Host "❌ MySQL 启动失败，请查看日志：" -ForegroundColor Red
    docker-compose -f docker-compose.mysql.yml logs
}

