#!/bin/bash

# 根据 data-integrate-test/config/test_config.yaml 配置启动 MySQL
# 配置信息：
#   - 用户名: root
#   - 密码: password
#   - 数据库: test_db
#   - 端口: 3306

echo "启动 MySQL 数据库..."
echo "配置信息："
echo "  - 用户名: root"
echo "  - 密码: password"
echo "  - 数据库: test_db"
echo "  - 端口: 3306"
echo ""

docker-compose -f docker-compose.mysql.yml up -d

echo ""
echo "等待 MySQL 启动..."
sleep 5

# 检查容器状态
if docker ps | grep -q mysql-test; then
    echo "✅ MySQL 启动成功！"
    echo ""
    echo "连接信息："
    echo "  - 主机: localhost"
    echo "  - 端口: 3306"
    echo "  - 用户名: root"
    echo "  - 密码: password"
    echo "  - 数据库: test_db"
    echo ""
    echo "测试连接："
    echo "  mysql -h localhost -P 3306 -u root -ppassword test_db"
    echo ""
    echo "查看日志："
    echo "  docker-compose -f docker-compose.mysql.yml logs -f"
    echo ""
    echo "停止服务："
    echo "  docker-compose -f docker-compose.mysql.yml down"
else
    echo "❌ MySQL 启动失败，请查看日志："
    docker-compose -f docker-compose.mysql.yml logs
fi

