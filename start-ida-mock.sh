#!/bin/bash

# 启动 ida-access-service-mock 容器
echo "启动 ida-access-service-mock 容器..."

# 使用 docker-compose 启动
docker-compose -f docker-compose.all.yml up -d ida-access-service-mock

# 等待服务启动
echo "等待服务启动..."
sleep 3

# 检查服务状态
docker ps | grep ida-access-service-mock

echo "ida-access-service-mock 容器已启动"
echo "服务地址: localhost:9091"
