# 获取 MySQL 8.0 Docker 镜像

## 方法一：直接拉取官方镜像（推荐）

```bash
docker pull mysql:8.0
```

## 方法二：使用国内镜像源（如果官方源较慢）

### 配置 Docker 镜像加速器

编辑或创建 `/etc/docker/daemon.json`（Linux）或 Docker Desktop 设置（Windows/Mac）：

```json
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com",
    "https://mirror.baidubce.com"
  ]
}
```

然后重启 Docker 服务，再执行：

```bash
docker pull mysql:8.0
```

## 方法三：指定完整标签

```bash
# 拉取最新 8.0 版本
docker pull mysql:8.0

# 拉取特定版本
docker pull mysql:8.0.35

# 拉取最新稳定版
docker pull mysql:latest
```

## 方法四：验证镜像

拉取完成后，验证镜像是否存在：

```bash
# 查看本地 MySQL 镜像
docker images | grep mysql

# 查看镜像详细信息
docker inspect mysql:8.0
```

## 方法五：使用 docker-compose 自动拉取

如果使用 docker-compose，首次启动时会自动拉取镜像：

```bash
cd datasource-hub
docker-compose -f docker-compose.mysql.yml up -d
```

## 常见问题

### 1. 网络超时

如果遇到网络超时，可以：
- 配置 Docker 镜像加速器（见方法二）
- 使用代理
- 重试几次

### 2. 查看拉取进度

```bash
docker pull mysql:8.0
```

拉取过程会显示进度条。

### 3. 检查镜像大小

MySQL 8.0 镜像大小约为 500-600 MB。

```bash
docker images mysql:8.0
```

## 使用镜像

拉取完成后，可以使用以下方式启动：

```bash
# 使用 docker run
docker run -d \
  --name mysql-test \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=test_db \
  -p 3306:3306 \
  mysql:8.0

# 或使用 docker-compose
docker-compose -f docker-compose.mysql.yml up -d
```

