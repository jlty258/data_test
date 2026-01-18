# DataSource Hub

数据源中心，用于通过 Docker Compose 快速启动和管理多个数据库实例，包括 MySQL、KingBase、VastBase、GBase 等。

## 功能特性

- 🚀 一键启动多个数据库服务
- 🔧 支持环境变量配置
- 💾 数据持久化存储
- 🏥 健康检查机制
- 🔄 自动重启策略
- 📝 初始化脚本支持

## 支持的数据库

| 数据库 | 版本 | 默认端口 | 镜像来源 |
|--------|------|----------|----------|
| MySQL | 8.0 | 3306 | 官方镜像 |
| KingBase | V8R6 | 54321 | huzhihui/kingbase |
| VastBase | 2.2.15 | 5432 | thankwhite/vastbase_g100 |
| GBase | 8.8 | 19088 | liaosnet/gbase8s |

## 快速开始

### 1. 环境准备

确保已安装：
- Docker (>= 20.10)
- Docker Compose (>= 2.0)

### 2. 启动 MySQL（匹配 test_config.yaml 配置）

**方式一：使用专用配置文件（推荐）**

根据 `data-integrate-test/config/test_config.yaml` 的配置启动 MySQL：

```bash
# Linux/Mac
./start-mysql.sh

# Windows PowerShell
.\start-mysql.ps1

# 或直接使用 docker-compose
docker-compose -f docker-compose.mysql.yml up -d
```

**配置信息**（与 test_config.yaml 匹配）：
- 用户名: `root`
- 密码: `password`
- 数据库: `test_db`
- 端口: `3306`

**方式二：使用完整配置文件**

复制环境变量模板并修改：

```bash
cp .env.example .env
```

编辑 `.env` 文件，修改数据库密码、端口等配置。

启动所有数据库：

```bash
docker-compose up -d
```

### 3. 启动指定数据库

```bash
# 只启动 MySQL（使用完整配置）
docker-compose up -d mysql

# 启动 MySQL 和 KingBase
docker-compose up -d mysql kingbase
```

### 5. 查看服务状态

```bash
docker-compose ps
```

### 6. 查看日志

```bash
# 查看所有服务日志
docker-compose logs -f

# 查看指定服务日志
docker-compose logs -f mysql
```

### 7. 停止服务

```bash
# 停止所有服务
docker-compose down

# 停止并删除数据卷（谨慎使用）
docker-compose down -v
```

## 数据库连接信息

### MySQL

- **主机**: `localhost` (容器内使用服务名 `mysql`)
- **端口**: `3306` (可在 `.env` 中修改)
- **用户名**: `root` 或 `.env` 中配置的 `MYSQL_USER`
- **密码**: `.env` 中的 `MYSQL_ROOT_PASSWORD` 或 `MYSQL_PASSWORD`
- **数据库**: `.env` 中的 `MYSQL_DATABASE`

连接示例：
```bash
mysql -h localhost -P 3306 -u root -p
```

### KingBase

- **主机**: `localhost` (容器内使用服务名 `kingbase`)
- **端口**: `54321` (可在 `.env` 中修改)
- **用户名**: `SYSTEM`
- **密码**: `.env` 中的 `KINGBASE_SYSTEM_PASSWORD`
- **数据库**: `TEST`

连接示例：
```bash
ksql -USYSTEM -W123456 -h localhost -p 54321 TEST
```

### VastBase

- **主机**: `localhost` (容器内使用服务名 `vastbase`)
- **端口**: `5432` (可在 `.env` 中修改)
- **用户名**: `.env` 中的 `VASTBASE_USER`
- **密码**: `.env` 中的 `VASTBASE_PASSWORD`
- **数据库**: `.env` 中的 `VASTBASE_DB`

连接示例：
```bash
psql -h localhost -p 5432 -U vastbase -d vastbase
```

### GBase

- **主机**: `localhost` (容器内使用服务名 `gbase`)
- **端口**: `19088` (可在 `.env` 中修改)
- **用户名**: 根据镜像配置
- **密码**: `.env` 中的 `GBASE_PASSWORD`

连接示例：
```bash
dbaccess -h localhost -p 19088 sysmaster
```

## 初始化脚本

每个数据库支持通过初始化脚本自动创建数据库、表结构等。

初始化脚本位置：
- MySQL: `init-scripts/mysql/*.sql`
- KingBase: `init-scripts/kingbase/*.sql`
- VastBase: `init-scripts/vastbase/*.sql`
- GBase: `init-scripts/gbase/*.sql`

脚本会在数据库首次启动时自动执行。

## 数据持久化

所有数据库数据存储在 Docker volumes 中，即使容器删除，数据也不会丢失：

- `mysql_data`: MySQL 数据
- `kingbase_data`: KingBase 数据
- `vastbase_data`: VastBase 数据
- `gbase_data`: GBase 数据

查看 volumes：
```bash
docker volume ls | grep datasource
```

备份数据：
```bash
# 备份 MySQL 数据
docker run --rm -v datasource-hub_mysql_data:/data -v $(pwd):/backup alpine tar czf /backup/mysql_backup.tar.gz /data
```

## 健康检查

所有服务都配置了健康检查，确保数据库完全启动后才标记为健康状态。

查看健康状态：
```bash
docker-compose ps
```

## 网络配置

所有数据库服务在同一个 Docker 网络 `datasource-network` 中，可以通过服务名互相访问。

例如，在另一个容器中连接 MySQL：
```yaml
# 在其他服务的 docker-compose.yml 中
environment:
  - MYSQL_HOST=mysql  # 使用服务名
  - MYSQL_PORT=3306
```

## 常见问题

### 1. 端口冲突

如果默认端口已被占用，在 `.env` 文件中修改对应端口配置。

### 2. KingBase 授权文件

如果 KingBase 需要授权文件，请：
1. 将 `license.dat` 文件放置到 `./kingbase/` 目录
2. 在 `docker-compose.yml` 中取消注释授权文件挂载配置

### 3. GBase 镜像问题

GBase 的公开镜像较少，如果使用的镜像不可用：
1. 联系 GBase 官方获取镜像
2. 或自行构建 Dockerfile

### 4. 数据卷清理

⚠️ **警告**: 删除数据卷会永久删除所有数据！

```bash
# 删除所有数据卷（谨慎使用）
docker-compose down -v
```

## 开发建议

### 与 data-integrate-test 集成

在 `data-integrate-test` 的配置文件中，可以这样配置数据源：

```yaml
database:
  type: "mysql"
  host: "localhost"  # 或使用 docker 服务名 "mysql"
  port: 3306
  user: "root"
  password: "root123"
  database: "testdb"
```

### 与 mira-data-service-server 集成

在 `mira-data-service-server` 的配置中：

```yaml
dbms:
  type: "mysql"
  host: "mysql"  # 使用 docker 服务名
  port: 3306
  user: "root"
  password: "root123"
  db: "testdb"
```

## 许可证

本项目仅用于开发和测试环境。生产环境使用请遵循各数据库的许可证要求。

## 贡献

欢迎提交 Issue 和 Pull Request！

