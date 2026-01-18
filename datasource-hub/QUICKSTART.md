# MySQL 快速启动指南

根据 `data-integrate-test/config/test_config.yaml` 配置启动 MySQL 实例。

## 配置信息

- **用户名**: `root`
- **密码**: `password`
- **数据库**: `test_db`
- **端口**: `3306`

## 启动方式

### 方式一：使用 Docker Compose（推荐）

```bash
# 进入 datasource-hub 目录
cd datasource-hub

# 启动 MySQL
docker-compose -f docker-compose.mysql.yml up -d
```

### 方式二：使用启动脚本

**Linux/Mac:**
```bash
cd datasource-hub
chmod +x start-mysql.sh
./start-mysql.sh
```

**Windows PowerShell:**
```powershell
cd datasource-hub
.\start-mysql.ps1
```

### 方式三：直接使用 Docker 命令

```bash
docker run -d \
  --name mysql-test \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=test_db \
  -p 3306:3306 \
  -v mysql_test_data:/var/lib/mysql \
  mysql:8.0 \
  --character-set-server=utf8mb4 \
  --collation-server=utf8mb4_unicode_ci \
  --default-authentication-plugin=mysql_native_password
```

## 验证连接

### 使用 MySQL 客户端

```bash
mysql -h localhost -P 3306 -u root -ppassword test_db
```

### 使用 Docker 执行

```bash
docker exec -it mysql-test mysql -uroot -ppassword test_db
```

### 测试连接

```bash
docker exec -it mysql-test mysql -uroot -ppassword -e "SHOW DATABASES;"
```

## 常用操作

### 查看日志

```bash
docker-compose -f docker-compose.mysql.yml logs -f
```

### 停止服务

```bash
docker-compose -f docker-compose.mysql.yml down
```

### 停止并删除数据

```bash
docker-compose -f docker-compose.mysql.yml down -v
```

### 重启服务

```bash
docker-compose -f docker-compose.mysql.yml restart
```

## 连接信息

在 `data-integrate-test` 项目中，配置如下：

```yaml
databases:
  - name: "mysql_test"
    type: "mysql"
    host: "host.docker.internal"  # 从容器访问本地MySQL
    port: 3306
    user: "root"
    password: "password"
    database: "test_db"
```

**注意**: 
- 如果 `data-integrate-test` 在 Docker 容器中运行，使用 `host.docker.internal` 作为主机名
- 如果 `data-integrate-test` 在本地运行，使用 `localhost` 作为主机名

## 初始化脚本

可以将 SQL 初始化脚本放在 `init-scripts/mysql/` 目录下，MySQL 启动时会自动执行。

例如：`init-scripts/mysql/01-init.sql`

```sql
-- 创建测试表
CREATE TABLE IF NOT EXISTS test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 故障排查

### 检查容器状态

```bash
docker ps | grep mysql-test
```

### 查看容器日志

```bash
docker logs mysql-test
```

### 进入容器

```bash
docker exec -it mysql-test bash
```

### 检查端口占用

```bash
# Linux/Mac
netstat -tuln | grep 3306

# Windows
netstat -ano | findstr 3306
```

如果端口被占用，可以修改 `docker-compose.mysql.yml` 中的端口映射。

