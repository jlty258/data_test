# Docker 快速使用指南

## 构建镜像

```bash
# 构建包含所有工具的基础镜像
docker build -t data-integrate-test:latest .
```

## 使用方式

### 1. 查看可用工具

```bash
docker run --rm data-integrate-test:latest
```

### 2. 导出表数据

```bash
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/exports:/app/exports \
  data-integrate-test:latest \
  /app/export-table -db=mysql -dbname=test -table=users -output=/app/exports
```

### 3. 导入表数据

```bash
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/exports:/app/exports \
  data-integrate-test:latest \
  /app/import-table -db=mysql -dbname=test -table=users -input=/app/exports
```

### 4. 管理 IDA 服务

```bash
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  data-integrate-test:latest \
  /app/manage-ida -action=all
```

### 5. 执行测试

```bash
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/templates:/app/templates:ro \
  data-integrate-test:latest \
  /app/data-integrate-test -template=templates/mysql/mysql_1m_8fields.yaml
```

## 工具列表

| 工具 | 路径 | 用途 |
|------|------|------|
| data-integrate-test | `/app/data-integrate-test` | 执行测试模板 |
| manage-ida | `/app/manage-ida` | IDA 服务管理 |
| export-table | `/app/export-table` | 导出单个表 |
| import-table | `/app/import-table` | 导入单个表 |
| export-snapshots | `/app/export-snapshots` | 批量导出快照 |
| import-snapshots | `/app/import-snapshots` | 批量导入快照 |
| test-clients | `/app/test-clients` | 测试客户端 |

## 卷挂载说明

- `config`: 配置文件目录（只读）
- `templates`: 模板文件目录（只读）
- `exports/imports`: 数据目录（读写）

## 更多信息

详细的最佳实践和架构说明，请参考 [DOCKER_BEST_PRACTICES.md](./DOCKER_BEST_PRACTICES.md)
