#!/bin/bash
set -e

# 确保配置目录存在
mkdir -p /home/workspace/config

# 从环境变量读取配置，如果没有则使用默认值
# 在 Linux 上，使用 mysql 服务名（如果在同一网络）或 172.17.0.1（宿主机）
MYSQL_HOST=${MYSQL_HOST:-mysql}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-root123456}
MYSQL_DATABASE=${MYSQL_DATABASE:-mira_db}

MINIO_HOST=${MINIO_HOST:-minio}
MINIO_PORT=${MINIO_PORT:-9000}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

# Doris 配置
DORIS_HOST=${DORIS_HOST:-doris-fe}
DORIS_PORT=${DORIS_PORT:-9030}
DORIS_USER=${DORIS_USER:-root}
DORIS_PASSWORD=${DORIS_PASSWORD:-}
DORIS_DATABASE=${DORIS_DATABASE:-}

LOG_LEVEL=${LOG_LEVEL:-info}

# 生成 config.yaml
cat > /home/workspace/config/config.yaml << EOF
oss:
  type: "minio"
  host: "${MINIO_HOST}"
  port: ${MINIO_PORT}
  access_key: "${MINIO_ACCESS_KEY}"
  secret_key: "${MINIO_SECRET_KEY}"

dbms:
  type: "mysql"
  params: "parseTime=true&loc=Local"
  host: "${MYSQL_HOST}"
  port: ${MYSQL_PORT}
  user: "${MYSQL_USER}"
  password: "${MYSQL_PASSWORD}"
  db: "${MYSQL_DATABASE}"
  dsn: "${MYSQL_USER}:${MYSQL_PASSWORD}@tcp(${MYSQL_HOST}:${MYSQL_PORT})/${MYSQL_DATABASE}?parseTime=true&loc=Local"
  max_open_conns: 10
  max_idle_conns: 5

http:
  port: 8080
  data_server: "http://localhost:8080"

spark:
  account_name: "default-account"
  namespace: "default"
  upload_path: "/path/to/upload"
  image_name: "your-spark-image"
  image_tag: "latest"
  master: "local[*]"
  event_log_enable: true
  event_log_dir: "file:///path/to/eventlog"
  clean_interval: 3600
  bucket_name: "your-bucket-name"
  retention_days: 7
  log_retention_days: 14
  data_retention_days: 30

log:
  level: "${LOG_LEVEL}"

clean_table:
  clean_enable: true
  retention_days: 7
  interval: 24

common:
  port: 9090

doris:
  address: "${DORIS_HOST}"
  port: ${DORIS_PORT}
  user: "${DORIS_USER}"
  password: "${DORIS_PASSWORD}"
  db: "${DORIS_DATABASE}"
  batch_insert_size: 1000
  max_open_conns: 10
  max_idle_conns: 5
  max_life_time: 60
  max_idle_time: 30
  query_timeout: 1800
  import_batch_size: 10000
  import_max_retry: 3
  s3_export_max_file_size: "1GB"
  s3_export_request_timeout: 300
  s3_export_connection_timeout: 60
  s3_export_connection_maximum: 10
EOF

echo "=========================================="
echo "Config file generated from environment variables"
echo "=========================================="
echo "MySQL: ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}"
echo "MinIO: ${MINIO_HOST}:${MINIO_PORT}"
echo "Doris: ${DORIS_USER}@${DORIS_HOST}:${DORIS_PORT}/${DORIS_DATABASE}"
echo "Log Level: ${LOG_LEVEL}"
echo "=========================================="

# 启动服务
# 如果 dataserver 在 /home/workspace/bin/ 目录，使用完整路径
if [ -f "/home/workspace/bin/dataserver" ]; then
    exec /home/workspace/bin/dataserver -config /home/workspace/config/config.yaml
elif [ -f "/home/workspace/dataserver" ]; then
    exec /home/workspace/dataserver -config /home/workspace/config/config.yaml
else
    echo "Error: dataserver binary not found!"
    exit 1
fi
