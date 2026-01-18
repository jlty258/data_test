#!/bin/bash
# 根据环境变量生成 config.yaml 文件

CONFIG_FILE="/home/workspace/config/config.yaml"

# 从环境变量读取配置，如果没有则使用默认值
MYSQL_HOST=${MYSQL_HOST:-host.docker.internal}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-password}
MYSQL_DATABASE=${MYSQL_DATABASE:-mira_db}

MINIO_HOST=${MINIO_HOST:-minio}
MINIO_PORT=${MINIO_PORT:-9000}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

LOG_LEVEL=${LOG_LEVEL:-info}

# 生成 config.yaml
cat > "$CONFIG_FILE" << EOF
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
EOF

echo "Config file generated at $CONFIG_FILE"
cat "$CONFIG_FILE"
