/*
*

	@author: shiliang
	@date: 2024/9/19
	@note:

*
*/
package common

import (
	"data-service/config"
	"time"
)

const (
	TASk_DATA_BUCKET_NAME  = "task-data"
	BATCH_DATA_BUCKET_NAME = "data-service"

	MYSQL_TLS_CONFIG    = "mysql-tls"
	KINGBASE_TLS_CONFIG = "kingbase-tls"

	//BATCH_DATA_SIZE  = 20000
	//STREAM_DATA_SIZE = 20000

	MAX_RETRY_COUNT    = 3
	MAX_RETRY_INTERVAL = 2 * time.Second

	MAX_CHUNK_SIZE = 1024 * 1024 // 1MB

	SPARK_UPLOAD_PATH_PREFIX = "spark-upload"

	SPARK_EVENT_LOG_PATH_PREFIX = "logs"

	SPARK_DATA_PATH_PREFIX = "data"

	DATA_DIR = "/home/workspace/data"

	GRPC_MAX_MESSAGE_SIZE = 3 * 1024 * 1024 // 3MB

	// 批量插入时参数数量的最大限制
	MAX_BATCH_ARGS_SIZE = 50000

	// 中间表数据库
	MPC_TEMP_DB_NAME = "mira_task_tmp"

	MIRA_TMP_TASK_DB = "mira_task_tmp"

	// 存储结果的bucket
	RESULT_BUCKET_NAME = "result"

	// 新增 gRPC 传输大小限制变量
	GRPC_TRANSFER_SIZE = 500 * 1024 * 1024 // 500MB

	// 存放TLS证书bucket
	TLS_CERT_BUCKET_NAME = "tls-cert"

	// 后缀随机数位数
	SUFFIX_RANDOM_LENGTH = 8

	// Doris token
	DORIS_TOKEN = "ea3151f7-040c-4903-9322-5aa0f02daca6"

	// MySQL证书目录
	MYSQL_CERT_DIR = "mysql"

	// KingBase证书目录
	KINGBASE_CERT_DIR = "kingbase"

	// TLS keystore/client p12 password
	TLS_KEYSTORE_PASSWORD = "doris123"
)

var (
	BATCH_DATA_SIZE  int
	STREAM_DATA_SIZE int
)

func LoadCommonConfig(conf *config.DataServiceConf) {
	// 如果 conf.Dbms.BatchDataSize 未设置，使用默认值 20000
	if conf.Dbms.BatchDataSize == 0 {
		BATCH_DATA_SIZE = 20000
	} else {
		BATCH_DATA_SIZE = conf.Dbms.BatchDataSize
	}

	// 如果 conf.Dbms.StreamDataSize 未设置，使用默认值 20000
	if conf.Dbms.StreamDataSize == 0 {
		STREAM_DATA_SIZE = 20000
	} else {
		STREAM_DATA_SIZE = conf.Dbms.StreamDataSize
	}
}
