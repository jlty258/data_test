package service

import (
	"context"
	"data-service/common"
	"data-service/config"
	ds "data-service/generated/datasource"
	"data-service/log"
	"data-service/oss"
	"fmt"
	"strings"
)

// cleanupTlsFiles 清理 TLS 相关文件（Doris FILE + MinIO 对象）
func (s *DorisService) cleanupTlsFiles(dbName, requestId string, dbType int32) {
	// 清理 Doris FILE 引用
	s.cleanupDorisTlsFiles(dbName, requestId, dbType)

	// 清理 MinIO 对象
	s.cleanupMinioTlsObjects(requestId, dbType)
}

// cleanupDorisTlsFiles 清理 Doris 中的 TLS 文件引用
func (s *DorisService) cleanupDorisTlsFiles(dbName, requestId string, dbType int32) {
	switch dbType {
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE):
		// Kingbase: PEM 格式，catalog=kingbase
		s.dropFileFromDB(dbName, fmt.Sprintf("%s_ca_cert.pem", requestId), common.KINGBASE_CERT_DIR)
		s.dropFileFromDB(dbName, fmt.Sprintf("%s_client_cert.pem", requestId), common.KINGBASE_CERT_DIR)
		s.dropFileFromDB(dbName, fmt.Sprintf("%s_client_key.pem", requestId), common.KINGBASE_CERT_DIR)
		s.dropFileFromDB(dbName, fmt.Sprintf("%s_client_cert.p12", requestId), common.KINGBASE_CERT_DIR)

	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL),
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_TIDB),
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_TDSQL):
		// MySQL/TiDB/TDSQL: PKCS12 格式，catalog=mysql
		s.dropFileFromDB(dbName, fmt.Sprintf("%s_ca_cert.p12", requestId), common.MYSQL_CERT_DIR)
		s.dropFileFromDB(dbName, fmt.Sprintf("%s_client_cert.p12", requestId), common.MYSQL_CERT_DIR)
	}
}

// cleanupMinioTlsObjects 清理 MinIO 上的 TLS 证书对象
func (s *DorisService) cleanupMinioTlsObjects(requestId string, dbType int32) {
	conf := config.GetConfigMap()
	ossClient, err := oss.NewOSSFactory(conf).NewOSSClient()
	if err != nil {
		log.Logger.Errorf("Failed to create OSS client for TLS cleanup: %v", err)
		return
	}

	ctx := context.Background()
	bucket := common.TLS_CERT_BUCKET_NAME

	var candidates []string
	switch dbType {
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE):
		// Kingbase: PEM 格式
		candidates = []string{
			fmt.Sprintf("%s_ca_cert.pem", requestId),
			fmt.Sprintf("%s_client_cert.pem", requestId),
			fmt.Sprintf("%s_client_key.pem", requestId),
			fmt.Sprintf("%s_client_cert.p12", requestId),
		}
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL),
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_TIDB),
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_TDSQL):
		// MySQL/TiDB/TDSQL: PKCS12 格式
		candidates = []string{
			fmt.Sprintf("%s_ca_cert.p12", requestId),
			fmt.Sprintf("%s_client_cert.p12", requestId),
		}
	default:
		return // 其他类型不需要清理
	}

	for _, name := range candidates {
		if err := ossClient.DeleteObject(ctx, bucket, name); err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "not found") {
				log.Logger.Warnf("Failed to delete TLS cert object %s/%s: %v", bucket, name, err)
			}
		} else {
			log.Logger.Infof("Deleted TLS cert object %s/%s", bucket, name)
		}
	}
}

// isTlsUsed 检查是否使用了 TLS 连接
func (s *DorisService) isTlsUsed(dbName, requestId string) bool {
	// 检查是否存在证书文件（兼容 PEM 和 PKCS12 格式）
	return s.getFileInfo(dbName, fmt.Sprintf("%s_ca_cert.pem", requestId)) != nil ||
		s.getFileInfo(dbName, fmt.Sprintf("%s_ca_cert.p12", requestId)) != nil
}

// 精确删除：DROP FILE "xxx" FROM <db> PROPERTIES("catalog"="mysql")
func (s *DorisService) dropFileFromDB(dbName, fileName string, catalog string) error {
	dropSQL := fmt.Sprintf(`DROP FILE "%s" FROM %s PROPERTIES("catalog" = "%s")`, fileName, dbName, catalog)
	_, err := s.ExecuteUpdate(dropSQL)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") ||
			strings.Contains(strings.ToLower(err.Error()), "no such file") {
			log.Logger.Debugf("TLS file already gone or catalog mismatch (ignored): db=%s catalog=%s file=%s", dbName, catalog, fileName)
			return nil
		}
		return fmt.Errorf("failed to drop file %s from %s: %v", fileName, dbName, err)
	}
	log.Logger.Infof("Dropped file %s from db %s", fileName, dbName)
	return nil
}

// CleanupAllFilesInDatabase 删除指定数据库中通过 CREATE FILE 创建的所有文件
func (s *DorisService) CleanupAllFilesInDatabase(dbName string) {
	query := fmt.Sprintf("SHOW FILE FROM %s", dbName)
	rows, done, err := s.ExecuteSQL(query)
	if err != nil || rows == nil {
		if err != nil {
			log.Logger.Warnf("Failed to list FILE entries for db=%s: %v", dbName, err)
		} else {
			log.Logger.Infof("No FILE entries found for db=%s", dbName)
		}
		return
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	for rows.Next() {
		var fileId int64
		var db, catalog, fileName, fileSize, isContent, md5 string
		if scanErr := rows.Scan(&fileId, &db, &catalog, &fileName, &fileSize, &isContent, &md5); scanErr != nil {
			log.Logger.Warnf("Failed to scan FILE row for db=%s: %v", dbName, scanErr)
			continue
		}
		if err := s.dropFileFromDB(dbName, fileName, catalog); err != nil {
			log.Logger.Warnf("Failed to drop FILE '%s' from db=%s (catalog=%s): %v", fileName, dbName, catalog, err)
		}
	}
	log.Logger.Infof("Finished cleaning up FILE entries for db=%s", dbName)
}
