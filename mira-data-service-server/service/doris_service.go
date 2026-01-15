//go:generate mockgen -destination=../mocks/mock_doris_service.go -package=mocks data-service/service IDorisService
/*
*

	@author: shiliang
	@date: 2025/07/04
	@note: Apache Doris服务功能

*
*/
package service

import (
	"data-service/common"
	"data-service/config"
	"data-service/database"
	ds "data-service/generated/datasource"
	"data-service/log"
	"data-service/oss"
	"data-service/utils"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"bytes"

	"context"
	"io"
	"os"
	"path/filepath"

	"sync"

	"net/url"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/google/uuid"
)

var (
	globalResourceName string
	resourceInitOnce   sync.Once
)

// IDorisService Doris服务接口
type IDorisService interface {
	ExecuteSQL(sql string, args ...interface{}) (*sql.Rows, func(), error)
	ExecuteUpdate(sql string, args ...interface{}) (int64, error)
	Close() error
	EnsureDorisDatabaseExists(dbName string) error
	CreateExternalAndInternalTableAndImportData(assetName string, chainInfoId string, alias string, jobInstanceId string, targetTableName string, targetDbName string, columns ...string) (string, error)
	CreateExternalAndInternalTableAndImportDataBatched(assetName string, chainInfoId string, alias string, jobInstanceId string, targetTableName string, targetDbName string, pkColumn string, batchSize int, columns ...string) (string, error)
	ImportArrowFileToDoris(bucketName string, objectName string, tableName string, dbName string) error
	ImportCsvFileToDoris(request *ds.ImportCsvFileToDorisRequest) error
	ExportParquetFileFromDoris(request *ds.ExportCsvFileFromDorisRequest) error
	ExportDorisDataToMiraDB(request *ds.ExportDorisDataToMiraDBRequest) error
	ImportMiraDBDataToDoris(request *ds.ImportMiraDBDataToDorisRequest) (string, error)
	ProcessDataSourceAndExport(request *ds.ReadDataSourceStreamingRequest, enhancedJobInstanceId string) (string, error)
	CreateExternalTableFromAsset(assetName string, chainInfoId string, alias string, jobInstanceId string, targetTableName string, targetDbName string, columnList ...string) ([]string, string, error)
	GetDorisTableSchema(dbName, tableName string) ([]*ds.ColumnItem, error)
	ConvertRequestToArrowSchema(request *ds.ExportDorisDataToMiraDBRequest) (*arrow.Schema, error)
	InitGlobalResource() error
	InitMiraTaskTmpDatabase() error
	CleanDorisTableWithPrefix(prefix string) error
	DropDatabase(dbName string) error
	DropTable(dbName, tableName string) error
	GetDBName() string
	SwitchDatabase(dbName string) error // 新增
}

// DorisService 提供Doris数据库操作的服务
type DorisService struct {
	dbStrategy database.DatabaseStrategy
}

// FileInfo 文件信息结构体
type FileInfo struct {
	FileId int64
	MD5    string
}

// NewDorisService 创建新的Doris服务实例
func NewDorisService(dbName ...string) (IDorisService, error) {
	config := config.GetConfigMap()

	// 如果提供了数据库名，使用它；否则使用配置中的默认数据库
	var targetDbName string
	if len(dbName) > 0 && dbName[0] != "" {
		targetDbName = dbName[0]
	} else {
		targetDbName = ""
	}

	connInfo := &ds.ConnectionInfo{
		Host:     config.DorisConfig.Address,
		Port:     9030,
		User:     config.DorisConfig.User,
		Password: config.DorisConfig.Password,
		DbName:   targetDbName,
	}

	// 使用数据库工厂创建Doris策略
	strategy, err := database.DatabaseFactory(ds.DataSourceType_DATA_SOURCE_TYPE_DORIS, connInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create Doris strategy: %v", err)
	}

	// 连接到Doris数据库
	if err := strategy.ConnectToDBWithPass(connInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to Doris: %v", err)
	}

	if targetDbName != "" {
		log.Logger.Infof("Connected to Doris database: %s", targetDbName)
	} else {
		log.Logger.Infof("Connected to Doris with default database")
	}

	return &DorisService{
		dbStrategy: strategy,
	}, nil
}

// ExecuteSQL 执行SQL语句并返回结果
func (s *DorisService) ExecuteSQL(sql string, args ...interface{}) (*sql.Rows, func(), error) {
	log.Logger.Infof("Executing SQL on Doris: %s", sql)
	s.printCurrentDatabase()
	// 检查SQL类型（不区分大小写）
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))
	targetDB := s.GetDBName()
	if strings.HasPrefix(sqlUpper, "SELECT") {
		// 特判：SELECT ... INTO OUTFILE 需作为更新执行，避免阻塞等待结果集
		// 问题原因：SELECT ... INTO OUTFILE 被错误地当作普通 SELECT 查询处理，走了 Query() 路径等待结果集，但该语句只返回 OK 包，导致驱动阻塞等待列头。
		// 解决措施：在 ExecuteSQL 中添加特判，检测到 INTO OUTFILE 时改走 ExecuteUpdate 路径使用 db.Exec()，避免协议语义不匹配导致的阻塞问题。
		if strings.Contains(sqlUpper, "INTO OUTFILE") {
			log.Logger.Infof("Detected SELECT INTO OUTFILE; executing as update")
			_, err := s.ExecuteUpdate(sql, args...)
			if err != nil {
				return nil, nil, err
			}
			return nil, nil, nil
		}

		// 使用 DorisStrategy 的专用连接查询，避免串库
		if dsStrategy, ok := s.dbStrategy.(*database.DorisStrategy); ok && targetDB != "" {
			rows, done, err := dsStrategy.QueryInDB(context.Background(), targetDB, sql, args...)
			if err != nil {
				log.Logger.Errorf("Failed to execute SQL on Doris: %v", err)
				return nil, nil, fmt.Errorf("failed to execute SQL: %v", err)
			}
			return rows, done, nil
		}
		// SELECT查询，返回结果集
		rows, err := s.dbStrategy.Query(sql, args...)
		if err != nil {
			log.Logger.Errorf("Failed to execute SELECT SQL on Doris: %v", err)
			return nil, nil, fmt.Errorf("failed to execute SELECT SQL: %v", err)
		}
		return rows, nil, nil
	} else if strings.HasPrefix(sqlUpper, "INSERT") ||
		strings.HasPrefix(sqlUpper, "UPDATE") ||
		strings.HasPrefix(sqlUpper, "DELETE") ||
		strings.HasPrefix(sqlUpper, "CREATE") ||
		strings.HasPrefix(sqlUpper, "ALTER") ||
		strings.HasPrefix(sqlUpper, "DROP") {
		// INSERT, UPDATE, DELETE, CREATE, ALTER, DROP等操作，不返回结果集
		affectedRows, err := s.ExecuteUpdate(sql, args...)
		if err != nil {
			return nil, nil, err
		}
		log.Logger.Infof("SQL executed successfully, affected rows: %d", affectedRows)
		return nil, nil, nil
	} else {
		// 其他SQL语句，尝试作为查询执行
		rows, err := s.dbStrategy.Query(sql, args...)
		if err != nil {
			log.Logger.Errorf("Failed to execute SQL on Doris: %v", err)
			return nil, nil, fmt.Errorf("failed to execute SQL: %v", err)
		}
		return rows, nil, nil
	}
}

// ExecuteUpdate 执行更新操作（INSERT, UPDATE, DELETE等）并返回受影响的行数
func (s *DorisService) ExecuteUpdate(sql string, args ...interface{}) (int64, error) {
	//log.Logger.Infof("Executing update SQL on Doris: %s", sql)
	log.Logger.Infow("Executing SQL on Doris",
		"len", len(sql),
		"preview", utils.PreviewSQL(sql, 500),
	)

	// 优先使用 DorisStrategy 的专用连接（保障并发不同库隔离）
	if dsStrategy, ok := s.dbStrategy.(*database.DorisStrategy); ok {
		targetDB := s.GetDBName()
		if targetDB != "" {
			result, err := dsStrategy.ExecInDB(context.Background(), targetDB, sql, args...)
			if err != nil {
				log.Logger.Errorf("Failed to execute update SQL on Doris: %v", err)
				return 0, fmt.Errorf("failed to execute update SQL: %v", err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return 0, fmt.Errorf("failed to get rows affected: %v", err)
			}
			return affected, nil
		}
	}

	// 获取底层数据库连接
	db := database.GetDB(s.dbStrategy)
	if db == nil {
		return 0, fmt.Errorf("failed to get database connection")
	}

	// 执行更新操作
	result, err := db.Exec(sql, args...)
	if err != nil {
		log.Logger.Errorf("Failed to execute update SQL on Doris: %v", err)
		return 0, fmt.Errorf("failed to execute update SQL: %v", err)
	}

	// 获取受影响的行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %v", err)
	}

	return rowsAffected, nil
}

// Close 关闭数据库连接
func (s *DorisService) Close() error {
	if s.dbStrategy != nil {
		return s.dbStrategy.Close()
	}
	return nil
}

// ExternalTableConfig 外部表配置
type ExternalTableConfig struct {
	ResourceName    string              // 资源名称
	TableName       string              // 表名
	Columns         []common.ColumnInfo // 列信息
	JdbcURL         string              // JDBC连接URL
	DriverURL       string              // 驱动JAR包URL
	DriverClass     string              // 驱动类名
	Username        string              // 用户名
	Password        string              // 密码
	SourceTableName string              // 源表名
	TableType       string              // 表类型（mysql, postgresql等）
	Properties      map[string]string   // 额外属性
}

// CreateExternalResourceAndTables 创建外部资源、外部表、内部表
func (s *DorisService) CreateExternalResourceAndTables(config ExternalTableConfig) error {
	// 1. 创建外部资源
	if err := s.createExternalResource(config); err != nil {
		return fmt.Errorf("failed to create external resource: %v", err)
	}

	// 2. 创建外部表
	if err := s.createExternalTable(config); err != nil {
		// 如果创建外部表失败，尝试清理已创建的资源
		s.dropExternalResource(config.ResourceName)
		return fmt.Errorf("failed to create external table: %v", err)
	}

	log.Logger.Infof("Successfully created external resource '%s' and table '%s'",
		config.ResourceName, config.TableName)
	return nil
}

// createExternalResource 创建外部资源
func (s *DorisService) createExternalResource(config ExternalTableConfig) error {
	// 构建创建外部资源的SQL
	resourceSQL := fmt.Sprintf(`
		CREATE EXTERNAL RESOURCE IF NOT EXISTS "%s"
		PROPERTIES (
			"type" = "jdbc",
			"user" = "%s",
			"password" = "%s",
			"jdbc_url" = "%s",
			"driver_url" = "%s",
			"driver_class" = "%s"
		)
	`, config.ResourceName, config.Username, config.Password,
		config.JdbcURL, config.DriverURL, config.DriverClass)

	// 执行创建资源SQL
	_, err := s.ExecuteUpdate(resourceSQL)
	if err != nil {
		return fmt.Errorf("failed to create external resource '%s': %v", config.ResourceName, err)
	}

	log.Logger.Infof("Created external resource: %s", config.ResourceName)
	return nil
}

func (s *DorisService) dropResource(resourceName string) error {
	dropSQL := fmt.Sprintf(`DROP RESOURCE "%s"`, resourceName)
	_, err := s.ExecuteUpdate(dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop external resource '%s': %v", resourceName, err)
	}
	log.Logger.Infof("Dropped external resource: %s", resourceName)
	return nil
}

// createExternalTable 创建外部表
func (s *DorisService) createExternalTable(config ExternalTableConfig) error {
	// 构建列定义
	var columnDefs []string
	for _, col := range config.Columns {
		columnDef := fmt.Sprintf("`%s` %s", col.Name, col.DataType)

		if !col.Nullable {
			columnDef += " NOT NULL"
		}

		if col.Default != "" {
			columnDef += fmt.Sprintf(" DEFAULT %s", col.Default)
		}

		columnDefs = append(columnDefs, columnDef)
	}

	// 构建创建外部表的SQL
	tableSQL := fmt.Sprintf(`
		CREATE EXTERNAL TABLE IF NOT EXISTS %s (
			%s
		)
		ENGINE=JDBC
		PROPERTIES (
			"resource" = "%s",
			"table" = "%s",
			"table_type" = "%s"
		)
	`, config.TableName, strings.Join(columnDefs, ",\n\t\t"),
		config.ResourceName, config.SourceTableName, config.TableType)

	// 执行创建表SQL
	_, err := s.ExecuteUpdate(tableSQL)
	if err != nil {
		return fmt.Errorf("failed to create external table '%s': %v", config.TableName, err)
	}

	log.Logger.Infof("Created external table: %s", config.TableName)
	return nil
}

// dropExternalResource 删除外部资源
func (s *DorisService) dropExternalResource(resourceName string) error {
	dropSQL := fmt.Sprintf(`DROP RESOURCE "%s"`, resourceName)
	_, err := s.ExecuteUpdate(dropSQL)
	if err != nil {
		log.Logger.Warnf("Failed to drop external resource '%s': %v", resourceName, err)
		return err
	}
	log.Logger.Infof("Dropped external resource: %s", resourceName)
	return nil
}

// DropExternalTableAndResource 删除外部表和资源
func (s *DorisService) DropExternalTableAndResource(tableName, resourceName string) error {
	// 1. 删除外部表
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := s.ExecuteUpdate(dropTableSQL)
	if err != nil {
		log.Logger.Warnf("Failed to drop external table '%s': %v", tableName, err)
		// 继续尝试删除资源
	} else {
		log.Logger.Infof("Dropped external table: %s", tableName)
	}

	// 2. 删除外部资源
	if err := s.dropExternalResource(resourceName); err != nil {
		return fmt.Errorf("failed to drop external resource: %v", err)
	}

	log.Logger.Infof("Successfully dropped external table '%s' and resource '%s'", tableName, resourceName)
	return nil
}

// getAssetInfo 获取数据资产信息，使用现有的GetDatasourceByAssetName函数
func (s *DorisService) getAssetInfo(assetName string, chainInfoId string, alias string, targetDbName string) (*AssetInfo, error) {
	// 使用现有的GetDatasourceByAssetName函数获取数据源信息
	requestId := uuid.New().String()
	connInfo, err := utils.GetDatasourceByAssetName(requestId, assetName, chainInfoId, alias)
	if err != nil {
		return nil, fmt.Errorf("failed to get datasource info: %v", err)
	}

	log.Logger.Infof("Datasource connInfo: %v", connInfo)

	// 失败清理哨兵
	ok := false
	defer func() {
		if !ok {
			db := targetDbName
			if db != "" && s.isTlsUsed(db, requestId) {
				s.cleanupTlsFiles(db, requestId, connInfo.Dbtype)
			}
		}
	}()

	// 如果有TLS配置，上传证书到doris
	if connInfo.TlsConfig != nil && connInfo.TlsConfig.UseTls == 2 {
		if err := s.processTLSCertificates(connInfo.TlsConfig, requestId, connInfo.Dbtype); err != nil {
			log.Logger.Errorf("failed to process TLS certificates: %v", err)
			return nil, fmt.Errorf("failed to process TLS certificates: %v", err)
		}
	}

	// 根据数据库类型确定表类型和驱动信息
	tableType, driverURL, driverClass := s.getDatabaseTypeInfo(connInfo.Dbtype)

	// 如果 Kingbase 且启用 TLS，则追加自定义 SSL 工厂 fat jar
	if (tableType == "kingbase8") && connInfo.TlsConfig != nil && connInfo.TlsConfig.UseTls == 2 {
		driverURL = "file:///opt/apache-doris/driver/kingbase-ssl-factory-fat-1.0-SNAPSHOT.jar"
	}

	// todo vastbase是不是要特殊处理
	if tableType == "vastbase" {
		driverURL = "file:///opt/apache-doris/driver/vastbase-ssl-factory-fat-1.0-SNAPSHOT.jar"
	}

	// 构建JDBC URL
	jdbcURL := s.buildJdbcURL(connInfo, tableType, requestId, targetDbName)

	// 获取表结构信息
	columns, err := s.getTableColumns(connInfo, assetName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %v", err)
	}
	ok = true

	return &AssetInfo{
		JdbcURL:         jdbcURL,
		DriverURL:       driverURL,
		DriverClass:     driverClass,
		Username:        connInfo.User,
		Password:        connInfo.Password,
		SourceTableName: connInfo.TableName,
		TableType:       tableType,
		Columns:         columns,
		RequestId:       requestId,
	}, nil
}

// getDatabaseTypeInfo 根据数据库类型获取表类型和驱动信息
func (s *DorisService) getDatabaseTypeInfo(dbType int32) (tableType, driverURL, driverClass string) {
	switch dbType {
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL):
		return "mysql",
			"file:///opt/apache-doris/driver/mysql-connector-j-8.0.33.jar",
			"com.mysql.cj.jdbc.Driver"
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE):
		return "kingbase8",
			"file:///opt/apache-doris/driver/kingbase8-8.6.0.jar",
			"com.kingbase8.Driver"
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_TIDB):
		return "mysql",
			"file:///opt/apache-doris/driver/mysql-connector-j-8.0.33.jar",
			"com.mysql.cj.jdbc.Driver"
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_TDSQL):
		return "mysql",
			"file:///opt/apache-doris/driver/mysql-connector-j-8.0.33.jar",
			"com.mysql.cj.jdbc.Driver"
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_VASTBASE):
		return "vastbase",
			"file:///opt/apache-doris/driver/vastbase-jdbc-2.9v.jar",
			"cn.com.vastbase.Driver"
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_GBASE):
		return "gbase",
			"file:///opt/apache-doris/driver/gbase-connector-java-9.5.0.8-build1-bin.jar",
			"com.gbase.jdbc.Driver"
	default:
		return "mysql",
			"file:///opt/apache-doris/driver/mysql-connector-j-8.0.33.jar",
			"com.mysql.cj.jdbc.Driver"
	}
}

// buildJdbcURL 构建JDBC连接URL
func (s *DorisService) buildJdbcURL(connInfo *ds.ConnectionInfo, tableType string, requestId string, targetDbName string) string {
	// 基础 JDBC URL
	jdbcURL := fmt.Sprintf("jdbc:%s://%s:%d/%s", tableType, connInfo.Host, connInfo.Port, connInfo.DbName)

	// 检查TLS配置
	if connInfo.TlsConfig != nil && connInfo.TlsConfig.UseTls == 2 {
		log.Logger.Infof("Building JDBC URL with TLS enabled, mode: %d", connInfo.TlsConfig.Mode)
		curDB := targetDbName
		if curDB == "" {
			curDB = common.MIRA_TMP_TASK_DB
		}

		switch tableType {
		case "kingbase8":
			return s.buildKingbaseSSLJdbcURL(jdbcURL, connInfo.TlsConfig, curDB, requestId)
		case "vastbase":
			return s.buildVastbaseSSLJdbcURL(jdbcURL, connInfo.TlsConfig, curDB, requestId)
		case "gbase":
			return s.buildGbasebaseSSLJdbcURL(jdbcURL, connInfo.TlsConfig, curDB, requestId)
		default:
			return s.buildMySQLSSLJdbcURL(jdbcURL, connInfo.TlsConfig, curDB, requestId)
		}
	}

	// 非SSL情况
	switch tableType {
	case "kingbase8", "vastbase":
		return jdbcURL + "?sslmode=disable"
	case "gbase":
		return jdbcURL + "?useSSL=false&useDynamicCharsetInfo=false&defaultFetchSize=-2147483648"
	default:
		return jdbcURL + "?useSSL=false"
	}
}

// buildMySQLSSLJdbcURL 构建MySQL SSL JDBC URL，使用 Doris FILE 引用
func (s *DorisService) buildMySQLSSLJdbcURL(baseURL string, tlsConfig *ds.DatasourceTlsConfig, dbName string, requestId string) string {
	var params []string

	// 启用 SSL
	params = append(params, "useSSL=true")

	// sslMode 映射
	switch tlsConfig.Mode {
	case 1:
		params = append(params, "sslMode=REQUIRED")
	case 2:
		params = append(params, "sslMode=VERIFY_CA")
	case 3:
		params = append(params, "sslMode=VERIFY_IDENTITY")
	default:
		params = append(params, "sslMode=REQUIRED")
	}
	// 打印当前数据库
	s.printCurrentDatabase()

	// 获取 FE 地址和 token
	config := config.GetConfigMap()
	feHost := config.DorisConfig.Address
	fePort := config.DorisConfig.Port

	// 获取 token
	token := common.DORIS_TOKEN

	// 使用 HTTP API 下载小文件
	if tlsConfig.CaCert != "" {
		caFileName := fmt.Sprintf("%s_ca_cert.p12", requestId)
		caFileInfo := s.getFileInfo(dbName, caFileName)
		if caFileInfo != nil {
			caUrl := fmt.Sprintf("http://%s:%d/api/get_small_file?token=%s&file_id=%d",
				feHost, fePort, token, caFileInfo.FileId)
			encoded := strings.ReplaceAll(caUrl, "&", "%26")
			params = append(params,
				fmt.Sprintf("trustCertificateKeyStoreUrl=%s", encoded),
				"trustCertificateKeyStoreType=PKCS12",
				fmt.Sprintf("trustCertificateKeyStorePassword=%s", common.TLS_KEYSTORE_PASSWORD))
			log.Logger.Infof("Using HTTP API for CA certificate: %s", caUrl)
		} else {
			log.Logger.Warnf("CA certificate file not found in database %s: %s", dbName, caFileName)
		}
	}

	if tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "" {
		clientFileName := fmt.Sprintf("%s_client_cert.p12", requestId)
		clientFileInfo := s.getFileInfo(dbName, clientFileName)
		if clientFileInfo != nil {
			clientUrl := fmt.Sprintf("http://%s:%d/api/get_small_file?token=%s&file_id=%d",
				feHost, fePort, token, clientFileInfo.FileId)
			encoded := strings.ReplaceAll(clientUrl, "&", "%26")
			params = append(params,
				fmt.Sprintf("clientCertificateKeyStoreUrl=%s", encoded),
				"clientCertificateKeyStoreType=PKCS12",
				fmt.Sprintf("clientCertificateKeyStorePassword=%s", common.TLS_KEYSTORE_PASSWORD))
			log.Logger.Infof("Using HTTP API for client certificate: %s", clientUrl)
		} else {
			log.Logger.Warnf("Client certificate file not found in database %s: %s", dbName, clientFileName)
		}
	}

	finalURL := baseURL + "?" + strings.Join(params, "&")
	log.Logger.Infof("Generated MySQL SSL JDBC URL with HTTP API: %s", finalURL)
	return finalURL
}

// buildKingbaseSSLJdbcURL 构建KingBase SSL JDBC URL
func (s *DorisService) buildKingbaseSSLJdbcURL(baseURL string, tlsConfig *ds.DatasourceTlsConfig, dbName string, requestId string) string {
	var params []string

	// 启用 SSL
	params = append(params, "ssl=true")

	// sslmode
	var sslmode string
	switch tlsConfig.Mode {
	case 1:
		sslmode = "require"
	case 2:
		sslmode = "verify-ca"
	case 3:
		sslmode = "verify-full"
	default:
		sslmode = "require"
	}
	params = append(params, fmt.Sprintf("sslmode=%s", sslmode))

	// 获取 FE 地址与 token
	conf := config.GetConfigMap()
	feHost, fePort := conf.DorisConfig.Address, conf.DorisConfig.Port
	token := common.DORIS_TOKEN

	needFactory := tlsConfig.CaCert != "" || (tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "")

	if needFactory {
		// 使用自定义 SSL 工厂
		params = append(params, "sslfactory=com.kingbase.ssl.KbHttpSslFactory")

		// 构建 sslfactoryarg 参数
		var sslArgs []string

		// CA 证书（PEM）
		if tlsConfig.CaCert != "" {
			if info := s.getFileInfo(dbName, fmt.Sprintf("%s_ca_cert.pem", requestId)); info != nil {
				u := fmt.Sprintf("http://%s:%d/api/get_small_file?token=%s&file_id=%d", feHost, fePort, token, info.FileId)
				sslArgs = append(sslArgs, fmt.Sprintf("caUrl=%s", strings.ReplaceAll(u, "&", "%26")))
			}
		}

		// 客户端证书（PKCS12）
		if tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "" {
			if info := s.getFileInfo(dbName, fmt.Sprintf("%s_client_cert.p12", requestId)); info != nil {
				u := fmt.Sprintf("http://%s:%d/api/get_small_file?token=%s&file_id=%d", feHost, fePort, token, info.FileId)
				sslArgs = append(sslArgs, fmt.Sprintf("clientP12Url=%s", strings.ReplaceAll(u, "&", "%26")))
				sslArgs = append(sslArgs, fmt.Sprintf("clientP12Password=%s", common.TLS_KEYSTORE_PASSWORD))
			}
		}

		// 先用 & 连接键值，再整体 URL 编码，避免顶层 & 截断
		if len(sslArgs) > 0 {
			raw := strings.Join(sslArgs, "&")
			params = append(params, "sslfactoryarg="+url.QueryEscape(raw))
		}
	}

	finalURL := baseURL + "?" + strings.Join(params, "&")
	log.Logger.Infof("Generated KingBase SSL JDBC URL with custom factory: %s", finalURL)
	return finalURL
}

func (s *DorisService) buildVastbaseSSLJdbcURL(baseURL string, tlsConfig *ds.DatasourceTlsConfig, dbName string, requestId string) string {
	var params []string

	// 启用 SSL
	params = append(params, "ssl=true")

	// sslmode
	var sslmode string
	switch tlsConfig.Mode {
	case 1:
		sslmode = "require"
	case 2:
		sslmode = "verify-ca"
	case 3:
		sslmode = "verify-full"
	default:
		sslmode = "require"
	}
	params = append(params, fmt.Sprintf("sslmode=%s", sslmode))

	// 获取 FE 地址与 token
	conf := config.GetConfigMap()
	feHost, fePort := conf.DorisConfig.Address, conf.DorisConfig.Port
	token := common.DORIS_TOKEN

	needFactory := tlsConfig.CaCert != "" || (tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "")

	if needFactory {
		// 使用自定义 SSL 工厂
		params = append(params, "sslfactory=com.vastbase.ssl.VbHttpSslFactory")

		// 构建 sslfactoryarg 参数
		var sslArgs []string

		// CA 证书（PEM）
		if tlsConfig.CaCert != "" {
			if info := s.getFileInfo(dbName, fmt.Sprintf("%s_ca_cert.pem", requestId)); info != nil {
				u := fmt.Sprintf("http://%s:%d/api/get_small_file?token=%s&file_id=%d", feHost, fePort, token, info.FileId)
				sslArgs = append(sslArgs, fmt.Sprintf("caUrl=%s", strings.ReplaceAll(u, "&", "%26")))
			}
		}

		// 客户端证书（PKCS12）
		if tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "" {
			if info := s.getFileInfo(dbName, fmt.Sprintf("%s_client_cert.p12", requestId)); info != nil {
				u := fmt.Sprintf("http://%s:%d/api/get_small_file?token=%s&file_id=%d", feHost, fePort, token, info.FileId)
				sslArgs = append(sslArgs, fmt.Sprintf("clientP12Url=%s", strings.ReplaceAll(u, "&", "%26")))
				sslArgs = append(sslArgs, fmt.Sprintf("clientP12Password=%s", common.TLS_KEYSTORE_PASSWORD))
			}
		}

		// 先用 & 连接键值，再整体 URL 编码，避免顶层 & 截断
		if len(sslArgs) > 0 {
			raw := strings.Join(sslArgs, "&")
			params = append(params, "sslfactoryarg="+url.QueryEscape(raw))
		}
	}

	finalURL := baseURL + "?" + strings.Join(params, "&")
	log.Logger.Infof("Generated Vastbase SSL JDBC URL with custom factory: %s", finalURL)

	return finalURL
}

// buildGbasebaseSSLJdbcURL 构建GBase SSL JDBC URL，使用 Doris FILE 引用
// GBase JDBC 驱动不支持 sslMode 参数，使用 useSSL/requireSSL/verifyServerCertificate
func (s *DorisService) buildGbasebaseSSLJdbcURL(baseURL string, tlsConfig *ds.DatasourceTlsConfig, dbName string, requestId string) string {
	var params []string

	// 启用 SSL
	params = append(params, "useSSL=true")

	// 根据 TLS mode 设置 SSL 验证级别
	// GBase 不支持 sslMode，使用 requireSSL 和 verifyServerCertificate 替代
	switch tlsConfig.Mode {
	case 1:
		// Mode 1: 仅要求 SSL，不验证证书
		params = append(params, "requireSSL=true")
		params = append(params, "verifyServerCertificate=false")
	case 2:
		// Mode 2: 验证 CA 证书
		params = append(params, "requireSSL=true")
		params = append(params, "verifyServerCertificate=true")
	case 3:
		// Mode 3: 验证完整证书链（包括服务器身份）
		params = append(params, "requireSSL=true")
		params = append(params, "verifyServerCertificate=true")
	default:
		// 默认：仅要求 SSL
		params = append(params, "requireSSL=true")
		params = append(params, "verifyServerCertificate=false")
	}

	// 打印当前数据库
	s.printCurrentDatabase()

	// 获取 FE 地址和 token
	config := config.GetConfigMap()
	feHost := config.DorisConfig.Address
	fePort := config.DorisConfig.Port

	// 获取 token
	token := common.DORIS_TOKEN

	// 使用 HTTP API 下载小文件
	// 注意：GBase 驱动可能不支持 trustCertificateKeyStoreUrl 等参数
	// 如果驱动不支持，这些参数会被忽略，需要通过 JVM 系统属性配置
	if tlsConfig.CaCert != "" {
		caFileName := fmt.Sprintf("%s_ca_cert.p12", requestId)
		caFileInfo := s.getFileInfo(dbName, caFileName)
		if caFileInfo != nil {
			caUrl := fmt.Sprintf("http://%s:%d/api/get_small_file?token=%s&file_id=%d",
				feHost, fePort, token, caFileInfo.FileId)
			encoded := strings.ReplaceAll(caUrl, "&", "%26")
			params = append(params,
				fmt.Sprintf("trustCertificateKeyStoreUrl=%s", encoded),
				"trustCertificateKeyStoreType=PKCS12",
				fmt.Sprintf("trustCertificateKeyStorePassword=%s", common.TLS_KEYSTORE_PASSWORD))
			log.Logger.Infof("Using HTTP API for CA certificate: %s", caUrl)
		} else {
			log.Logger.Warnf("CA certificate file not found in database %s: %s", dbName, caFileName)
		}
	}

	if tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "" {
		clientFileName := fmt.Sprintf("%s_client_cert.p12", requestId)
		clientFileInfo := s.getFileInfo(dbName, clientFileName)
		if clientFileInfo != nil {
			clientUrl := fmt.Sprintf("http://%s:%d/api/get_small_file?token=%s&file_id=%d",
				feHost, fePort, token, clientFileInfo.FileId)
			encoded := strings.ReplaceAll(clientUrl, "&", "%26")
			params = append(params,
				fmt.Sprintf("clientCertificateKeyStoreUrl=%s", encoded),
				"clientCertificateKeyStoreType=PKCS12",
				fmt.Sprintf("clientCertificateKeyStorePassword=%s", common.TLS_KEYSTORE_PASSWORD))
			log.Logger.Infof("Using HTTP API for client certificate: %s", clientUrl)
		} else {
			log.Logger.Warnf("Client certificate file not found in database %s: %s", dbName, clientFileName)
		}
	}

	finalURL := baseURL + "?" + strings.Join(params, "&")
	log.Logger.Infof("Generated GBase SSL JDBC URL with HTTP API: %s", finalURL)
	return finalURL
}

// getTableColumns 获取表结构信息
func (s *DorisService) getTableColumns(connInfo *ds.ConnectionInfo, tableName string) ([]common.ColumnInfo, error) {
	// 如果ConnectionInfo中已经包含了列信息，直接使用
	if len(connInfo.Columns) > 0 {
		var columns []common.ColumnInfo
		for _, col := range connInfo.Columns {
			columns = append(columns, common.ColumnInfo{
				Name:     col.Name,
				DataType: s.convertDatabaseTypeToDorisType(col.DataType, connInfo.Dbtype),
				Nullable: true, // 默认设为可空，实际应该从数据库查询获取
				Default:  "",
			})
		}
		return columns, nil
	}

	return nil, fmt.Errorf("failed to get table columns")
}

// CreateExternalTableFromAsset 根据数据资产信息创建外部表和同名内部表
func (s *DorisService) CreateExternalTableFromAsset(assetName string, chainInfoId string, alias string, jobInstanceId string, targetTableName string, targetDbName string, columnList ...string) ([]string, string, error) {
	// 获取数据资产信息
	assetInfo, err := s.getAssetInfo(assetName, chainInfoId, alias, targetDbName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get asset info: %v", err)
	}

	filteredColumns, err := common.FilterColumnsWithRowidHandling(assetInfo.Columns, columnList)
	if err != nil {
		return nil, "", fmt.Errorf("failed to filter columns with rowid handling: %v", err)
	}

	// 使用过滤后的列信息继续处理
	assetInfo.Columns = filteredColumns

	// 构建外部表配置，表名加external_前缀
	externalTableName := fmt.Sprintf("%s_external", jobInstanceId)
	internalTableName := targetTableName
	resourceName := fmt.Sprintf("%s_resource", jobInstanceId)

	config := ExternalTableConfig{
		ResourceName:    resourceName,
		TableName:       externalTableName,
		JdbcURL:         assetInfo.JdbcURL,
		DriverURL:       assetInfo.DriverURL,
		DriverClass:     assetInfo.DriverClass,
		Username:        assetInfo.Username,
		Password:        assetInfo.Password,
		SourceTableName: assetInfo.SourceTableName,
		TableType:       assetInfo.TableType,
		Columns:         assetInfo.Columns,
	}

	log.Logger.Debugf("config: %+v", config)
	// 导入前清理可能存在的外部表和内部表（重试的情况下）
	currentDb := targetDbName
	if currentDb != "" {
		_ = s.DropTable(currentDb, internalTableName)
		_ = s.DropTable(currentDb, externalTableName)
	} else {
		log.Logger.Warnf("Cannot determine current database, skipping internal and external table cleanup")
	}

	// 创建外部资源和表
	if err := s.CreateExternalResourceAndTables(config); err != nil {
		return nil, "", err
	}

	// 创建内部表
	internalConfig := config
	internalConfig.TableName = internalTableName
	if err := s.createInternalTable(internalConfig); err != nil {
		// 回滚外部表与资源
		_ = s.DropExternalTableAndResource(externalTableName, resourceName)

		// 回滚TLS FILE（根据 tableType 推断 dbType）
		db := targetDbName
		if db != "" && s.isTlsUsed(db, assetInfo.RequestId) {
			var dbType int32
			if assetInfo.TableType == "kingbase8" {
				dbType = int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE)
			} else {
				dbType = int32(ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL)
			}
			s.cleanupTlsFiles(db, assetInfo.RequestId, dbType)
		}

		return nil, "", fmt.Errorf("failed to create internal table: %v", err)
	}

	var columns []string
	for _, col := range config.Columns {
		columns = append(columns, col.Name)
	}

	return columns, assetInfo.RequestId, nil
}

// AssetInfo 数据资产信息
type AssetInfo struct {
	JdbcURL         string
	DriverURL       string
	DriverClass     string
	Username        string
	Password        string
	SourceTableName string
	TableType       string
	Columns         []common.ColumnInfo
	RequestId       string
}

// createInternalTable 创建内部表，结构与外部表一致
func (s *DorisService) createInternalTable(config ExternalTableConfig) error {
	// 构建列定义，添加一个独立的主键字段
	var columnDefs []string

	// 使用UUID生成5位随机字符串作为主键字段名的一部分
	uuidStr := uuid.New().String()
	// 取UUID的前5位字符（去掉连字符）
	randomSuffix := strings.ReplaceAll(uuidStr[:8], "-", "")[:5]
	primaryKeyFieldName := fmt.Sprintf("pk_%s", randomSuffix)
	primaryKeyField := fmt.Sprintf("`%s` BIGINT NOT NULL AUTO_INCREMENT", primaryKeyFieldName)
	columnDefs = append(columnDefs, primaryKeyField)

	// 然后添加原始表的列
	for _, col := range config.Columns {
		columnDef := fmt.Sprintf("`%s` %s", col.Name, col.DataType)
		if !col.Nullable {
			columnDef += " NOT NULL"
		}
		if col.Default != "" {
			columnDef += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		columnDefs = append(columnDefs, columnDef)
	}

	// 构建创建内部表的SQL，使用主键语法
	tableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s
		)
		ENGINE=OLAP
		UNIQUE KEY (`+"`%s`"+`)
		DISTRIBUTED BY HASH(`+"`%s`"+`) BUCKETS AUTO
		PROPERTIES (
			"replication_num" = "1"
		)
	`, config.TableName, strings.Join(columnDefs, ",\n\t\t"), primaryKeyFieldName, primaryKeyFieldName)

	_, err := s.ExecuteUpdate(tableSQL)
	if err != nil {
		return fmt.Errorf("failed to create internal table '%s': %v", config.TableName, err)
	}
	log.Logger.Infof("Created internal table: %s with auto-increment primary key %s", config.TableName, primaryKeyFieldName)
	return nil
}

// CreateExternalAndInternalTableAndImportData 根据数据资产创建external/internal表并导入数据
func (s *DorisService) CreateExternalAndInternalTableAndImportData(assetName string, chainInfoId string, alias string, jobInstanceId string, targetTableName string, targetDbName string, columns ...string) (string, error) {

	columns, reqId, err := s.CreateExternalTableFromAsset(assetName, chainInfoId, alias, jobInstanceId, targetTableName, targetDbName, columns...)
	if err != nil {
		return "", fmt.Errorf("failed to create external/internal table: %v", err)
	}

	// 构建资源名称
	resourceName := fmt.Sprintf("%s_resource", jobInstanceId)

	// 使用defer确保函数结束时删除resource
	defer func() {
		if err := s.dropExternalResource(resourceName); err != nil {
			log.Logger.Warnf("Failed to cleanup resource '%s': %v", resourceName, err)
		} else {
			log.Logger.Infof("Successfully cleaned up resource: %s", resourceName)
		}

		// 再删 TLS 证书 FILE（关键收尾）
		db := targetDbName
		if db == "" {
			log.Logger.Warn("Unable to determine current database for TLS file cleanup")
			return
		}

		// 获取数据库类型（需要从 connInfo 获取）
		connInfo, err := utils.GetDatasourceByAssetName(reqId, assetName, chainInfoId, alias)
		if err != nil {
			log.Logger.Warnf("Failed to get datasource info for cleanup: %v", err)
			return
		}
		// 根据数据库类型判断是否使用 TLS 并清理对应文件
		// 检查是否使用 TLS 并清理
		if connInfo.TlsConfig != nil && connInfo.TlsConfig.UseTls == 2 && s.isTlsUsed(db, reqId) {
			s.cleanupTlsFiles(db, reqId, connInfo.Dbtype)
		}
	}()

	// 2. 构造表名

	// 构建外部表配置，表名加external_前缀和随机后缀
	externalTableName := fmt.Sprintf("%s_external", jobInstanceId)
	internalTableName := targetTableName

	var qualifiedInternalTable, qualifiedExternalTable string
	if targetDbName != "" {
		qualifiedInternalTable = fmt.Sprintf("`%s`.`%s`", targetDbName, internalTableName)
		qualifiedExternalTable = fmt.Sprintf("`%s`.`%s`", targetDbName, externalTableName)
	} else {
		qualifiedInternalTable = internalTableName
		qualifiedExternalTable = externalTableName
	}

	// 3. 执行数据导入
	columnList := strings.Join(columns, ", ")
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
		qualifiedInternalTable, columnList, columnList, qualifiedExternalTable)
	_, err = s.ExecuteUpdate(insertSQL)
	if err != nil {
		assetInfo, assetErr := s.getAssetInfo(assetName, chainInfoId, alias, targetDbName)
		if assetErr == nil {
			isConnError := LogSourceConnectionError(err, SourceConnLogCtx{
				TableType:   assetInfo.TableType,
				JdbcURL:     assetInfo.JdbcURL,
				DriverURL:   assetInfo.DriverURL,
				DriverClass: assetInfo.DriverClass,
				Username:    assetInfo.Username,
			})

			if isConnError {
				return "", fmt.Errorf("failed to import data from external to internal table (connection error): %v", err)
			}
		}
		return "", fmt.Errorf("failed to import data from external to internal table: %v", err)
	}

	return internalTableName, nil
}

// CreateExternalAndInternalTableAndImportDataBatched
// 1) 创建外部/内部表
// 2) 在 Doris 上对外部表查询 MIN/MAX(pk)
// 3) 以 pk 的数值区间按 batchSize 批量 INSERT INTO ... SELECT ... WHERE pk BETWEEN ? AND ?
// 4) 每个批次失败自动重试（默认 3 次）
// 返回内部表表名
func (s *DorisService) CreateExternalAndInternalTableAndImportDataBatched(
	assetName string,
	chainInfoId string,
	alias string,
	jobInstanceId string,
	targetTableName string,
	targetDbName string,
	pkColumn string,
	batchSize int,
	columns ...string,
) (string, error) {
	// 确保 pkColumn 在列列表中（已存在则不重复）
	colSet := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		colSet[c] = struct{}{}
	}
	if pkColumn != "" {
		if _, ok := colSet[pkColumn]; !ok {
			columns = append(columns, pkColumn)
		}
	}

	columns, reqId, err := s.CreateExternalTableFromAsset(assetName, chainInfoId, alias, jobInstanceId, targetTableName, targetDbName, columns...)
	if err != nil {
		return "", fmt.Errorf("failed to create external/internal table: %v", err)
	}

	// 与非分批方法相同的资源清理逻辑
	resourceName := fmt.Sprintf("%s_resource", jobInstanceId)
	defer func() {
		if err := s.dropExternalResource(resourceName); err != nil {
			log.Logger.Warnf("Failed to cleanup resource '%s': %v", resourceName, err)
		} else {
			log.Logger.Infof("Successfully cleaned up resource: %s", resourceName)
		}

		db := targetDbName
		if db == "" {
			log.Logger.Warn("Unable to determine current database for TLS file cleanup")
			return
		}
		connInfo, err := utils.GetDatasourceByAssetName(reqId, assetName, chainInfoId, alias)
		if err != nil {
			log.Logger.Warnf("Failed to get datasource info for cleanup: %v", err)
			return
		}
		if connInfo.TlsConfig != nil && connInfo.TlsConfig.UseTls == 2 && s.isTlsUsed(db, reqId) {
			s.cleanupTlsFiles(db, reqId, connInfo.Dbtype)
		}
	}()

	externalTableName := fmt.Sprintf("%s_external", jobInstanceId)
	internalTableName := targetTableName

	// 计算 MIN/MAX(pk) - 直接连接源数据库查询
	connInfo, err := utils.GetDatasourceByAssetName(reqId, assetName, chainInfoId, alias)
	if err != nil {
		return "", fmt.Errorf("failed to get datasource info: %v", err)
	}

	// 根据数据库类型创建对应的 strategy
	dbType := utils.ConvertDataSourceType(connInfo.Dbtype)
	dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
	if err != nil {
		return "", fmt.Errorf("failed to create database strategy: %v", err)
	}

	// 连接到源数据库
	err = dbStrategy.ConnectToDBWithPass(connInfo)
	if err != nil {
		log.Logger.Errorf("failed to connect to source database: %v", err)
		return "", fmt.Errorf("failed to connect to source database: %v", err)
	}
	// 构造查询 MIN/MAX 的 SQL
	// 根据数据库类型选择正确的标识符引用方式
	var quotedPk string
	if dbType == ds.DataSourceType_DATA_SOURCE_TYPE_VASTBASE ||
		dbType == ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
		// Vastbase 和 Kingbase 不加引号
		quotedPk = pkColumn
	} else {
		// MySQL 等其他数据库使用反引号
		quotedPk = fmt.Sprintf("`%s`", pkColumn)
	}
	minMaxSQL := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quotedPk, quotedPk, connInfo.TableName)

	// 执行查询
	var minID, maxID sql.NullInt64
	db := database.GetDB(dbStrategy)
	if db == nil {
		return "", fmt.Errorf("failed to get database connection from strategy")
	}

	err = db.QueryRow(minMaxSQL).Scan(&minID, &maxID)
	if err != nil {
		if err == sql.ErrNoRows {
			// 没有数据，直接返回空表
			log.Logger.Warnf("no data in source table: %v", err)
			return internalTableName, nil
		}
		log.Logger.Errorf("failed to query MIN/MAX on source table: %v", err)
		return "", fmt.Errorf("failed to query MIN/MAX on source table: %v", err)
	}

	// 打印最大值和最小值
	if minID.Valid && maxID.Valid {
		log.Logger.Infof("MIN(%s) = %d, MAX(%s) = %d", pkColumn, minID.Int64, pkColumn, maxID.Int64)
	} else {
		log.Logger.Warnf("MIN/MAX values are invalid: minID.Valid=%v, maxID.Valid=%v", minID.Valid, maxID.Valid)
	}

	if !minID.Valid || !maxID.Valid {
		// 值无效，退化为一次性导入
		columnList := strings.Join(columns, ", ")
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", internalTableName, columnList, columnList, externalTableName)
		if _, err := s.ExecuteUpdate(insertSQL); err != nil {
			return "", fmt.Errorf("failed to import (fallback single insert): %v", err)
		}
		return internalTableName, nil
	}

	// 分批循环（按主键范围近似 5000 条一批）
	columnList := strings.Join(columns, ", ")
	maxRetry := config.GetConfigMap().DorisConfig.ImportMaxRetry
	if maxRetry <= 0 {
		maxRetry = 3
	}
	for start := minID.Int64; start <= maxID.Int64; start += int64(batchSize) {
		end := start + int64(batchSize) - 1
		insertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s BETWEEN %d AND %d",
			internalTableName, columnList, columnList, externalTableName, quotedPk, start, end,
		)

		var lastErr error
		for attempt := 1; attempt <= maxRetry; attempt++ {
			_, lastErr = s.ExecuteUpdate(insertSQL)
			if lastErr == nil {
				break
			}
			log.Logger.Warnf("Batch import failed (attempt %d/%d) for range [%d, %d]: %v", attempt, maxRetry, start, end, lastErr)
		}
		if lastErr != nil {
			return "", fmt.Errorf("failed to import batch range [%d, %d] after %d attempts: %v", start, end, maxRetry, lastErr)
		}
	}

	return internalTableName, nil
}

// ImportArrowFileToDoris 导入Arrow格式文件到Doris
func (s *DorisService) ImportArrowFileToDoris(bucketName string, objectName string, tableName string, dbName string) error {
	// 使用项目中定义的DATA_DIR常量
	dataDir := common.DATA_DIR

	// 生成唯一的文件名
	fileName := fmt.Sprintf("arrow_import_%s.arrow", uuid.New().String())
	filePath := filepath.Join(dataDir, fileName)

	// 创建文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer func() {
		file.Close()
		// 使用完后删除文件
		if err := os.Remove(filePath); err != nil {
			log.Logger.Warnf("Failed to remove file %s: %v", filePath, err)
		}
	}()

	// 获取OSS客户端
	ossFactory := oss.NewOSSFactory(config.GetConfigMap())
	ossClient, err := ossFactory.NewOSSClient()
	if err != nil {
		return fmt.Errorf("failed to create OSS client: %v", err)
	}

	// 从Minio下载文件
	object, err := ossClient.GetObject(context.Background(), bucketName, objectName, &oss.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object from MinIO: %v", err)
	}
	defer object.Close()

	// 将文件内容写入data目录下的文件
	if _, err := io.Copy(file, object); err != nil {
		return fmt.Errorf("failed to write object to file: %v", err)
	}

	// 关闭文件以确保内容写入磁盘
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %v", err)
	}

	// 读取Arrow文件并获取schema
	schema, err := s.readArrowFileSchema(filePath)
	if err != nil {
		return fmt.Errorf("failed to read Arrow file schema: %v", err)
	}

	// 创建Doris表（如果不存在）
	if err := s.createDorisTableFromArrowSchema(dbName, tableName, schema); err != nil {
		return fmt.Errorf("failed to create Doris table: %v", err)
	}

	// 使用doris-streamloader导入Arrow文件
	if err := s.importArrowFileWithStreamloader(filePath, dbName, tableName); err != nil {
		return fmt.Errorf("failed to import Arrow file: %v", err)
	}

	log.Logger.Infof("Successfully imported Arrow file to Doris table %s.%s", dbName, tableName)
	return nil
}

// readArrowFileSchema 读取Arrow文件的schema
func (s *DorisService) readArrowFileSchema(filePath string) (*arrow.Schema, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open Arrow file: %v", err)
	}
	defer file.Close()

	// 使用Arrow的FileReader读取文件
	reader, err := ipc.NewFileReader(file, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow file reader: %v", err)
	}

	schema := reader.Schema()
	log.Logger.Infof("Read Arrow schema with %d fields", len(schema.Fields()))
	return schema, nil
}

// createDorisTableFromArrowSchema 根据Arrow schema创建Doris表
func (s *DorisService) createDorisTableFromArrowSchema(dbName, tableName string, schema *arrow.Schema) error {
	// 构建列定义
	var columnDefs []string
	for _, field := range schema.Fields() {
		dorisType := s.convertArrowTypeToDorisType(field.Type)
		columnDef := fmt.Sprintf("`%s` %s", field.Name, dorisType)
		columnDefs = append(columnDefs, columnDef)
	}

	// 构建CREATE TABLE SQL
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			%s
		)
		ENGINE=OLAP
		DISTRIBUTED BY HASH(`+"`%s`"+`) BUCKETS AUTO
		PROPERTIES (
			"replication_num" = "1"
		)
	`, dbName, tableName, strings.Join(columnDefs, ",\n\t\t"), schema.Field(0).Name)

	// 执行CREATE TABLE
	_, err := s.ExecuteUpdate(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create Doris table: %v", err)
	}

	log.Logger.Infof("Created Doris table %s.%s", dbName, tableName)
	return nil
}

// convertArrowTypeToDorisType 将Arrow数据类型转换为Doris数据类型
func (s *DorisService) convertArrowTypeToDorisType(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INT"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "TINYINT UNSIGNED"
	case arrow.UINT16:
		return "SMALLINT UNSIGNED"
	case arrow.UINT32:
		return "INT UNSIGNED"
	case arrow.UINT64:
		return "BIGINT UNSIGNED"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING:
		return "VARCHAR(65533)"
	case arrow.LARGE_STRING:
		return "VARCHAR(65533)"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.DATE32:
		return "DATE"
	case arrow.DATE64:
		return "DATETIME"
	case arrow.TIMESTAMP:
		return "DATETIME"
	case arrow.DECIMAL128:
		decimalType := arrowType.(*arrow.Decimal128Type)
		return fmt.Sprintf("DECIMAL(%d,%d)", decimalType.Precision, decimalType.Scale)
	case arrow.DECIMAL256:
		decimalType := arrowType.(*arrow.Decimal256Type)
		return fmt.Sprintf("DECIMAL(%d,%d)", decimalType.Precision, decimalType.Scale)
	default:
		return "VARCHAR(65533)" // 默认使用VARCHAR
	}
}

// importArrowFileWithStreamloader 使用doris-streamloader导入Arrow文件
func (s *DorisService) importArrowFileWithStreamloader(filePath, dbName, tableName string) error {
	// 构建doris-streamloader命令参数
	config := config.GetConfigMap()
	args := []string{
		"--source_file", filePath,
		"--u", config.DorisConfig.User,
		"--p", config.DorisConfig.Password,
		"--url", fmt.Sprintf("http://%s:%d", config.DorisConfig.Address, config.DorisConfig.Port),
		"--db", dbName,
		"--table", tableName,
		"--workers", "0", // 0表示自动选择最佳线程数
		"--format", "arrow", // 指定格式为Arrow
	}

	// 执行doris-streamloader命令
	cmd := exec.Command("/home/workspace/bin/doris-streamloader", args...)

	log.Logger.Infof("Executing doris-streamloader command for Arrow file: %s %s",
		"/home/workspace/bin/doris-streamloader",
		strings.Join(args, " "))

	// 捕获输出
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// 执行命令
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("doris-streamloader failed: %v, stderr: %s", err, stderr.String())
	}

	log.Logger.Infof("doris-streamloader output: %s", stdout.String())
	return nil
}

// ImportCsvFileToDoris 使用doris-streamloader导入CSV文件到Doris
func (s *DorisService) ImportCsvFileToDoris(request *ds.ImportCsvFileToDorisRequest) error {
	// 使用项目中定义的DATA_DIR常量
	dataDir := common.DATA_DIR

	// 生成唯一的文件名
	fileName := fmt.Sprintf("csv_import_%s.csv", uuid.New().String())
	filePath := filepath.Join(dataDir, fileName)

	// 创建文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer func() {
		file.Close()
		// 使用完后删除文件
		if err := os.Remove(filePath); err != nil {
			log.Logger.Warnf("Failed to remove file %s: %v", filePath, err)
		}
	}()

	// 获取OSS客户端
	ossFactory := oss.NewOSSFactory(config.GetConfigMap())
	ossClient, err := ossFactory.NewOSSClient()
	if err != nil {
		return fmt.Errorf("failed to create OSS client: %v", err)
	}

	// 从Minio下载文件
	object, err := ossClient.GetObject(context.Background(), request.BucketName, request.ObjectName, &oss.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object from MinIO: %v", err)
	}
	defer object.Close()

	// 将文件内容写入data目录下的文件
	if _, err := io.Copy(file, object); err != nil {
		return fmt.Errorf("failed to write object to file: %v", err)
	}

	// 关闭文件以确保内容写入磁盘
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %v", err)
	}

	// 构建doris-streamloader命令参数
	config := config.GetConfigMap()
	args := []string{
		"--source_file", filePath,
		"--u", config.DorisConfig.User,
		"--p", config.DorisConfig.Password,
		"--url", fmt.Sprintf("http://%s:%d", config.DorisConfig.Address, config.DorisConfig.Port),
		"--db", request.DbName,
		"--table", request.TableName,
		"--workers", "0", // 0表示自动选择最佳线程数
	}

	// 构建header参数
	headerParts := []string{}
	if request.ColumnSeparator != "" {
		headerParts = append(headerParts, fmt.Sprintf("column_separator:%s", request.ColumnSeparator))
	}
	if len(request.Columns) > 0 {
		headerParts = append(headerParts, fmt.Sprintf("columns:%s", strings.Join(request.Columns, ",")))
	}
	if len(headerParts) > 0 {
		args = append(args, "--header", strings.Join(headerParts, "?"))
	}

	// 根据操作系统架构选择正确的doris-streamloader路径
	streamloaderPath := common.GetDorisStreamloaderPath()
	cmd := exec.Command(streamloaderPath, args...)

	log.Logger.Infof("Executing doris-streamloader command: %s %s",
		streamloaderPath,
		strings.Join(args, " "))

	// 捕获输出
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// 执行命令
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("doris-streamloader failed: %v, stderr: %s", err, stderr.String())
	}

	log.Logger.Infof("doris-streamloader output: %s", stdout.String())
	if err := checkStreamloaderOutput(stdout.String()); err != nil {
		return err
	}
	return nil
}

func (s *DorisService) ExportParquetFileFromDoris(request *ds.ExportCsvFileFromDorisRequest) error {
	// log.Logger.Debugf("ExportCsvFileFromDoris request: %v", request)
	// // 获取配置信息
	// conf := config.GetConfigMap()
	// sqlGenerator := &database.SQLGenerator{}
	// // 构建EXPORT SQL语句（带排序/过滤或列很多时走 SELECT ... INTO OUTFILE）
	// var exportSQL string
	// randomSuffix, err := common.GenerateRandomString(8)
	// if err != nil {
	// 	return fmt.Errorf("failed to generate random suffix: %v", err)
	// }
	// labelName := randomSuffix

	// hasAdvanced := len(request.SortRules) > 0 || len(request.FilterConditions) > 0
	// shouldUseSelect := hasAdvanced || len(request.Columns) > 30

	// if shouldUseSelect {
	// 	exportSQL = sqlGenerator.BuildSelectIntoOutfileSQL(request, conf)
	// } else {
	// 	exportSQL = sqlGenerator.BuildExportSQL(request, conf, labelName)
	// }

	// log.Logger.Infof("Executing export SQL: %s", exportSQL)

	// // 执行导出
	// _, err = s.ExecuteSQL(exportSQL)
	// if err != nil {
	// 	return fmt.Errorf("failed to execute export SQL: %v", err)
	// }

	// // 仅 EXPORT TABLE 需要追踪任务
	// if !shouldUseSelect {
	// 	err = s.trackExportTaskStatusByJobId(request.DbName, labelName)
	// 	if err != nil {
	// 		return fmt.Errorf("export task failed: %v", err)
	// 	}
	// }

	// log.Logger.Infof("Successfully exported CSV file from Doris table %s.%s to MinIO",
	// 	request.DbName, request.TableName)
	// return nil

	// Validate request parameters
	if request == nil {
		return fmt.Errorf("request cannot be nil")
	}
	if request.DbName == "" {
		log.Logger.Errorf("DbName is empty in export request")
		return fmt.Errorf("export failed: DbName is empty")
	}
	if request.TableName == "" {
		log.Logger.Errorf("TableName is empty in export request, DbName: %s", request.DbName)
		return fmt.Errorf("export failed: TableName is empty")
	}
	log.Logger.Debugf("ExportParquetFileFromDoris request: %v", request)
	conf := config.GetConfigMap()
	sqlGenerator := &database.SQLGenerator{}

	// 统一使用 SELECT ... INTO OUTFILE
	exportSQL := sqlGenerator.BuildSelectIntoOutfileSQL(request, conf)
	log.Logger.Infof("Executing export SQL: %s", exportSQL)

	// Execute with retry mechanism
	maxRetries := 5
	baseDelay := 1 * time.Second
	err := utils.WithRetry(maxRetries, baseDelay, func() error {
		_, _, err := s.ExecuteSQL(exportSQL)
		return err
	}, utils.IsRetryableNetErr)
	if err != nil {
		return fmt.Errorf("failed to execute export SQL after %d retries: %v", maxRetries+1, err)
	}

	log.Logger.Infof("Successfully exported Parquet file from Doris table %s.%s to MinIO",
		request.DbName, request.TableName)
	return nil
}

// ExportDorisDataToMiraDB 导出Doris数据到Mira数据库
func (s *DorisService) ExportDorisDataToMiraDB(request *ds.ExportDorisDataToMiraDBRequest) error {
	// 1. 在mira_tmp_task数据库中创建中间表
	if request == nil {
		return fmt.Errorf("request cannot be nil")
	}

	schema, err := s.ConvertRequestToArrowSchema(request)
	if err != nil {
		return fmt.Errorf("failed to convert request to Arrow schema: %v", err)
	}

	log.Logger.Infof("Successfully converted request to Arrow schema for table: %s", request.TableName)

	connInfo, dbType, err := s.buildDeployedDBConnectionInfo(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return fmt.Errorf("failed to build connection info: %v", err)
	}

	if err := s.createTableByDeployedDBType(request.TableName, schema, connInfo, dbType); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	resourceName, err := GetGlobalResourceName()
	if err != nil {
		return fmt.Errorf("failed to get global resource name: %v", err)
	}

	columnInfos := s.convertColumnsToColumnInfo(request.Columns)

	// 构建外部表配置
	uuidStr := strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
	config := s.buildExternalTableConfig(
		request.TableName+"_"+uuidStr+"_external",
		resourceName,
		connInfo,
		columnInfos,
		request.TableName,
	)

	// 创建外表
	if err := s.createExternalTable(config); err != nil {
		return fmt.Errorf("failed to create external table: %v", err)
	}

	columnList := s.buildColumnList(request.Columns)

	// 导入数据到外表，明确指定字段名
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
		request.TableName+"_"+uuidStr+"_external", columnList, columnList, request.SourceTableName)
	_, err = s.ExecuteUpdate(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert data from external to internal table: %v", err)
	}

	return nil
}

func (s *DorisService) ImportMiraDBDataToDoris(request *ds.ImportMiraDBDataToDorisRequest) (string, error) {
	// 在doris创建外表
	resourceName, err := GetGlobalResourceName()
	if err != nil {
		return "", fmt.Errorf("failed to get global resource name: %v", err)
	}

	// 使用重构后的方法构建连接信息
	connInfo, _, err := s.buildDeployedDBConnectionInfo(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return "", fmt.Errorf("failed to build connection info: %v", err)
	}

	// 源表字段信息
	sourceTableColumns, err := s.getMiraTableSchema(request.JobInstanceId + "_" + request.MiraTableName)
	if err != nil {
		return "", fmt.Errorf("failed to get mira table schema: %v", err)
	}

	// 转换列信息
	columnInfos := s.convertColumnsToColumnInfo(sourceTableColumns)

	externalTableName := request.JobInstanceId + "_" + request.MiraTableName + "_external"
	internalTableName := request.JobInstanceId + "_" + request.MiraTableName + "_internal"

	// 使用重构后的方法构建外部表配置
	config := s.buildExternalTableConfig(
		externalTableName,
		resourceName,
		connInfo,
		columnInfos,
		request.JobInstanceId+"_"+request.MiraTableName,
	)

	if err := s.createExternalTable(config); err != nil {
		return "", fmt.Errorf("failed to create external table: %v", err)
	}

	// 创建内部表
	internalConfig := config
	internalConfig.TableName = internalTableName
	if err := s.createInternalTable(internalConfig); err != nil {
		return "", fmt.Errorf("failed to create internal table: %v", err)
	}

	// 导入数据到内部表
	columnList := s.buildColumnList(sourceTableColumns)
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
		internalTableName, columnList, columnList, externalTableName)
	log.Logger.Infof("Executing insert SQL: %s", insertSQL)
	_, err = s.ExecuteUpdate(insertSQL)
	if err != nil {
		return "", fmt.Errorf("failed to insert data from mira to doris table: %v", err)
	}

	return internalTableName, nil
}

// createTableByDeployedDBType 根据部署的数据库类型创建表
func (s *DorisService) createTableByDeployedDBType(tableName string, schema *arrow.Schema, connInfo *ds.ConnectionInfo, dbType ds.DataSourceType) error {

	// 使用数据库工厂创建策略
	dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
	if err != nil {
		return fmt.Errorf("failed to create database strategy: %v", err)
	}

	// 连接到数据库
	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	// 使用CreateTemporaryTableIfNotExists创建表
	if err := dbStrategy.CreateTemporaryTableIfNotExists(tableName, schema); err != nil {
		return fmt.Errorf("failed to create table '%s': %v", tableName, err)
	}

	log.Logger.Infof("Successfully created table '%s'", tableName)
	return nil
}

// ConvertRequestToArrowSchema 将ExportDorisDataToMiraDBRequest中的字段信息转换为Arrow Schema
func (s *DorisService) ConvertRequestToArrowSchema(request *ds.ExportDorisDataToMiraDBRequest) (*arrow.Schema, error) {
	if request == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	if len(request.Columns) == 0 {
		return nil, fmt.Errorf("no columns specified in request")
	}

	// 打印request.Columns的详细信息
	log.Logger.Infof("=== ConvertRequestToArrowSchema Debug Info ===")
	log.Logger.Infof("Total columns: %d", len(request.Columns))
	for i, column := range request.Columns {
		log.Logger.Infof("Column[%d]: Name='%s', DataType='%s'",
			i, column.Name, column.DataType)
	}
	log.Logger.Infof("=== End Debug Info ===")

	var fields []arrow.Field

	for _, column := range request.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf("column name cannot be empty")
		}

		// 将数据库数据类型转换为Arrow数据类型
		arrowType, err := s.convertDataTypeToArrowType(column.DataType)
		if err != nil {
			return nil, fmt.Errorf("failed to convert data type for column '%s': %v", column.Name, err)
		}

		field := arrow.Field{
			Name: column.Name,
			Type: arrowType,
		}

		fields = append(fields, field)
	}

	schema := arrow.NewSchema(fields, nil)
	log.Logger.Infof("Converted request to Arrow schema with %d columns", len(fields))

	return schema, nil
}

func (s *DorisService) convertDataTypeToArrowType(dataType string) (arrow.DataType, error) {
	// 获取系统配置以确定数据库类型
	conf := config.GetConfigMap()
	dbTypeName := conf.Dbms.Type
	dbType := utils.ConvertDBType(dbTypeName)

	// 根据数据库类型调用相应的转换函数
	switch dbType {
	case ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
		ds.DataSourceType_DATA_SOURCE_TYPE_TDSQL,
		ds.DataSourceType_DATA_SOURCE_TYPE_TIDB:
		return common.ConvertDorisTypeToArrowType(dataType)
	case ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE:
		return s.convertKingbaseTypeToArrowType(dataType)
	default:
		// 默认使用MySQL的转换逻辑
		return common.ConvertDorisTypeToArrowType(dataType)
	}
}

// convertKingbaseTypeToArrowType 复用Kingbase的类型转换逻辑
func (s *DorisService) convertKingbaseTypeToArrowType(dataType string) (arrow.DataType, error) {
	// 转换为大写以匹配kingbaseTypeToArrowType的逻辑
	dbTypeName := strings.ToUpper(strings.TrimSpace(dataType))

	// 处理浮点数类型
	if strings.Contains(dbTypeName, "FLOAT") || strings.Contains(dbTypeName, "DOUBLE") {
		return arrow.PrimitiveTypes.Float64, nil
	}

	if dbTypeName == "REAL" {
		return arrow.PrimitiveTypes.Float32, nil
	}

	if strings.Contains(dbTypeName, "DECIMAL") || strings.Contains(dbTypeName, "NUMERIC") {
		// 使用默认的精度和标度
		return &arrow.Decimal128Type{Precision: 38, Scale: 10}, nil
	}

	// 处理 INT 和无符号类型
	switch {
	case strings.Contains(dbTypeName, "TINYINT"):
		return arrow.PrimitiveTypes.Int8, nil
	case strings.Contains(dbTypeName, "SMALLINT"):
		return arrow.PrimitiveTypes.Int16, nil
	case strings.Contains(dbTypeName, "MEDIUMINT"):
		return arrow.PrimitiveTypes.Int32, nil
	case strings.Contains(dbTypeName, "BIGINT"):
		return arrow.PrimitiveTypes.Int64, nil
	case strings.Contains(dbTypeName, "INT"), dbTypeName == "INTEGER":
		return arrow.PrimitiveTypes.Int32, nil
	case strings.Contains(dbTypeName, "UNSIGNED TINYINT"):
		return arrow.PrimitiveTypes.Uint8, nil
	case strings.Contains(dbTypeName, "UNSIGNED SMALLINT"):
		return arrow.PrimitiveTypes.Uint16, nil
	case strings.Contains(dbTypeName, "UNSIGNED MEDIUMINT"), strings.Contains(dbTypeName, "UNSIGNED INT"):
		return arrow.PrimitiveTypes.Uint32, nil
	case strings.Contains(dbTypeName, "UNSIGNED BIGINT"):
		return arrow.PrimitiveTypes.Uint64, nil
	}

	// 处理 CHAR 或 TEXT 类型
	if strings.Contains(dbTypeName, "CHAR") {
		return arrow.BinaryTypes.String, nil
	}

	if strings.Contains(dbTypeName, "TEXT") {
		return arrow.BinaryTypes.LargeString, nil
	}

	// 处理 TIME 类型
	if dbTypeName == "TIME" {
		return arrow.BinaryTypes.String, nil
	}

	// 处理 YEAR 类型
	if dbTypeName == "YEAR" {
		return arrow.PrimitiveTypes.Int64, nil
	}

	// 处理 DATE 类型
	if dbTypeName == "DATE" {
		return arrow.BinaryTypes.String, nil
	}

	// 处理 TIMESTAMP 类型
	if dbTypeName == "TIMESTAMP" {
		return arrow.BinaryTypes.String, nil
	}

	// 处理其他类型
	return arrow.BinaryTypes.String, nil
}

func GetGlobalResourceName() (string, error) {
	if globalResourceName == "" {
		return "", fmt.Errorf("global resource not initialized")
	}
	return globalResourceName, nil
}

func (s *DorisService) InitGlobalResource() error {
	var err error
	resourceInitOnce.Do(func() {

		conf := config.GetConfigMap()
		dbTypeName := conf.Dbms.Type
		dbType := utils.ConvertDBType(dbTypeName)
		conf = config.GetConfigMap()
		connInfo := &ds.ConnectionInfo{
			Host:     conf.Dbms.Host,
			Port:     conf.Dbms.Port,
			User:     conf.Dbms.User,
			Password: conf.Dbms.Password,
			DbName:   common.MIRA_TMP_TASK_DB,
			Dbtype:   int32(dbType),
		}
		resourceName := "mira_system_global_resource"
		exists, checkErr := s.CheckResourceExists(resourceName)
		if checkErr != nil {
			err = fmt.Errorf("failed to check if resource exists: %v", checkErr)
			return
		}
		if exists {
			log.Logger.Infof("Resource '%s' already exists, dropping it before re-creating", resourceName)
			_ = s.dropExternalResource(resourceName) // 忽略drop错误，保证后续能创建
		}
		tableType, driverURL, driverClass := s.getDatabaseTypeInfo(connInfo.Dbtype)
		jdbcUrl := s.buildJdbcURL(connInfo, tableType, uuid.New().String(), common.MIRA_TMP_TASK_DB)
		config := ExternalTableConfig{
			TableType:    tableType,
			JdbcURL:      jdbcUrl,
			DriverURL:    driverURL,
			DriverClass:  driverClass,
			Username:     connInfo.User,
			Password:     connInfo.Password,
			ResourceName: resourceName,
		}
		if createErr := s.createExternalResource(config); createErr != nil {
			err = fmt.Errorf("failed to create external resource: %v", createErr)
			return
		}
		globalResourceName = resourceName
		log.Logger.Infof("Successfully initialized Doris resource: %s", resourceName)
	})
	return err
}

// CheckResourceExists 检查资源是否存在
func (s *DorisService) CheckResourceExists(resourceName string) (bool, error) {
	query := fmt.Sprintf("SHOW RESOURCES WHERE NAME = '%s'", resourceName)
	rows, done, err := s.ExecuteSQL(query)
	if err != nil {
		return false, fmt.Errorf("failed to query resource: %v", err)
	}
	if rows == nil {
		return false, nil
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}
	return rows.Next(), nil
}

// buildColumnList 构建列名字符串列表
func (s *DorisService) buildColumnList(columns []*ds.ColumnItem) string {
	var columnNames []string
	for _, column := range columns {
		columnNames = append(columnNames, column.Name)
	}
	return strings.Join(columnNames, ", ")
}

// buildDeployedDBConnectionInfo 构建部署数据库的连接信息
func (s *DorisService) buildDeployedDBConnectionInfo(dbName string) (*ds.ConnectionInfo, ds.DataSourceType, error) {
	// 获取系统配置
	conf := config.GetConfigMap()

	// 获取部署的数据库类型
	dbTypeName := conf.Dbms.Type
	dbType := utils.ConvertDBType(dbTypeName)

	// 构建连接信息
	connInfo := &ds.ConnectionInfo{
		Host:     conf.Dbms.Host,
		Port:     conf.Dbms.Port,
		User:     conf.Dbms.User,
		Password: conf.Dbms.Password,
		DbName:   dbName,
		Dbtype:   int32(dbType),
	}

	return connInfo, dbType, nil
}

// buildExternalTableConfig 构建外部表配置
func (s *DorisService) buildExternalTableConfig(tableName, resourceName string, connInfo *ds.ConnectionInfo, columns []common.ColumnInfo, sourceTableName string) ExternalTableConfig {
	tableType, driverURL, driverClass := s.getDatabaseTypeInfo(connInfo.Dbtype)
	// 如果 Kingbase 且启用 TLS，则追加自定义 SSL 工厂 fat jar
	if (tableType == "kingbase8") && connInfo.TlsConfig != nil && connInfo.TlsConfig.UseTls == 2 {
		driverURL = "file:///opt/apache-doris/driver/kingbase-ssl-factory-fat-1.0-SNAPSHOT.jar"
	}
	// todo vastbase是不是要特殊处理
	if tableType == "vastbase" {
		driverURL = "file:///opt/apache-doris/driver/vastbase-ssl-factory-fat-1.0-SNAPSHOT.jar"
	}
	jdbcUrl := s.buildJdbcURL(connInfo, tableType, uuid.New().String(), common.MIRA_TMP_TASK_DB)

	return ExternalTableConfig{
		TableName:       tableName,
		TableType:       tableType,
		JdbcURL:         jdbcUrl,
		DriverURL:       driverURL,
		DriverClass:     driverClass,
		Username:        connInfo.User,
		Password:        connInfo.Password,
		ResourceName:    resourceName,
		Columns:         columns,
		SourceTableName: sourceTableName,
	}
}

// convertColumnsToColumnInfo 将请求中的列信息转换为ColumnInfo
func (s *DorisService) convertColumnsToColumnInfo(columns []*ds.ColumnItem) []common.ColumnInfo {
	var columnInfos []common.ColumnInfo
	for _, col := range columns {
		columnInfos = append(columnInfos, common.ColumnInfo{
			Name:     col.Name,
			DataType: col.DataType,
			Nullable: true, // 可根据实际情况设置
		})
	}
	return columnInfos
}

// getMiraTableSchema 获取Mira数据库表的字段信息
func (s *DorisService) getMiraTableSchema(tableName string) ([]*ds.ColumnItem, error) {
	// 构建连接信息
	connInfo, dbType, err := s.buildDeployedDBConnectionInfo(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return nil, fmt.Errorf("failed to build connection info: %v", err)
	}

	// 使用数据库工厂创建策略
	dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create database strategy: %v", err)
	}

	// 连接到数据库
	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	// 使用GetTableInfo方法获取表信息，包括列结构
	var tableInfo *ds.TableInfoResponse
	switch dbType {
	case ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
		ds.DataSourceType_DATA_SOURCE_TYPE_TDSQL,
		ds.DataSourceType_DATA_SOURCE_TYPE_TIDB:
		tableInfo, err = dbStrategy.GetTableInfo(common.MIRA_TMP_TASK_DB, tableName, false)
	case ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE:
		tableInfo, err = dbStrategy.GetTableInfo("public", tableName, false)
	default:
		return nil, fmt.Errorf("unsupported database type: %v", dbType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %v", err)
	}

	if len(tableInfo.Columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", tableName)
	}

	log.Logger.Infof("Retrieved %d columns for Mira table %s", len(tableInfo.Columns), tableName)
	return tableInfo.Columns, nil
}

func (s *DorisService) CleanDorisTableWithPrefix(prefix string) error {

	// 使用您提供的SQL模式生成DROP语句
	query := fmt.Sprintf(`
		SELECT CONCAT('DROP TABLE IF EXISTS ', table_name, ';') AS drop_command
		FROM information_schema.tables
		WHERE table_schema = '%s' AND table_name LIKE '%s%%'`, common.MIRA_TMP_TASK_DB, prefix)

	rows, done, err := s.ExecuteSQL(query)
	if err != nil {
		return fmt.Errorf("failed to query drop commands: %v", err)
	}
	if rows == nil {
		log.Logger.Infof("No tables found with prefix: %s", prefix)
		return nil
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	var dropCommands []string
	for rows.Next() {
		var dropCommand string
		err := rows.Scan(&dropCommand)
		if err != nil {
			log.Logger.Errorf("failed to scan drop command: %v", err)
			continue
		}
		dropCommands = append(dropCommands, dropCommand)
	}

	if len(dropCommands) == 0 {
		log.Logger.Infof("No tables found with prefix '%s' to drop", prefix)
		return nil
	}

	log.Logger.Infof("Found %d tables to drop with prefix '%s'", len(dropCommands), prefix)

	// 执行所有DROP命令
	var errors []string
	successCount := 0
	for _, dropCommand := range dropCommands {
		// 去掉末尾的分号，因为ExecuteUpdate不需要
		cleanCommand := strings.TrimSuffix(dropCommand, ";")

		log.Logger.Infof("Executing: %s", cleanCommand)

		_, err := s.ExecuteUpdate(cleanCommand)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to execute '%s': %v", cleanCommand, err)
			log.Logger.Errorf(errorMsg)
			errors = append(errors, errorMsg)
		} else {
			log.Logger.Infof("Successfully executed: %s", cleanCommand)
			successCount++
		}
	}

	log.Logger.Infof("Cleanup completed. Successfully dropped %d tables, %d errors occurred",
		successCount, len(errors))

	if len(errors) > 0 {
		return fmt.Errorf("some tables failed to drop: %s", strings.Join(errors, "; "))
	}

	return nil

}

// InitMiraTaskTmpDatabase 初始化 mira_task_tmp 数据库
func (s *DorisService) InitMiraTaskTmpDatabase() error {
	// 确保 mira_task_tmp 数据库存在
	if dbErr := s.EnsureDorisDatabaseExists(common.MIRA_TMP_TASK_DB); dbErr != nil {
		return fmt.Errorf("failed to ensure Doris database exists: %v", dbErr)
	}
	log.Logger.Infof("Ensured Doris database '%s' exists", common.MIRA_TMP_TASK_DB)

	// 切换到 mira_task_tmp 数据库
	useQuery := fmt.Sprintf("USE `%s`", common.MIRA_TMP_TASK_DB)
	_, useErr := s.ExecuteUpdate(useQuery)
	if useErr != nil {
		return fmt.Errorf("failed to switch to database '%s': %v", common.MIRA_TMP_TASK_DB, useErr)
	}
	log.Logger.Infof("Switched to Doris database: %s", common.MIRA_TMP_TASK_DB)

	return nil
}

// ensureDorisDatabaseExists 确保 Doris 数据库存在，如果不存在则创建
func (s *DorisService) EnsureDorisDatabaseExists(dbName string) error {
	createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
	_, createErr := s.ExecuteUpdate(createQuery)
	if createErr != nil {
		return fmt.Errorf("failed to create Doris database '%s': %v", dbName, createErr)
	}
	log.Logger.Infof("Created Doris database: %s", dbName)
	return nil
}

// GetDorisTableSchema 根据表名获取Doris表的字段信息
func (s *DorisService) GetDorisTableSchema(dbName, tableName string) ([]*ds.ColumnItem, error) {
	// 方法1: 使用 DESCRIBE 语句
	describeSQL := fmt.Sprintf("DESCRIBE %s.%s", dbName, tableName)
	rows, done, err := s.ExecuteSQL(describeSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %s.%s: %v", dbName, tableName, err)
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	var columns []*ds.ColumnItem
	for rows.Next() {
		var field, dataType, null, key, defaultVal, extra sql.NullString
		err := rows.Scan(&field, &dataType, &null, &key, &defaultVal, &extra)
		if err != nil {
			return nil, fmt.Errorf("failed to scan describe result: %v", err)
		}

		if field.Valid {
			column := &ds.ColumnItem{
				Name:     field.String,
				DataType: dataType.String,
			}
			columns = append(columns, column)
		}
	}

	if len(columns) > 0 {
		log.Logger.Debugf("Retrieved %d columns for Doris table %s.%s using DESCRIBE", len(columns), dbName, tableName)
		return columns, nil
	}

	// 方法2: 如果DESCRIBE失败，使用information_schema
	infoSchemaSQL := fmt.Sprintf(`
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
		FROM information_schema.COLUMNS 
		WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
		ORDER BY ORDINAL_POSITION
	`, dbName, tableName)

	rows, done, err = s.ExecuteSQL(infoSchemaSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema for table %s.%s: %v", dbName, tableName, err)
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	for rows.Next() {
		var columnName, dataType, isNullable, columnDefault, extra sql.NullString
		err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &extra)
		if err != nil {
			return nil, fmt.Errorf("failed to scan information_schema result: %v", err)
		}

		if columnName.Valid {
			column := &ds.ColumnItem{
				Name:     columnName.String,
				DataType: dataType.String,
			}
			columns = append(columns, column)
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s.%s", dbName, tableName)
	}

	log.Logger.Debugf("Retrieved %d columns for Doris table %s.%s using information_schema", len(columns), dbName, tableName)
	return columns, nil
}

func (s *DorisService) convertDatabaseTypeToDorisType(dataType string, dbType int32) string {
	// 转换为大写以匹配类型
	dbTypeName := strings.ToUpper(strings.TrimSpace(dataType))

	// 根据数据库类型进行不同的处理
	switch dbType {
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE),
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_VASTBASE):
		return s.convertKingbaseTypeToDorisType(dbTypeName)
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL),
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_TIDB),
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_TDSQL):
		return s.convertMySQLTypeToDorisType(dbTypeName)
	default:
		// 默认使用MySQL的转换逻辑
		return s.convertMySQLTypeToDorisType(dbTypeName)
	}
}

// convertKingbaseTypeToDorisType 将Kingbase数据类型转换为Doris数据类型
func (s *DorisService) convertKingbaseTypeToDorisType(dbTypeName string) string {
	// 处理整数类型
	switch {
	case strings.Contains(dbTypeName, "INT8") || strings.Contains(dbTypeName, "BIGINT"):
		return "BIGINT"
	case strings.Contains(dbTypeName, "INT2") || strings.Contains(dbTypeName, "SMALLINT"):
		return "SMALLINT"
	case strings.Contains(dbTypeName, "INT4") || strings.Contains(dbTypeName, "INT") || dbTypeName == "INTEGER":
		return "INT"
	case strings.Contains(dbTypeName, "TINYINT"):
		return "TINYINT"
	}

	// 处理数值类型
	if strings.Contains(dbTypeName, "NUMERIC") || strings.Contains(dbTypeName, "DECIMAL") {
		// 提取精度和标度信息
		if strings.Contains(dbTypeName, "(") && strings.Contains(dbTypeName, ")") {
			// 保持原有的精度和标度
			return dbTypeName
		}
		// 默认精度和标度
		return "DECIMAL(38,10)"
	}

	// 处理浮点数类型
	// KingBase 的 float4 对应 Doris 的单精度浮点 FLOAT
	if strings.Contains(dbTypeName, "FLOAT4") || dbTypeName == "FLOAT4" {
		return "FLOAT"
	}
	// KingBase 的 float8 对应 Doris 的双精度浮点 DOUBLE
	if strings.Contains(dbTypeName, "FLOAT8") || dbTypeName == "FLOAT8" {
		return "DOUBLE"
	}

	if strings.Contains(dbTypeName, "FLOAT") || strings.Contains(dbTypeName, "DOUBLE") {
		return "DOUBLE"
	}

	if dbTypeName == "REAL" {
		return "FLOAT"
	}

	// 处理字符串类型
	if strings.Contains(dbTypeName, "VARCHAR") || strings.Contains(dbTypeName, "CHAR") {
		// 保持原有的长度信息
		if strings.Contains(dbTypeName, "(") && strings.Contains(dbTypeName, ")") {
			return dbTypeName
		}
		return "VARCHAR"
	}

	if strings.Contains(dbTypeName, "TEXT") {
		return "VARCHAR"
	}

	// 处理时间类型

	if dbTypeName == "DATE" {
		return "DATE"
	}

	if dbTypeName == "TIMESTAMP" {
		return "DATETIME"
	}

	// 处理布尔类型
	if dbTypeName == "BOOLEAN" || dbTypeName == "BOOL" {
		return "BOOLEAN"
	}

	// 默认返回字符串类型
	return "VARCHAR"
}

// convertMySQLTypeToDorisType 将MySQL数据类型转换为Doris数据类型
func (s *DorisService) convertMySQLTypeToDorisType(dbTypeName string) string {
	// 处理整数类型
	switch {
	case strings.Contains(dbTypeName, "TINYINT"):
		return "TINYINT"
	case strings.Contains(dbTypeName, "SMALLINT"):
		return "SMALLINT"
	case strings.Contains(dbTypeName, "BIGINT"):
		return "BIGINT"
	case strings.Contains(dbTypeName, "MEDIUMINT") || strings.Contains(dbTypeName, "INT") || dbTypeName == "INTEGER":
		return "INT"
	}

	// 处理数值类型
	if strings.Contains(dbTypeName, "DECIMAL") || strings.Contains(dbTypeName, "NUMERIC") {
		// 提取精度和标度信息
		if strings.Contains(dbTypeName, "(") && strings.Contains(dbTypeName, ")") {
			// 保持原有的精度和标度
			return dbTypeName
		}
		// 默认精度和标度
		return "DECIMAL(38,10)"
	}

	// 处理浮点数类型
	if dbTypeName == "FLOAT" {
		return "FLOAT"
	}

	if dbTypeName == "DOUBLE" {
		return "DOUBLE"
	}

	// 处理字符串类型
	if strings.Contains(dbTypeName, "VARCHAR") || strings.Contains(dbTypeName, "CHAR") {
		// 保持原有的长度信息
		if strings.Contains(dbTypeName, "(") && strings.Contains(dbTypeName, ")") {
			return dbTypeName
		}
		return "VARCHAR"
	}

	if strings.Contains(dbTypeName, "TEXT") || strings.Contains(dbTypeName, "LONGTEXT") ||
		strings.Contains(dbTypeName, "MEDIUMTEXT") || strings.Contains(dbTypeName, "TINYTEXT") {
		return "VARCHAR"
	}

	// 处理时间类型

	if dbTypeName == "DATE" {
		return "DATE"
	}

	if dbTypeName == "TIMESTAMP" || dbTypeName == "DATETIME" {
		return "DATETIME"
	}

	// 处理布尔类型
	if dbTypeName == "BOOLEAN" || dbTypeName == "BOOL" {
		return "BOOLEAN"
	}

	// 默认返回字符串类型
	return "VARCHAR"
}

// trackExportTaskStatusByLabel 使用SHOW EXPORT命令根据Label追踪导出任务状态直到完成
func (s *DorisService) trackExportTaskStatusByJobId(dbName string, label string) error {
	maxRetries := 300                // 最大重试次数，避免无限等待
	retryInterval := 2 * time.Second // 重试间隔

	for i := 0; i < maxRetries; i++ {
		// 查询导出任务状态
		taskInfo, err := s.GetExportTaskByLabel(dbName, label)
		if err != nil {
			log.Logger.Warnf("Failed to get export task status (attempt %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(retryInterval)
			continue
		}

		if taskInfo == nil {
			log.Logger.Infof("Export task not found yet, waiting... (attempt %d/%d)", i+1, maxRetries)
			time.Sleep(retryInterval)
			continue
		}

		switch taskInfo.State {
		case common.ExportStatusFinished:
			log.Logger.Infof("Export task completed successfully for job %s", label)
			return nil
		case common.ExportStatusCancelled:
			return fmt.Errorf("export task was cancelled for job %s", label)
		case common.ExportStatusFailed:
			return fmt.Errorf("export task failed for job %s: %s", label, taskInfo.ErrorMsg)
		case common.ExportStatusRunning, common.ExportStatusPending:
			log.Logger.Infof("Export task is still running for job %s, progress: %s (attempt %d/%d)",
				label, taskInfo.Progress, i+1, maxRetries)
			time.Sleep(retryInterval)
		default:
			log.Logger.Infof("Export task status: %s for job %s (attempt %d/%d)",
				taskInfo.State, label, i+1, maxRetries)
			time.Sleep(retryInterval)
		}
	}

	return fmt.Errorf("export task timeout after %d attempts for job %s", maxRetries, label)
}

// GetExportTaskByLabel 获取指定Label的导出任务状态
func (s *DorisService) GetExportTaskByLabel(dbName string, label string) (*common.ExportTaskInfo, error) {
	// 构建查询SQL，使用LIKE匹配JobInstanceId
	querySQL := fmt.Sprintf("SHOW EXPORT FROM %s WHERE Label LIKE '%%%s%%'", dbName, label)

	log.Logger.Debugf("Querying export task with SQL: %s", querySQL)

	rows, done, err := s.ExecuteSQL(querySQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SHOW EXPORT: %v", err)
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	// 获取第一行（最新的任务）
	if rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan SHOW EXPORT result: %v", err)
		}

		// 构建结果映射
		result := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val != nil {
				switch v := val.(type) {
				case []byte:
					result[col] = string(v)
				default:
					result[col] = v
				}
			} else {
				result[col] = nil
			}
		}

		// 转换为ExportTaskInfo结构体
		taskInfo := common.ToExportTaskInfo(result)
		log.Logger.Infof("Found export task: %+v", taskInfo)
		return taskInfo, nil
	}

	// 没有找到匹配的任务
	return nil, nil
}

// ProcessDataSourceAndExport 处理数据源并导出到 MinIO
func (s *DorisService) ProcessDataSourceAndExport(request *ds.ReadDataSourceStreamingRequest, enhancedJobInstanceId string) (string, error) {
	var tableName string
	var err error

	// 1.从数据源拉取数据到doris
	switch src := request.DataSource.(type) {
	case *ds.ReadDataSourceStreamingRequest_External:
		// 1.1 从外部数据源拉取数据到doris
		tableName, err = s.CreateExternalAndInternalTableAndImportData(src.External.AssetName, src.External.ChainInfoId, src.External.Alias, enhancedJobInstanceId, enhancedJobInstanceId+"_"+"internal", enhancedJobInstanceId)
		if err != nil {
			log.Logger.Errorf("failed to create external table from asset: %v", err)
			return "", fmt.Errorf("failed to create external table from asset: %v", err)
		}
		if len(tableName) == 0 {
			log.Logger.Errorf("tableName is empty after import, import failed")
			return "", fmt.Errorf("import failed: tableName is empty")
		}

		// 1.2 执行导出csv文件到minio
		err = s.ExportParquetFileFromDoris(&ds.ExportCsvFileFromDorisRequest{
			DbName:           common.MIRA_TMP_TASK_DB,
			TableName:        tableName,
			JobInstanceId:    enhancedJobInstanceId,
			Columns:          request.Columns,
			SortRules:        request.SortRules,
			FilterConditions: request.FilterConditions,
		})
		if err != nil {
			log.Logger.Errorf("failed to export csv file from doris: %v", err)
			return "", fmt.Errorf("failed to export csv file from doris: %v", err)
		}

	case *ds.ReadDataSourceStreamingRequest_Internal:
		// 1.1 从内部数据源拉取数据到doris
		tableName, err = s.ImportMiraDBDataToDoris(&ds.ImportMiraDBDataToDorisRequest{
			MiraTableName: src.Internal.TableName,
			JobInstanceId: enhancedJobInstanceId,
		})
		if err != nil {
			log.Logger.Errorf("failed to import mira db data to doris: %v", err)
			return "", fmt.Errorf("failed to import mira db data to doris: %v", err)
		}
		// 1.2 执行导出arrow文件到minio
		err = s.ExportParquetFileFromDoris(&ds.ExportCsvFileFromDorisRequest{
			DbName:           common.MIRA_TMP_TASK_DB,
			TableName:        tableName,
			JobInstanceId:    enhancedJobInstanceId,
			Columns:          request.Columns,
			SortRules:        request.SortRules,
			FilterConditions: request.FilterConditions,
		})
		if err != nil {
			log.Logger.Errorf("failed to export parquet file from doris: %v", err)
			return "", fmt.Errorf("failed to export parquet file from doris: %v", err)
		}

	case *ds.ReadDataSourceStreamingRequest_Doris:
		// 1.2 执行导出arrow文件到minio
		err = s.ExportParquetFileFromDoris(&ds.ExportCsvFileFromDorisRequest{
			DbName:           src.Doris.DbName,
			TableName:        src.Doris.TableName,
			JobInstanceId:    enhancedJobInstanceId,
			Columns:          request.Columns,
			SortRules:        request.SortRules,
			FilterConditions: request.FilterConditions,
		})
		if err != nil {
			log.Logger.Errorf("failed to export parquet file from doris: %v", err)
			return "", fmt.Errorf("failed to export parquet file from doris: %v", err)
		}
		tableName = src.Doris.TableName

	default:
		return "", fmt.Errorf("unknown data source type")
	}

	return tableName, nil
}

// DropDatabase 删除指定的数据库
func (s *DorisService) DropDatabase(dbName string) error {
	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s` FORCE", dbName)
	_, err := s.ExecuteUpdate(dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop database '%s': %v", dbName, err)
	}
	log.Logger.Infof("Successfully dropped database: %s", dbName)
	return nil
}

func (s *DorisService) DropTable(dbName, tableName string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", dbName, tableName)
	_, err := s.ExecuteUpdate(dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop table '%s': %v", tableName, err)
	}
	log.Logger.Infof("Successfully dropped table: %s.%s", dbName, tableName)
	return nil
}

// processTLSCertificates 处理TLS证书（根据数据库类型选择格式）
func (s *DorisService) processTLSCertificates(tlsConfig *ds.DatasourceTlsConfig, requestId string, dbType int32) error {
	log.Logger.Infof("TLS configuration detected, processing certificates for database type: %d", dbType)

	if dbType == int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE) ||
		dbType == int32(ds.DataSourceType_DATA_SOURCE_TYPE_VASTBASE) {
		// 1) CA 用 PEM（catalog=kingbase）
		if err := s.processPEMCertificates(tlsConfig, requestId, common.KINGBASE_CERT_DIR); err != nil {
			return fmt.Errorf("failed to process PEM certificates: %v", err)
		}
		// 2) 客户端证书用 PKCS12（catalog=kingbase）
		if tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "" {
			return s.processPKCS12Certificates(tlsConfig, requestId, dbType)
		}
		return nil
	}

	// 非 Kingbase：统一用 PKCS12
	return s.processPKCS12Certificates(tlsConfig, requestId, dbType)
}

// processPEMCertificates 处理PEM格式证书
func (s *DorisService) processPEMCertificates(tlsConfig *ds.DatasourceTlsConfig, requestId string, catalog string) error {
	log.Logger.Infof("Processing certificates in PEM format")

	// 1. 上传PEM证书到MinIO
	if err := s.uploadCertificatesToMinio(tlsConfig, requestId); err != nil {
		return fmt.Errorf("failed to upload PEM certificates: %v", err)
	}

	// 2. 在Doris中创建PEM文件引用
	if err := s.createCertificateFilesInDoris(tlsConfig, requestId, catalog); err != nil {
		return fmt.Errorf("failed to create PEM certificate files: %v", err)
	}

	return nil
}

// processPKCS12Certificates 处理各数据库的 PKCS12 证书（根据 dbType 选择 catalog）
func (s *DorisService) processPKCS12Certificates(tlsConfig *ds.DatasourceTlsConfig, requestId string, dbType int32) error {
	log.Logger.Infof("Processing certificates in PKCS12 format, dbType=%d", dbType)

	// 选择 catalog：Kingbase 也使用 kingbase 目录，其他保持 mysql
	catalog := common.MYSQL_CERT_DIR
	if dbType == int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE) {
		catalog = common.KINGBASE_CERT_DIR
	}

	// 创建转换器
	converter, err := NewCertificateConverter(requestId)
	if err != nil {
		return fmt.Errorf("failed to create certificate converter: %v", err)
	}
	defer converter.Cleanup()

	// Kingbase：只生成客户端 p12（CA 仍走 PEM）；其他类型：CA 与客户端均可生成 p12
	var caP12Path, clientP12Path string
	if dbType == int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE) {
		_, clientP12Path, err = converter.ConvertToPKCS12(
			"", // 不生成 ca.p12
			tlsConfig.ClientCert,
			tlsConfig.ClientKey,
		)
	} else {
		caP12Path, clientP12Path, err = converter.ConvertToPKCS12(
			tlsConfig.CaCert,
			tlsConfig.ClientCert,
			tlsConfig.ClientKey,
		)
	}
	if err != nil {
		return fmt.Errorf("failed to convert certificates to PKCS12: %v", err)
	}

	// 上传到 MinIO（空路径会被忽略）
	if err := s.uploadPKCS12CertificatesToMinio(caP12Path, clientP12Path, requestId); err != nil {
		return fmt.Errorf("failed to upload PKCS12 certificates: %v", err)
	}

	// 在 Doris 中创建 FILE 引用（使用对应 catalog）
	if err := s.createPKCS12CertificateFilesInDoris(caP12Path, clientP12Path, requestId, catalog); err != nil {
		return fmt.Errorf("failed to create PKCS12 certificate files: %v", err)
	}

	return nil
}

// uploadPKCS12CertificatesToMinio 上传PKCS12证书到MinIO
func (s *DorisService) uploadPKCS12CertificatesToMinio(caP12Path, clientP12Path, requestId string) error {
	log.Logger.Infof("Uploading PKCS12 certificates to MinIO bucket: %s", common.TLS_CERT_BUCKET_NAME)

	// 获取OSS客户端
	ossFactory := oss.NewOSSFactory(config.GetConfigMap())
	ossClient, err := ossFactory.NewOSSClient()
	if err != nil {
		return fmt.Errorf("failed to create OSS client: %v", err)
	}

	var uploadedFiles []string

	// 上传CA证书（PKCS12格式）
	if caP12Path != "" {
		fileName := fmt.Sprintf("%s_ca_cert.p12", requestId)
		if err := s.uploadSingleFile(ossClient, caP12Path, fileName, "CA certificate"); err != nil {
			return fmt.Errorf("failed to upload CA certificate: %v", err)
		}
		uploadedFiles = append(uploadedFiles, fileName)
	}

	// 上传客户端证书（PKCS12格式）
	if clientP12Path != "" {
		fileName := fmt.Sprintf("%s_client_cert.p12", requestId)
		if err := s.uploadSingleFile(ossClient, clientP12Path, fileName, "client certificate"); err != nil {
			return fmt.Errorf("failed to upload client certificate: %v", err)
		}
		uploadedFiles = append(uploadedFiles, fileName)
	}

	log.Logger.Infof("Successfully uploaded %d PKCS12 certificate files: %v", len(uploadedFiles), uploadedFiles)
	return nil
}

// uploadSingleFile 上传单个文件
func (s *DorisService) uploadSingleFile(ossClient oss.ClientInterface, filePath, fileName, fileType string) error {
	// 读取文件内容
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read %s file: %v", fileType, err)
	}

	// 设置上传选项
	putOpts := &oss.PutOptions{
		ContentType: "application/x-pkcs12",
	}

	// 上传到MinIO
	_, err = ossClient.PutObject(context.Background(), common.TLS_CERT_BUCKET_NAME, fileName,
		bytes.NewReader(fileData), int64(len(fileData)), putOpts)
	if err != nil {
		return fmt.Errorf("failed to upload %s to MinIO: %v", fileType, err)
	}

	log.Logger.Infof("Successfully uploaded %s: %s", fileType, fileName)
	return nil
}

// createPKCS12CertificateFilesInDoris 在Doris中创建PKCS12证书文件引用
func (s *DorisService) createPKCS12CertificateFilesInDoris(caP12Path, clientP12Path, requestId string, catalog string) error {
	config := config.GetConfigMap()
	minioHost := config.OSSConfig.Host
	minioPort := config.OSSConfig.Port

	// 打印当前数据库
	s.printCurrentDatabase()

	// 创建CA证书文件引用
	if caP12Path != "" {
		fileName := fmt.Sprintf("%s_ca_cert.p12", requestId)
		fileURL := fmt.Sprintf("http://%s:%d/%s/%s", minioHost, minioPort, common.TLS_CERT_BUCKET_NAME, fileName)

		createFileSQL := fmt.Sprintf(`
            CREATE FILE "%s" PROPERTIES(
                "url" = "%s",
                "catalog" = "%s"
            )
        `, fileName, fileURL, catalog)

		_, err := s.ExecuteUpdate(createFileSQL)
		if err != nil {
			return fmt.Errorf("failed to create CA cert file: %v", err)
		}
		log.Logger.Infof("Created PKCS12 CA certificate file reference: %s", fileName)
	}

	// 创建客户端证书文件引用
	if clientP12Path != "" {
		fileName := fmt.Sprintf("%s_client_cert.p12", requestId)
		fileURL := fmt.Sprintf("http://%s:%d/%s/%s", minioHost, minioPort, common.TLS_CERT_BUCKET_NAME, fileName)

		createFileSQL := fmt.Sprintf(`
            CREATE FILE "%s" PROPERTIES(
                "url" = "%s",
                "catalog" = "%s"
            )
        `, fileName, fileURL, catalog)

		_, err := s.ExecuteUpdate(createFileSQL)
		if err != nil {
			return fmt.Errorf("failed to create client cert file: %v", err)
		}
		log.Logger.Infof("Created PKCS12 client certificate file reference: %s", fileName)
	}

	return nil
}

// getFileInfo 获取文件信息（ID和MD5）
func (s *DorisService) getFileInfo(dbName string, sourceFileName string) *FileInfo {
	query := fmt.Sprintf("SHOW FILE FROM %s", dbName) // 关键：带库名
	rows, done, err := s.ExecuteSQL(query)
	if err != nil {
		log.Logger.Errorf("Failed to execute SHOW FILE: %v", err)
		return nil
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	for rows.Next() {
		var fileId int64
		var dbName, catalog, fileName, fileSize, isContent, md5 string
		if err := rows.Scan(&fileId, &dbName, &catalog, &fileName, &fileSize, &isContent, &md5); err != nil {
			log.Logger.Errorf("Failed to scan file info: %v", err)
			continue
		}

		// 通过fileName进行过滤
		if fileName == sourceFileName {
			return &FileInfo{
				FileId: fileId,
				MD5:    md5,
			}
		}
	}

	log.Logger.Warnf("File not found: %s", sourceFileName)
	return nil
}

// uploadCertificatesToMinio 将TLS证书上传到MinIO的TLS证书桶
func (s *DorisService) uploadCertificatesToMinio(tlsConfig *ds.DatasourceTlsConfig, requestId string) error {
	log.Logger.Infof("Uploading TLS certificates to MinIO bucket: %s", common.TLS_CERT_BUCKET_NAME)

	// 获取OSS客户端
	ossFactory := oss.NewOSSFactory(config.GetConfigMap())
	ossClient, err := ossFactory.NewOSSClient()
	if err != nil {
		return fmt.Errorf("failed to create OSS client: %v", err)
	}

	// 检查并创建TLS证书桶（如果不存在）
	exists, err := ossClient.BucketExists(common.TLS_CERT_BUCKET_NAME)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %v", err)
	}

	if !exists {
		// 创建桶
		if err := ossClient.MakeBucket(common.TLS_CERT_BUCKET_NAME, ""); err != nil {
			return fmt.Errorf("failed to create TLS cert bucket: %v", err)
		}
		log.Logger.Infof("Created TLS cert bucket: %s", common.TLS_CERT_BUCKET_NAME)
	}

	var uploadedFiles []string

	// 1. 上传CA证书
	if tlsConfig.CaCert != "" {
		fileName := fmt.Sprintf("%s_ca_cert.pem", requestId)
		if err := s.uploadSingleCertificate(ossClient, tlsConfig.CaCert, fileName, "CA certificate"); err != nil {
			return fmt.Errorf("failed to upload CA certificate: %v", err)
		}
		uploadedFiles = append(uploadedFiles, fileName)
	}

	// 2. 上传客户端证书
	if tlsConfig.ClientCert != "" {
		fileName := fmt.Sprintf("%s_client_cert.pem", requestId)
		if err := s.uploadSingleCertificate(ossClient, tlsConfig.ClientCert, fileName, "client certificate"); err != nil {
			return fmt.Errorf("failed to upload client certificate: %v", err)
		}
		uploadedFiles = append(uploadedFiles, fileName)
	}

	// 3. 上传客户端私钥
	if tlsConfig.ClientKey != "" {
		fileName := fmt.Sprintf("%s_client_key.pem", requestId)
		if err := s.uploadSingleCertificate(ossClient, tlsConfig.ClientKey, fileName, "client key"); err != nil {
			return fmt.Errorf("failed to upload client key: %v", err)
		}
		uploadedFiles = append(uploadedFiles, fileName)
	}

	log.Logger.Infof("Successfully uploaded %d certificate files: %v", len(uploadedFiles), uploadedFiles)
	return nil
}

// createCertificateFilesInDoris 在Doris中创建证书文件引用
/**
DROP FILE "04910551-7982-4ddf-98f9-96c3e6f2fcf1_ca_cert.p12"
FROM mira_task_tmp
PROPERTIES("catalog" = "mysql");删除文件
**/
func (s *DorisService) createCertificateFilesInDoris(tlsConfig *ds.DatasourceTlsConfig, requestId string, catalog string) error {
	// 获取MinIO配置
	config := config.GetConfigMap()
	minioHost := config.OSSConfig.Host
	minioPort := config.OSSConfig.Port

	// 1. 创建CA证书文件引用
	if tlsConfig.CaCert != "" {
		caCertURL := fmt.Sprintf("http://%s:%d/%s/%s_ca_cert.pem",
			minioHost, minioPort, common.TLS_CERT_BUCKET_NAME, requestId)

		createFileSQL := fmt.Sprintf(`
            CREATE FILE "%s_ca_cert.pem" PROPERTIES(
                "url" = "%s",
                "catalog" = "%s"
            )
        `, requestId, caCertURL, catalog)

		_, err := s.ExecuteUpdate(createFileSQL)
		if err != nil {
			return fmt.Errorf("failed to create CA cert file: %v", err)
		}
		log.Logger.Infof("Created CA certificate file reference: %s_ca_cert.pem", requestId)
	}

	// 2. 创建客户端证书文件引用
	if tlsConfig.ClientCert != "" {
		clientCertURL := fmt.Sprintf("http://%s:%d/%s/%s_client_cert.pem",
			minioHost, minioPort, common.TLS_CERT_BUCKET_NAME, requestId)

		createFileSQL := fmt.Sprintf(`
            CREATE FILE "%s_client_cert.pem" PROPERTIES(
                "url" = "%s",
                "catalog" = "%s"
            )
        `, requestId, clientCertURL, catalog)

		_, err := s.ExecuteUpdate(createFileSQL)
		if err != nil {
			return fmt.Errorf("failed to create client cert file: %v", err)
		}
		log.Logger.Infof("Created client certificate file reference: %s_client_cert.pem", requestId)
	}

	// 3. 创建客户端私钥文件引用
	if tlsConfig.ClientKey != "" {
		clientKeyURL := fmt.Sprintf("http://%s:%d/%s/%s_client_key.pem",
			minioHost, minioPort, common.TLS_CERT_BUCKET_NAME, requestId)

		createFileSQL := fmt.Sprintf(`
            CREATE FILE "%s_client_key.pem" PROPERTIES(
                "url" = "%s",
                "catalog" = "%s"
            )
        `, requestId, clientKeyURL, catalog)

		_, err := s.ExecuteUpdate(createFileSQL)
		if err != nil {
			return fmt.Errorf("failed to create client key file: %v", err)
		}
		log.Logger.Infof("Created client key file reference: %s_client_key.pem", requestId)
	}

	return nil
}

// uploadSingleCertificate 上传单个证书文件
func (s *DorisService) uploadSingleCertificate(ossClient oss.ClientInterface, base64Cert, fileName, certType string) error {
	// 解码base64证书
	certData, err := base64.StdEncoding.DecodeString(base64Cert)
	if err != nil {
		return fmt.Errorf("failed to decode %s: %v", certType, err)
	}

	// 设置上传选项
	putOpts := &oss.PutOptions{
		ContentType: "application/x-pem-file",
	}

	// 上传到MinIO
	_, err = ossClient.PutObject(context.Background(), common.TLS_CERT_BUCKET_NAME, fileName,
		bytes.NewReader(certData), int64(len(certData)), putOpts)
	if err != nil {
		return fmt.Errorf("failed to upload %s to MinIO: %v", certType, err)
	}

	log.Logger.Infof("Successfully uploaded %s: %s", certType, fileName)
	return nil
}

func (s *DorisService) getCurrentDatabaseId(dbName string) (int, error) {
	// 执行 SHOW PROC '/dbs'; 命令获取数据库信息
	query := "SHOW PROC '/dbs';"
	rows, done, err := s.ExecuteSQL(query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute SHOW PROC '/dbs': %v", err)
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	// 查找指定的数据库
	for rows.Next() {
		var dbId int
		var currentDbName string
		var tableNum int
		var size string
		var quota string
		var lastConsistencyCheckTime sql.NullString
		var replicaCount int
		var replicaQuota int
		var transactionQuota int
		var lastUpdateTime sql.NullString

		err := rows.Scan(&dbId, &currentDbName, &tableNum, &size, &quota, &lastConsistencyCheckTime, &replicaCount, &replicaQuota, &transactionQuota, &lastUpdateTime)
		if err != nil {
			continue
		}

		// 匹配指定的数据库名
		if currentDbName == dbName {
			log.Logger.Infof("Found database: %s (ID: %d)", dbName, dbId)
			return dbId, nil
		}
	}

	// 如果没有找到指定的数据库，返回错误
	return 0, fmt.Errorf("database '%s' not found", dbName)
}

// getCurrentDatabase 获取当前数据库名
func (s *DorisService) getCurrentDatabase() string {
	query := "SELECT DATABASE()"
	rows, err := s.dbStrategy.Query(query) // 关键修复：避免调用 ExecuteSQL 造成递归
	if err != nil {
		log.Logger.Errorf("Failed to get current database: %v", err)
		return ""
	}
	defer rows.Close()

	if rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			log.Logger.Errorf("Failed to scan database name: %v", err)
			return ""
		}
		// log.Logger.Infof("Current database: %s", dbName)
		return dbName
	}
	return ""
}

// printCurrentDatabase 打印当前数据库信息
func (s *DorisService) printCurrentDatabase() {
	dbName := s.getCurrentDatabase()
	if dbName == "" {
		log.Logger.Warn("Unable to determine current database")
	} else {
		log.Logger.Infof("Current database: %s", dbName)
	}
}

func (s *DorisService) GetDBName() string {
	if dorisStrategy, ok := s.dbStrategy.(*database.DorisStrategy); ok {
		return dorisStrategy.Info.DbName
	}
	log.Logger.Warn("Unable to determine current database")
	return "" // 默认值
}

// SwitchDatabase 切换到指定数据库
func (s *DorisService) SwitchDatabase(dbName string) error {
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	// 执行 USE 语句切换数据库
	useSQL := fmt.Sprintf("USE `%s`", dbName)
	_, err := s.ExecuteUpdate(useSQL)
	if err != nil {
		log.Logger.Errorf("Failed to switch to database '%s': %v", dbName, err)
		return fmt.Errorf("failed to switch to database '%s': %v", dbName, err)
	}

	log.Logger.Infof("Successfully switched to database: %s", dbName)
	return nil
}
