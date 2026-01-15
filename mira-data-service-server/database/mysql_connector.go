/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接mysql功能

*
*/
package database

import (
	"crypto/tls"
	"crypto/x509"
	"data-service/common"
	"data-service/config"
	ds "data-service/generated/datasource"
	"data-service/log"
	"data-service/utils"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

var (
	mysqlPoolMap = make(map[string]*sql.DB)
	mysqlMutex   sync.Mutex
)

// TableInfoResponse 结构体用于存储查询结果
type TableInfoResponse struct {
	TableSchema string `json:"table_schema"`
	TableName   string `json:"table_name"`
	TableRows   int32  `json:"table_rows"`
	TableSize   int64  `json:"data_length"`
}

type MySQLStrategy struct {
	info *ds.ConnectionInfo
	DB   *sql.DB
}

func NewMySQLStrategy(info *ds.ConnectionInfo) *MySQLStrategy {
	return &MySQLStrategy{
		info: info,
	}
}

// 配置 TLS
func setupTLSConfig(tlsConfig *ds.DatasourceTlsConfig) error {
	if tlsConfig == nil || tlsConfig.UseTls != 2 {
		return nil // 不启用TLS
	}

	log.Logger.Info("Setting up MySQL TLS configuration")

	// 创建证书池
	rootCertPool := x509.NewCertPool()

	// 添加 CA 证书
	if tlsConfig.CaCert != "" {
		ca, err := base64.StdEncoding.DecodeString(tlsConfig.CaCert)
		if err != nil {
			return fmt.Errorf("failed to decode CA certificate: %v", err)
		}
		if !rootCertPool.AppendCertsFromPEM(ca) {
			return fmt.Errorf("failed to append CA certificate to pool")
		}
		log.Logger.Debug("CA certificate added to pool")
	}

	// 创建 TLS 配置
	config := &tls.Config{
		RootCAs:    rootCertPool,
		ServerName: tlsConfig.ServerName,
	}

	// 根据模式设置验证级别
	switch tlsConfig.Mode {
	case 1: // Require - 仅要求加密，不验证证书
		config.InsecureSkipVerify = true
		log.Logger.Debug("TLS mode set to Require (skip certificate verification)")
	case 2: // Verify CA - 验证 CA 证书，但不验证主机名
		config.InsecureSkipVerify = true // 跳过所有默认验证
		config.VerifyPeerCertificate = func(certificates [][]byte, verifiedChains [][]*x509.Certificate) error {
			// 手动验证CA证书
			if len(certificates) == 0 {
				log.Logger.Error("No certificates provided")
				return fmt.Errorf("no certificates provided")
			}

			// 解析服务器证书
			cert, err := x509.ParseCertificate(certificates[0])
			if err != nil {
				log.Logger.Error("Failed to parse certificate: %v", err)
				return fmt.Errorf("failed to parse certificate: %v", err)
			}

			// 验证证书是否由可信CA签发（不验证主机名）
			opts := x509.VerifyOptions{
				Roots: rootCertPool, // 包含CA证书的证书池
				// 不设置 DNSName，这样就跳过主机名验证
			}

			_, err = cert.Verify(opts)
			if err != nil {
				log.Logger.Error("Certificate verification failed: %v", err)
				return fmt.Errorf("certificate verification failed: %v", err)
			}

			log.Logger.Info("Certificate verified against CA (hostname verification skipped)")
			return nil
		}
	case 3: // Full Verification - 完全验证
		config.InsecureSkipVerify = false
		log.Logger.Debug("TLS mode set to Full Verification")
	default:
		config.InsecureSkipVerify = true
		log.Logger.Warn("Unknown TLS mode, defaulting to Require")
	}

	// 添加客户端证书（如果提供）
	if tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "" {
		clientCert, err := base64.StdEncoding.DecodeString(tlsConfig.ClientCert)
		if err != nil {
			return fmt.Errorf("failed to decode client certificate: %v", err)
		}

		clientKey, err := base64.StdEncoding.DecodeString(tlsConfig.ClientKey)
		if err != nil {
			return fmt.Errorf("failed to decode client key: %v", err)
		}

		cert, err := tls.X509KeyPair(clientCert, clientKey)
		if err != nil {
			return fmt.Errorf("failed to create X509 key pair: %v", err)
		}

		config.Certificates = []tls.Certificate{cert}
		log.Logger.Debug("Client certificate added to TLS configuration")
	}

	// 注册 TLS 配置到 MySQL 驱动
	err := mysql.RegisterTLSConfig(common.MYSQL_TLS_CONFIG, config)
	if err != nil {
		return fmt.Errorf("failed to register TLS config: %v", err)
	}

	log.Logger.Info("MySQL TLS configuration registered successfully")
	return nil
}

func (m *MySQLStrategy) ConnectToDB() error {
	return nil
}

func (m *MySQLStrategy) ConnectToDBWithPass(info *ds.ConnectionInfo) error {
	dbType := utils.GetDbTypeName(info.Dbtype)

	// 构建连接池键，包含 TLS 配置信息
	tlsKey := "no-tls"
	if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
		tlsKey = fmt.Sprintf("tls-%d-%s", info.TlsConfig.Mode, info.TlsConfig.ServerName)
	}
	key := fmt.Sprintf("%s_%s_%d_%s_%s_%s", dbType, info.Host, info.Port, info.DbName, info.Password, tlsKey)

	mysqlMutex.Lock()
	defer mysqlMutex.Unlock()

	// 检查连接池是否已经存在
	if db, ok := mysqlPoolMap[key]; ok {
		m.DB = db
		log.Logger.Debugf("Reusing existing MySQL connection pool")
		return nil
	}

	var dsn string

	// 检查是否需要设置 TLS
	log.Logger.Infof("TlsConfig config: %+v", info.TlsConfig)
	if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
		// 设置 TLS 配置
		err := setupTLSConfig(info.TlsConfig)
		if err != nil {
			return fmt.Errorf("failed to setup TLS configuration: %v", err)
		}

		// 构造带 TLS 的 DSN
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s&parseTime=true&loc=UTC",
			info.User, info.Password, info.Host, info.Port, info.DbName, common.MYSQL_TLS_CONFIG)

		log.Logger.Infof("Connecting to MySQL with TLS enabled")
	} else {
		// 构造普通 DSN
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC",
			info.User, info.Password, info.Host, info.Port, info.DbName)

		log.Logger.Infof("Connecting to MySQL without TLS")
	}
	conf := config.GetConfigMap()

	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Logger.Errorf("Failed to connect to MySQL: %v", err)
		return fmt.Errorf("failed to connect to MySQL: %v", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(conf.Dbms.MaxOpenConns) // 最大打开连接数
	db.SetMaxIdleConns(conf.Dbms.MaxIdleConns) // 最大空闲连接数
	db.SetConnMaxLifetime(10 * time.Minute)    // 连接最大生命周期
	db.SetConnMaxIdleTime(2 * time.Minute)     // 连接最大空闲时间

	// 成功连接后，保存数据库实例
	m.DB = db
	mysqlPoolMap[key] = db
	log.Logger.Info("Successfully connected to MySQL with username and password")
	return nil
}

func (m *MySQLStrategy) Query(sqlQuery string, args ...interface{}) (*sql.Rows, error) {
	log.Logger.Debugf("Executing query: %s with args: %v\n", sqlQuery, args)
	// 执行查询，args 用于绑定 SQL 查询中的占位符
	rows, err := m.DB.Query(sqlQuery, args...)
	if err != nil {
		log.Logger.Errorf("Query failed: %v\n", err)
		return nil, err
	}
	log.Logger.Debugf("%s Query executed successfully", sqlQuery)

	return rows, nil
}

func (m *MySQLStrategy) Close() error {
	// 检查数据库连接是否已经初始化
	if m.DB != nil {
		err := m.DB.Close()
		if err != nil {
			log.Logger.Errorf("Failed to close MySQL connection: %v", err)
			return err
		}
		log.Logger.Info("MySQL connection closed successfully")
		return nil
	}
	// 如果数据库连接未初始化，直接返回 nil
	log.Logger.Warn("Attempted to close a non-initialized DB connection")
	return nil
}

func (m *MySQLStrategy) GetJdbcUrl() string {
	// 构建 JDBC URL
	jdbcUrl := fmt.Sprintf(
		"jdbc:mysql://%s:%d/%s?user=%s&password=%s",
		m.info.Host,
		m.info.Port,
		m.info.DbName,
		m.info.User,     // 添加用户名
		m.info.Password, // 添加密码
	)
	return jdbcUrl
}

// 从数据库游标中按行读取数据，并构建当前批次的 Arrow Record
func (m *MySQLStrategy) RowsToArrowBatch(rows *sql.Rows, batchSize int) (arrow.Record, error) {
	if rows == nil {
		return nil, fmt.Errorf("no rows to convert")
	}

	// 创建内存分配器
	pool := memory.NewGoAllocator()

	// 获取列名
	cols, err := rows.Columns()
	if err != nil {
		/*if errors.Is(err, sql.ErrNoRows) || err == io.EOF {
			m.logger.Info("No more rows to read")
			return nil, io.EOF
		}*/
		//return nil, fmt.Errorf("failed to get columns: %v", err)
		return nil, io.EOF
	}

	// 获取每列的类型
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %v", err)
	}

	// 构建 Arrow Schema
	var fields []arrow.Field
	for i, col := range cols {
		arrowType, err := mysqlTypeToArrowType(colTypes[i])
		log.Logger.Debugf("SQL type for column %s: %s", col, colTypes[i].DatabaseTypeName())
		log.Logger.Debugf("Arrow type for column %s: %s", col, arrowType)
		if err != nil {
			return nil, fmt.Errorf("failed to convert SQL type to Arrow type: %v", err)
		}
		fields = append(fields, arrow.Field{Name: col, Type: arrowType})
	}
	schema := arrow.NewSchema(fields, nil)

	// 创建 Arrow RecordBuilder
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// 准备存储行数据的容器
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range valuePtrs {
		valuePtrs[i] = &values[i]
	}
	rowCount := 0
	// 遍历 rows 并填充 Arrow Builder
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		// 将值添加到 Arrow Builder 中
		for i, val := range values {
			err := utils.AppendValueToBuilder(builder.Field(i), val)
			if err != nil {
				log.Logger.Errorf("Failed to append value for column %d: %v", i, err)
				return nil, err
			}
		}
		rowCount++
		if rowCount >= batchSize {
			break // 达到批次大小，结束循环，发送给客户端
		}
	}
	// 创建 Arrow 批次 (Record)
	record := builder.NewRecord()
	return record, nil
}

// 将 SQL 列类型转换为 Arrow 类型
func mysqlTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, error) {
	switch colType.DatabaseTypeName() {
	case "VARCHAR", "CHAR":
		return arrow.BinaryTypes.String, nil
	case "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT":
		return arrow.BinaryTypes.LargeString, nil
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, nil
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nil
	case "MEDIUMINT", "INT":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "UNSIGNED TINYINT":
		return arrow.PrimitiveTypes.Uint8, nil
	case "UNSIGNED SMALLINT":
		return arrow.PrimitiveTypes.Uint16, nil
	case "UNSIGNED MEDIUMINT", "UNSIGNED INT":
		return arrow.PrimitiveTypes.Uint32, nil
	case "UNSIGNED BIGINT":
		return arrow.PrimitiveTypes.Uint64, nil
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case "TIMESTAMP", "DATETIME":
		return arrow.BinaryTypes.String, nil
	case "DECIMAL", "NUMERIC":
		// 提取精度和标度
		precision, scale, ok := colType.DecimalSize()
		if !ok {
			return nil, fmt.Errorf("unable to get precision and scale for DECIMAL column: %s", colType.Name())
		}
		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
	case "DATE":
		return arrow.BinaryTypes.String, nil
	// 处理其他类型
	default:
		return arrow.BinaryTypes.String, nil
	}
}

func (m *MySQLStrategy) CreateTemporaryTableIfNotExists(tableName string, schema *arrow.Schema) error {
	// 检查表是否存在
	var exists bool
	err := m.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)", tableName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %v", err)
	}

	if !exists {
		// 创建临时表
		createTableSQL := buildCreateMysqlTableSQL(tableName, schema)
		log.Logger.Infof("createTableSQL: %s", createTableSQL)
		_, err = m.DB.Exec(createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
		log.Logger.Infof("Created table: %s", tableName)
	} else {
		log.Logger.Infof("Table %s already exists", tableName)
	}

	return nil
}

// buildCreateTableSQL 根据 Arrow Schema 生成创建表的 SQL 语句
func buildCreateMysqlTableSQL(tableName string, schema *arrow.Schema) string {
	var columns []string

	// 生成一个随机的 UUID 并截取前 10 个字符
	randomUUID := "PK_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:10]

	// 将生成的 10 字符 UUID 作为自增主键的字段名
	columns = append(columns, fmt.Sprintf("`%s` INT AUTO_INCREMENT PRIMARY KEY", randomUUID))

	// 添加其他字段
	for _, field := range schema.Fields() {
		columnType := convertArrowTypeToMysqlType(field.Type)
		columns = append(columns, fmt.Sprintf("`%s` %s", field.Name, columnType))
	}

	// 返回完整的 CREATE TABLE 语句
	return fmt.Sprintf("CREATE TABLE `%s` (%s)", tableName, strings.Join(columns, ", "))
}

// convertArrowTypeToSQLType 将 Arrow 数据类型转换为 SQL 数据类型
func convertArrowTypeToMysqlType(arrowType arrow.DataType) string {
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
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.STRING:
		return "VARCHAR(255)"
	case arrow.LARGE_STRING:
		return "TEXT"
	case arrow.BINARY:
		return "BLOB"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DATE32:
		return "DATE"
	case arrow.DATE64:
		return "DATETIME"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		decimalType := arrowType.(arrow.FixedWidthDataType)
		precision := decimalType.(*arrow.Decimal128Type).Precision
		scale := decimalType.(*arrow.Decimal128Type).Scale
		return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
	default:
		return "TEXT" // 默认使用 TEXT 类型
	}
}

func (m *MySQLStrategy) GetTableInfo(database string, tableName string, isExactQuery bool) (*ds.TableInfoResponse, error) {
	// 定义 SQL 查询
	var sqlQuery string

	if isExactQuery {
		// 精确查询时，只查询记录数
		sqlQuery = fmt.Sprintf("SELECT COUNT(*) as table_rows FROM %s.%s", database, tableName)
	} else {
		// 普通查询，获取表的相关信息
		sqlQuery = fmt.Sprintf("SELECT table_schema, table_name, table_rows, data_length "+
			"FROM information_schema.tables "+
			"WHERE table_schema = '%s' AND table_name = '%s'", database, tableName)
	}

	// 记录日志
	log.Logger.Infof("Executing query: %s with parameters: database=%s, tableName=%s", sqlQuery, database, tableName)

	var result TableInfoResponse

	if isExactQuery {
		// 精确查询，只返回记录数
		if err := m.DB.QueryRow(sqlQuery).Scan(&result.TableRows); err != nil {
			return nil, err
		}
		result.TableSchema = ""      // 没有表模式
		result.TableName = tableName // 返回表名
		result.TableSize = 0         // 精确查询不关心表的大小
	} else {
		// 检查是否启用估算模式
		if ShouldUseEstimationOnly() {
			// 直接使用估算方式，跳过 information_schema 查询
			log.Logger.Infof("Using estimation mode (use_estimation_only enabled), skipping information_schema query")
			FillTableInfoFromEstimation(m, database, tableName, database, &result)
		} else {
			// 普通查询，先尝试从 information_schema.tables 获取
			err := m.DB.QueryRow(sqlQuery).Scan(&result.TableSchema, &result.TableName, &result.TableRows, &result.TableSize)
			if err != nil || result.TableName == "" || result.TableRows == 0 {
				// 查询失败或返回空结果，使用估算方式
				LogQueryFailure("information_schema.tables", err, result.TableName, result.TableRows)

				FillTableInfoFromEstimation(m, database, tableName, database, &result)
			}
		}
	}

	// 获取表结构信息
	columns, err := m.getTableSchema(database, tableName)
	if err != nil {
		log.Logger.Errorf("Failed to get table schema: %v", err)
		// 不返回错误，只记录日志，表结构信息为空
	}

	log.Logger.Infof("Table info: %+v", result)

	// 返回查询结果
	return &ds.TableInfoResponse{
		TableName:   result.TableName,
		RecordCount: result.TableRows,
		RecordSize:  result.TableSize,
		Columns:     columns,
	}, nil
}

// 获取表结构信息
func (m *MySQLStrategy) getTableSchema(database, tableName string) ([]*ds.ColumnItem, error) {
	query := `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT
              FROM INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION`

	rows, err := m.DB.Query(query, database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*ds.ColumnItem
	for rows.Next() {
		var col ds.ColumnItem
		var maxLength sql.NullInt64
		var isNullable, columnDefault, comment sql.NullString

		err := rows.Scan(&col.Name, &col.DataType, &maxLength, &isNullable, &columnDefault, &comment)
		if err != nil {
			return nil, err
		}

		columns = append(columns, &col)
	}

	return columns, nil
}

func (m *MySQLStrategy) BuildWithConditionQuery(
	tableName string,
	fields []string,
	filterNames []string,
	filterOperators []ds.FilterOperator,
	filterValues []*ds.FilterValue,
	sortRules []*ds.SortRule,
) (string, []interface{}, error) {
	var queryBuilder strings.Builder
	args := []interface{}{}
	queryTools := &QueryBuilder{}

	// 构建 SELECT 子句
	queryBuilder.WriteString("SELECT ")
	if len(fields) > 0 {
		// 检查每个字段是否有反引号，没有则加上
		quotedFields := make([]string, len(fields))
		for i, f := range fields {
			trimmed := strings.TrimSpace(f)
			if strings.HasPrefix(trimmed, "`") && strings.HasSuffix(trimmed, "`") {
				quotedFields[i] = trimmed
			} else {
				quotedFields[i] = fmt.Sprintf("`%s`", trimmed)
			}
		}
		queryBuilder.WriteString(strings.Join(quotedFields, ", "))
	} else {
		queryBuilder.WriteString("*")
	}
	queryBuilder.WriteString(" FROM ")
	queryBuilder.WriteString(tableName)

	// 构建 WHERE 子句
	if len(filterNames) > 0 && len(filterOperators) > 0 && len(filterValues) > 0 {
		if len(filterNames) != len(filterOperators) || len(filterOperators) != len(filterValues) {
			return "", nil, fmt.Errorf("filterNames, filterOperators, and filterValues must have the same length")
		}

		queryBuilder.WriteString(" WHERE ")

		conditions := make([]string, len(filterNames))
		for i := range filterNames {
			operator := filterOperators[i]
			filterValue := filterValues[i]
			filterName := filterNames[i]

			// 提取有效值
			argsFromFilter := queryTools.ExtractQueryArgs([]*ds.FilterValue{filterValue})
			if len(argsFromFilter) == 0 {
				return "", nil, fmt.Errorf("no valid values found for filter '%s'", filterName)
			}

			switch operator {
			case ds.FilterOperator_IN_OPERATOR:
				// 构建 IN 操作符的条件
				var inPlaceholders []string
				for _, v := range argsFromFilter {
					inPlaceholders = append(inPlaceholders, "?")
					args = append(args, v)
				}
				conditions[i] = fmt.Sprintf("%s IN (%s)", filterName, strings.Join(inPlaceholders, ", "))

			case ds.FilterOperator_GREATER_THAN, ds.FilterOperator_LESS_THAN,
				ds.FilterOperator_GREATER_THAN_OR_EQUAL, ds.FilterOperator_LESS_THAN_OR_EQUAL,
				ds.FilterOperator_NOT_EQUAL:
				// 构建比较操作符的条件
				conditions[i] = fmt.Sprintf("%s %s ?", filterName, queryTools.OperatorToString(operator))
				args = append(args, argsFromFilter[0]) // 取第一个有效值

			case ds.FilterOperator_LIKE_OPERATOR:
				// 构建 LIKE 操作符的条件
				conditions[i] = fmt.Sprintf("%s LIKE ?", filterName)
				args = append(args, argsFromFilter[0]) // 取第一个有效值

			default:
				// 默认等于操作
				conditions[i] = fmt.Sprintf("%s = ?", filterName)
				args = append(args, argsFromFilter[0]) // 取第一个有效值
			}
		}

		queryBuilder.WriteString(strings.Join(conditions, " AND "))
	}

	// 构建 ORDER BY 子句
	if len(sortRules) > 0 {
		queryBuilder.WriteString(" ORDER BY ")

		orders := make([]string, len(sortRules))
		for i, rule := range sortRules {
			order := "ASC"
			if rule.SortOrder == ds.SortOrder_DESC {
				order = "DESC"
			}
			orders[i] = fmt.Sprintf("%s %s", rule.FieldName, order)
		}
		queryBuilder.WriteString(strings.Join(orders, ", "))
	}

	return queryBuilder.String(), args, nil
}

func (m *MySQLStrategy) EnsureDatabaseExists(dbName string) error {
	createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
	_, err := m.DB.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("failed to create MySQL database '%s': %v", dbName, err)
	}
	log.Logger.Infof("Created MySQL database '%s'", dbName)
	return nil
}

func (m *MySQLStrategy) CheckTableExists(tableName string) (bool, error) {
	var exists bool
	err := m.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)", tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (m *MySQLStrategy) CleanupOldTables(dbName string, retentionDays int) error {
	// 切换到指定数据库
	useQuery := fmt.Sprintf("USE `%s`", dbName)
	if _, err := m.DB.Exec(useQuery); err != nil {
		return fmt.Errorf("failed to switch to database '%s': %v", dbName, err)
	}

	// 获取当前日期并计算保留的截止日期
	cutoffDate := time.Now().AddDate(0, 0, -retentionDays).Format("20060102")

	// 查询符合条件的表
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name LIKE ?`
	rows, err := m.Query(query, dbName, "20%")
	if err != nil {
		return fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	// 遍历结果，删除过期表
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %v", err)
		}

		// 检查表名是否早于保留日期
		if len(tableName) >= 8 && tableName[:8] < cutoffDate {
			dropQuery := fmt.Sprintf("DROP TABLE `%s`", tableName)
			if _, err := m.DB.Exec(dropQuery); err != nil {
				log.Logger.Errorf("failed to drop table '%s': %v", tableName, err)
			} else {
				log.Logger.Infof("Dropped table '%s'", tableName)
			}
		}
	}

	return nil
}

func (m *MySQLStrategy) GetGroupCountInfo(tableName string, groupBy []string, filterNames []string, filterOperators []ds.FilterOperator, filterValues []*ds.FilterValue) (*ds.GroupCountResponse, error) {
	queryTools := &QueryBuilder{}

	// 使用BuildGroupCountQuery生成SQL查询语句和参数
	query, args, err := queryTools.BuildGroupCountQuery(tableName, groupBy, filterNames, filterOperators,
		filterValues, ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to build group count query: %v", err)
	}

	// 执行查询
	log.Logger.Debugf("Executing group count query: %s with args: %v", query, args)
	rows, err := m.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute group count query: %v", err)
	}
	defer rows.Close()

	return ProcessGroupCountResults(rows, tableName)
}

// getTableRowCount 获取表的总行数
func (m *MySQLStrategy) getTableRowCount(database string, tableName string) (int32, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", database, tableName)
	var rowCount int32
	if err := m.DB.QueryRow(countQuery).Scan(&rowCount); err != nil {
		return 0, err
	}
	return rowCount, nil
}

// estimateTableSize 通过采样数据估算表的大小
func (m *MySQLStrategy) estimateTableSize(database string, tableName string, totalRows int32) (int64, error) {
	// 获取100条数据估算平均行大小
	sampleQuery := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 100", database, tableName)
	rows, err := m.DB.Query(sampleQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to get sample data for size estimation: %v", err)
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to get columns: %v", err)
	}

	// 计算样本数据的总大小
	var totalSampleSize int64
	rowCount := 0
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range valuePtrs {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Logger.Errorf("Failed to scan sample row: %v", err)
			continue
		}

		// 估算每行的大小
		totalSampleSize += common.EstimateRowSize(values)
		rowCount++
	}

	// 根据样本数据估算总大小
	if rowCount > 0 {
		avgRowSize := totalSampleSize / int64(rowCount)
		estimatedSize := avgRowSize * int64(totalRows)
		log.Logger.Infof("Estimated table size: %d bytes (avg row size: %d, total rows: %d)",
			estimatedSize, avgRowSize, totalRows)
		return estimatedSize, nil
	}

	return 0, fmt.Errorf("no sample data available for size estimation")
}

// GetTableRowCount 实现 TableInfoEstimator 接口
func (m *MySQLStrategy) GetTableRowCount(database string, tableName string) (int32, error) {
	return m.getTableRowCount(database, tableName)
}

// EstimateTableSize 实现 TableInfoEstimator 接口
func (m *MySQLStrategy) EstimateTableSize(database string, tableName string, totalRows int32) (int64, error) {
	return m.estimateTableSize(database, tableName, totalRows)
}
