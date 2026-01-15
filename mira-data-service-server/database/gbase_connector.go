/*
*

	@author: shiliang
	@date: 2024/12/25
	@note: GBase数据库连接功能

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
	gbasePoolMap = make(map[string]*sql.DB)
	gbaseMutex   sync.Mutex
)

type GBaseStrategy struct {
	info *ds.ConnectionInfo
	DB   *sql.DB
}

func NewGBaseStrategy(info *ds.ConnectionInfo) *GBaseStrategy {
	return &GBaseStrategy{
		info: info,
	}
}

// setupGBaseTLSConfig configures TLS for GBase connections
func setupGBaseTLSConfig(tlsConfig *ds.DatasourceTlsConfig) error {
	if tlsConfig == nil || tlsConfig.UseTls != 2 {
		return nil // TLS not enabled
	}

	log.Logger.Info("Setting up GBase TLS configuration")

	// Create certificate pool
	rootCertPool := x509.NewCertPool()

	// Add CA certificate
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

	// Create TLS configuration
	config := &tls.Config{
		RootCAs:    rootCertPool,
		ServerName: tlsConfig.ServerName,
	}

	// GBase8a 需要特殊处理:添加兼容老旧服务端的CipherSuites
	config.MinVersion = tls.VersionTLS10
	config.CipherSuites = gbase8aCipherSuites()

	// Set verification level based on mode
	switch tlsConfig.Mode {
	case 1: // Require - only encryption, no certificate verification
		config.InsecureSkipVerify = true
		log.Logger.Debug("TLS mode set to Require (skip certificate verification)")
	case 2: // Verify CA - verify CA certificate but not hostname
		config.InsecureSkipVerify = true
		config.VerifyPeerCertificate = func(certificates [][]byte, verifiedChains [][]*x509.Certificate) error {
			if len(certificates) == 0 {
				log.Logger.Error("No certificates provided")
				return fmt.Errorf("no certificates provided")
			}

			cert, err := x509.ParseCertificate(certificates[0])
			if err != nil {
				log.Logger.Error("Failed to parse certificate: %v", err)
				return fmt.Errorf("failed to parse certificate: %v", err)
			}

			opts := x509.VerifyOptions{
				Roots: rootCertPool,
			}

			_, err = cert.Verify(opts)
			if err != nil {
				log.Logger.Error("Certificate verification failed: %v", err)
				return fmt.Errorf("certificate verification failed: %v", err)
			}

			log.Logger.Info("Certificate verified against CA (hostname verification skipped)")
			return nil
		}
	case 3: // Full Verification
		config.InsecureSkipVerify = false
		log.Logger.Debug("TLS mode set to Full Verification")
	default:
		config.InsecureSkipVerify = true
		log.Logger.Warn("Unknown TLS mode, defaulting to Require")
	}

	// Add client certificate if provided
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

	// Register TLS configuration with MySQL driver (GBase uses MySQL protocol)
	err := mysql.RegisterTLSConfig(common.MYSQL_TLS_CONFIG, config)
	if err != nil {
		return fmt.Errorf("failed to register TLS config: %v", err)
	}

	log.Logger.Info("GBase TLS configuration registered successfully")
	return nil
}

func (g *GBaseStrategy) ConnectToDB() error {
	return nil
}

func (g *GBaseStrategy) ConnectToDBWithPass(info *ds.ConnectionInfo) error {
	dbType := utils.GetDbTypeName(info.Dbtype)

	// Build connection pool key with TLS info
	tlsKey := "no-tls"
	if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
		tlsKey = fmt.Sprintf("tls-%d-%s", info.TlsConfig.Mode, info.TlsConfig.ServerName)
	}
	key := fmt.Sprintf("%s_%s_%d_%s_%s_%s", dbType, info.Host, info.Port, info.DbName, info.Password, tlsKey)

	gbaseMutex.Lock()
	defer gbaseMutex.Unlock()

	// Check if connection pool already exists
	if db, ok := gbasePoolMap[key]; ok {
		g.DB = db
		log.Logger.Debugf("Reusing existing GBase connection pool")
		return nil
	}

	var dsn string

	// Check if TLS is needed
	log.Logger.Infof("TlsConfig config: %+v", info.TlsConfig)
	if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
		err := setupGBaseTLSConfig(info.TlsConfig)
		if err != nil {
			return fmt.Errorf("failed to setup TLS configuration: %v", err)
		}

		// Add charset=utf8mb4 to support 4-byte UTF-8 characters (emoji, etc.)
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s&charset=utf8mb4&parseTime=true&loc=UTC",
			info.User, info.Password, info.Host, info.Port, info.DbName, common.MYSQL_TLS_CONFIG)

		log.Logger.Infof("Connecting to GBase with TLS enabled")
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC",
			info.User, info.Password, info.Host, info.Port, info.DbName)

		log.Logger.Infof("Connecting to GBase without TLS")
	}

	conf := config.GetConfigMap()

	// Open database connection (GBase uses MySQL driver)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Logger.Errorf("Failed to connect to GBase: %v", err)
		return fmt.Errorf("failed to connect to GBase: %v", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(conf.Dbms.MaxOpenConns)
	db.SetMaxIdleConns(conf.Dbms.MaxIdleConns)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)

	// Save database instance after successful connection
	g.DB = db
	gbasePoolMap[key] = db
	log.Logger.Info("Successfully connected to GBase with username and password")
	return nil
}

func (g *GBaseStrategy) Query(sqlQuery string, args ...interface{}) (*sql.Rows, error) {
	log.Logger.Debugf("Executing query: %s with args: %v\n", sqlQuery, args)
	rows, err := g.DB.Query(sqlQuery, args...)
	if err != nil {
		log.Logger.Errorf("Query failed: %v\n", err)
		return nil, err
	}
	log.Logger.Debugf("%s Query executed successfully", sqlQuery)

	return rows, nil
}

func (g *GBaseStrategy) Close() error {
	if g.DB != nil {
		err := g.DB.Close()
		if err != nil {
			log.Logger.Errorf("Failed to close GBase connection: %v", err)
			return err
		}
		log.Logger.Info("GBase connection closed successfully")
		return nil
	}
	log.Logger.Warn("Attempted to close a non-initialized DB connection")
	return nil
}

func (g *GBaseStrategy) GetJdbcUrl() string {
	// Build JDBC URL for GBase
	jdbcUrl := fmt.Sprintf(
		"jdbc:gbase://%s:%d/%s?user=%s&password=%s",
		g.info.Host,
		g.info.Port,
		g.info.DbName,
		g.info.User,
		g.info.Password,
	)
	return jdbcUrl
}

// RowsToArrowBatch converts database rows to Arrow Record batch
func (g *GBaseStrategy) RowsToArrowBatch(rows *sql.Rows, batchSize int) (arrow.Record, error) {
	if rows == nil {
		return nil, fmt.Errorf("no rows to convert")
	}

	pool := memory.NewGoAllocator()

	// Get column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, io.EOF
	}

	// Get column types
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %v", err)
	}

	// Build Arrow Schema
	var fields []arrow.Field
	for i, col := range cols {
		arrowType, err := gbaseTypeToArrowType(colTypes[i])
		log.Logger.Debugf("SQL type for column %s: %s", col, colTypes[i].DatabaseTypeName())
		log.Logger.Debugf("Arrow type for column %s: %s", col, arrowType)
		if err != nil {
			return nil, fmt.Errorf("failed to convert SQL type to Arrow type: %v", err)
		}
		fields = append(fields, arrow.Field{Name: col, Type: arrowType})
	}
	schema := arrow.NewSchema(fields, nil)

	// Create Arrow RecordBuilder
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Prepare containers for row data
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range valuePtrs {
		valuePtrs[i] = &values[i]
	}
	rowCount := 0

	// Iterate through rows and populate Arrow Builder
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		// Add values to Arrow Builder
		for i, val := range values {
			err := utils.AppendValueToBuilder(builder.Field(i), val)
			if err != nil {
				log.Logger.Errorf("Failed to append value for column %d: %v", i, err)
				return nil, err
			}
		}
		rowCount++
		if rowCount >= batchSize {
			break
		}
	}

	// Create Arrow Record batch
	record := builder.NewRecord()
	return record, nil
}

// gbaseTypeToArrowType converts GBase SQL column type to Arrow type
func gbaseTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, error) {
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
		precision, scale, ok := colType.DecimalSize()
		if !ok {
			return nil, fmt.Errorf("unable to get precision and scale for DECIMAL column: %s", colType.Name())
		}
		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
	case "DATE":
		return arrow.BinaryTypes.String, nil
	default:
		return arrow.BinaryTypes.String, nil
	}
}

func (g *GBaseStrategy) CreateTemporaryTableIfNotExists(tableName string, schema *arrow.Schema) error {
	// Check if table exists
	var exists bool
	err := g.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)", tableName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %v", err)
	}

	if !exists {
		// Create table
		createTableSQL := buildCreateGBaseTableSQL(tableName, schema)
		log.Logger.Infof("createTableSQL: %s", createTableSQL)
		_, err = g.DB.Exec(createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
		log.Logger.Infof("Created table: %s", tableName)
	} else {
		log.Logger.Infof("Table %s already exists", tableName)
	}

	return nil
}

// buildCreateGBaseTableSQL generates CREATE TABLE SQL based on Arrow Schema
func buildCreateGBaseTableSQL(tableName string, schema *arrow.Schema) string {
	var columns []string

	// Generate random UUID for primary key field name (first 10 chars)
	randomUUID := "PK_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:10]

	// Add auto-increment primary key
	columns = append(columns, fmt.Sprintf("`%s` INT AUTO_INCREMENT PRIMARY KEY", randomUUID))

	// Add other fields
	for _, field := range schema.Fields() {
		columnType := convertArrowTypeToGBaseType(field.Type)
		columns = append(columns, fmt.Sprintf("`%s` %s", field.Name, columnType))
	}

	// Return complete CREATE TABLE statement with explicit charset and collation
	// Use utf8mb4 to support 4-byte UTF-8 characters (emoji, etc.)
	return fmt.Sprintf("CREATE TABLE `%s` (%s) ENGINE=EXPRESS DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
		tableName, strings.Join(columns, ", "))
}

// convertArrowTypeToGBaseType converts Arrow data type to GBase SQL type
func convertArrowTypeToGBaseType(arrowType arrow.DataType) string {
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
		return "TEXT"
	}
}

func (g *GBaseStrategy) GetTableInfo(database string, tableName string, isExactQuery bool) (*ds.TableInfoResponse, error) {
	// 定义 SQL 查询
	var sqlQuery string

	if isExactQuery {
		// 精确查询时，只查询记录数
		sqlQuery = fmt.Sprintf("SELECT COUNT(*) as table_rows FROM %s.%s", database, tableName)
	} else {
		// 先尝试单机部署方式，从information_schema.tables查询data_length和table_rows
		sqlQuery = fmt.Sprintf("SELECT table_schema, table_name, table_rows, data_length "+
			"FROM information_schema.tables "+
			"WHERE table_schema = '%s' AND table_name = '%s'", database, tableName)
	}

	// 记录日志
	log.Logger.Infof("Executing query: %s with parameters: database=%s, tableName=%s", sqlQuery, database, tableName)

	var result TableInfoResponse

	if isExactQuery {
		// 精确查询，只返回记录数
		if err := g.DB.QueryRow(sqlQuery).Scan(&result.TableRows); err != nil {
			return nil, err
		}
		result.TableSchema = ""      // 没有表模式
		result.TableName = tableName // 返回表名
		result.TableSize = 0         // 精确查询不关心表的大小
	} else {
		// 检查是否启用估算模式
		if ShouldUseEstimationOnly() {
			// 直接使用估算方式，跳过 information_schema 和 CLUSTER_TABLES 查询
			log.Logger.Infof("Using estimation mode (use_estimation_only enabled), skipping information_schema and CLUSTER_TABLES query")
			FillTableInfoFromEstimation(g, database, tableName, database, &result)
		} else {
			// 普通查询，先尝试单机方式
			err := g.DB.QueryRow(sqlQuery).Scan(&result.TableSchema, &result.TableName, &result.TableRows, &result.TableSize)
			if err != nil || result.TableName == "" || result.TableRows == 0 {
				// 单机方式失败或返回空结果，尝试集群方式
				LogQueryFailure("information_schema.tables", err, result.TableName, result.TableRows)

				// 集群部署，从CLUSTER_TABLES表查询data_length
				clusterQuery := fmt.Sprintf("SELECT table_schema, table_name, table_storage_size "+
					"FROM CLUSTER_TABLES "+
					"WHERE table_schema = '%s' AND table_name = '%s'", database, tableName)

				log.Logger.Infof("Executing cluster query: %s", clusterQuery)

				// 集群方式只能获取data_length，table_rows通过getTableRowCount获取
				clusterScanErr := g.DB.QueryRow(clusterQuery).Scan(&result.TableSchema, &result.TableName, &result.TableSize)
				if clusterScanErr != nil || result.TableName == "" {
					// 集群查询失败或返回空结果，使用估算方式
					if clusterScanErr != nil {
						log.Logger.Warnf("Failed to query from CLUSTER_TABLES, will try to estimate table size: %v", clusterScanErr)
					} else {
						log.Logger.Warnf("Query from CLUSTER_TABLES returned empty table name, will try to estimate table size")
					}

					// 使用通用函数进行估算
					FillTableInfoFromEstimation(g, database, tableName, database, &result)
				} else {
					// 集群查询成功，继续获取行数
					// 通过GetTableRowCount获取table_rows
					rowCount, err := g.GetTableRowCount(database, tableName)
					if err != nil {
						log.Logger.Errorf("Failed to get table row count: %v", err)
						// 不返回错误，只记录日志，table_rows为0
						result.TableRows = 0
					} else {
						result.TableRows = rowCount
					}
				}
			}
		}
	}

	// 获取表结构信息
	columns, err := g.getTableSchema(database, tableName)
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

// getTableSchema retrieves table structure information
func (g *GBaseStrategy) getTableSchema(database, tableName string) ([]*ds.ColumnItem, error) {
	query := `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT
              FROM INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION`

	rows, err := g.DB.Query(query, database, tableName)
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

func (g *GBaseStrategy) BuildWithConditionQuery(
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

	// Build SELECT clause
	queryBuilder.WriteString("SELECT ")
	if len(fields) > 0 {
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

	// Build WHERE clause
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

			argsFromFilter := queryTools.ExtractQueryArgs([]*ds.FilterValue{filterValue})
			if len(argsFromFilter) == 0 {
				return "", nil, fmt.Errorf("no valid values found for filter '%s'", filterName)
			}

			switch operator {
			case ds.FilterOperator_IN_OPERATOR:
				var inPlaceholders []string
				for _, v := range argsFromFilter {
					inPlaceholders = append(inPlaceholders, "?")
					args = append(args, v)
				}
				conditions[i] = fmt.Sprintf("%s IN (%s)", filterName, strings.Join(inPlaceholders, ", "))

			case ds.FilterOperator_GREATER_THAN, ds.FilterOperator_LESS_THAN,
				ds.FilterOperator_GREATER_THAN_OR_EQUAL, ds.FilterOperator_LESS_THAN_OR_EQUAL,
				ds.FilterOperator_NOT_EQUAL:
				conditions[i] = fmt.Sprintf("%s %s ?", filterName, queryTools.OperatorToString(operator))
				args = append(args, argsFromFilter[0])

			case ds.FilterOperator_LIKE_OPERATOR:
				conditions[i] = fmt.Sprintf("%s LIKE ?", filterName)
				args = append(args, argsFromFilter[0])

			default:
				conditions[i] = fmt.Sprintf("%s = ?", filterName)
				args = append(args, argsFromFilter[0])
			}
		}

		queryBuilder.WriteString(strings.Join(conditions, " AND "))
	}

	// Build ORDER BY clause
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

func (g *GBaseStrategy) EnsureDatabaseExists(dbName string) error {
	createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
	_, err := g.DB.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("failed to create GBase database '%s': %v", dbName, err)
	}
	log.Logger.Infof("Created GBase database '%s'", dbName)
	return nil
}

func (g *GBaseStrategy) CheckTableExists(tableName string) (bool, error) {
	var exists bool
	err := g.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)", tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (g *GBaseStrategy) CleanupOldTables(dbName string, retentionDays int) error {
	// Switch to specified database
	useQuery := fmt.Sprintf("USE `%s`", dbName)
	if _, err := g.DB.Exec(useQuery); err != nil {
		return fmt.Errorf("failed to switch to database '%s': %v", dbName, err)
	}

	// Calculate retention cutoff date
	cutoffDate := time.Now().AddDate(0, 0, -retentionDays).Format("20060102")

	// Query tables matching criteria
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name LIKE ?`
	rows, err := g.Query(query, dbName, "20%")
	if err != nil {
		return fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	// Iterate through results and drop expired tables
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %v", err)
		}

		// Check if table name is earlier than retention date
		if len(tableName) >= 8 && tableName[:8] < cutoffDate {
			dropQuery := fmt.Sprintf("DROP TABLE `%s`", tableName)
			if _, err := g.DB.Exec(dropQuery); err != nil {
				log.Logger.Errorf("failed to drop table '%s': %v", tableName, err)
			} else {
				log.Logger.Infof("Dropped table '%s'", tableName)
			}
		}
	}

	return nil
}

func (g *GBaseStrategy) GetGroupCountInfo(tableName string, groupBy []string, filterNames []string, filterOperators []ds.FilterOperator, filterValues []*ds.FilterValue) (*ds.GroupCountResponse, error) {
	queryTools := &QueryBuilder{}

	// Use BuildGroupCountQuery to generate SQL query and parameters
	query, args, err := queryTools.BuildGroupCountQuery(tableName, groupBy, filterNames, filterOperators,
		filterValues, ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL) // GBase uses MySQL-compatible queries
	if err != nil {
		return nil, fmt.Errorf("failed to build group count query: %v", err)
	}

	// Execute query
	log.Logger.Debugf("Executing group count query: %s with args: %v", query, args)
	rows, err := g.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute group count query: %v", err)
	}
	defer rows.Close()

	return ProcessGroupCountResults(rows, tableName)
}

// getTableRowCount 获取表的总行数
func (g *GBaseStrategy) getTableRowCount(database string, tableName string) (int32, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", database, tableName)
	var rowCount int32
	if err := g.DB.QueryRow(countQuery).Scan(&rowCount); err != nil {
		return 0, err
	}
	return rowCount, nil
}

// estimateTableSize 通过采样数据估算表的大小
func (g *GBaseStrategy) estimateTableSize(database string, tableName string, totalRows int32) (int64, error) {
	// 获取100条数据估算平均行大小
	sampleQuery := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 100", database, tableName)
	rows, err := g.DB.Query(sampleQuery)
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

// gbase8aCipherSuites 返回兼容 GBase8a 老旧服务端的 CipherSuites
// 在默认安全 Cipher 基础上追加老旧但兼容的 Cipher
func gbase8aCipherSuites() []uint16 {
	var suites []uint16
	for _, s := range tls.CipherSuites() {
		suites = append(suites, s.ID)
	}
	// 追加 GBase8a 服务端支持的老旧 Cipher,为了 tlsv1.0
	suites = append(suites,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	)
	return suites
}

// GetTableRowCount 实现 TableInfoEstimator 接口
func (g *GBaseStrategy) GetTableRowCount(database string, tableName string) (int32, error) {
	return g.getTableRowCount(database, tableName)
}

// EstimateTableSize 实现 TableInfoEstimator 接口
func (g *GBaseStrategy) EstimateTableSize(database string, tableName string, totalRows int32) (int64, error) {
	return g.estimateTableSize(database, tableName, totalRows)
}
