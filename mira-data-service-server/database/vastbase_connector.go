package database

import (
	"data-service/common"
	"data-service/config"
	ds "data-service/generated/datasource"
	pb "data-service/generated/datasource"
	"data-service/log"
	"data-service/utils"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

var (
	vastbasePoolMap = make(map[string]*sql.DB)
	vastbaseMutex   sync.Mutex
)

type VastbaseStrategy struct {
	info *ds.ConnectionInfo
	DB   *sql.DB
}

func NewVastbaseStrategy(info *ds.ConnectionInfo) *VastbaseStrategy {
	return &VastbaseStrategy{info: info}
}

func (v *VastbaseStrategy) ConnectToDB() error {
	return v.ConnectToDBWithPass(v.info)
}

func (v *VastbaseStrategy) ConnectToDBWithPass(info *ds.ConnectionInfo) error {
	dbType := utils.GetDbTypeName(info.Dbtype)
	tlsKey := "ssl-disable"
	if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
		tlsKey = fmt.Sprintf("sslmode-%d-%s", info.TlsConfig.Mode, info.TlsConfig.ServerName)
	}
	key := fmt.Sprintf("%s_%s_%d_%s_%s_%s_%s", dbType, info.Host, info.Port, info.DbName, info.User, info.Password, tlsKey)

	vastbaseMutex.Lock()
	defer vastbaseMutex.Unlock()

	if db, ok := vastbasePoolMap[key]; ok {
		v.DB = db
		log.Logger.Debug("Reusing existing Vastbase connection pool")
		return nil
	}

	// 构建 DSN，支持三种 TLS 模式
	dsn, err := buildVastbaseTLSDSN(info, info.TlsConfig)
	if err != nil {
		return fmt.Errorf("failed to build TLS DSN: %v", err)
	}

	conf := config.GetConfigMap()
	db, err := sql.Open("opengauss", dsn)
	if err != nil {
		log.Logger.Errorf("Failed to connect to Vastbase: %v", err)
		return fmt.Errorf("failed to connect to Vastbase: %v", err)
	}

	db.SetMaxOpenConns(conf.Dbms.MaxOpenConns)
	db.SetMaxIdleConns(conf.Dbms.MaxIdleConns)
	db.SetConnMaxLifetime(10 * time.Minute) // 连接最大生命周期
	db.SetConnMaxIdleTime(2 * time.Minute)  // 连接最大空闲时间

	v.DB = db
	vastbasePoolMap[key] = db
	log.Logger.Info("Successfully connected to Vastbase")
	return nil
}

func (v *VastbaseStrategy) Query(sqlQuery string, args ...interface{}) (*sql.Rows, error) {
	log.Logger.Debugf("Vastbase query: %s args=%v", sqlQuery, args)
	rows, err := v.DB.Query(sqlQuery, args...)
	if err != nil {
		log.Logger.Errorf("Vastbase query failed: %v", err)
		return nil, err
	}
	return rows, nil
}

func (v *VastbaseStrategy) Close() error {
	if v.DB == nil {
		log.Logger.Warn("Attempted to close nil Vastbase connection")
		return nil
	}
	if err := v.DB.Close(); err != nil {
		return fmt.Errorf("failed to close Vastbase connection: %v", err)
	}
	log.Logger.Info("Vastbase connection closed successfully")
	return nil
}

func (v *VastbaseStrategy) GetJdbcUrl() string {
	return fmt.Sprintf("jdbc:postgresql://%s:%d/%s?user=%s&password=%s",
		v.info.Host, v.info.Port, v.info.DbName, v.info.User, v.info.Password)
}

func (v *VastbaseStrategy) RowsToArrowBatch(rows *sql.Rows, batchSize int) (arrow.Record, error) {
	if rows == nil {
		return nil, fmt.Errorf("no rows to convert")
	}
	pool := memory.NewGoAllocator()
	cols, err := rows.Columns()
	if err != nil {
		return nil, io.EOF
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var fields []arrow.Field
	for i, col := range cols {
		arrowType, err := vastbaseTypeToArrowType(colTypes[i])
		if err != nil {
			return nil, err
		}
		fields = append(fields, arrow.Field{Name: col, Type: arrowType})
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range valuePtrs {
		valuePtrs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		for i, val := range values {
			if err := utils.AppendValueToBuilder(builder.Field(i), val); err != nil {
				return nil, err
			}
		}
		rowCount++
		if rowCount >= batchSize {
			break
		}
	}
	if rowCount == 0 {
		return nil, io.EOF
	}
	return builder.NewRecord(), nil
}

func vastbaseTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, error) {
	dbType := strings.ToUpper(colType.DatabaseTypeName())
	switch {
	case strings.Contains(dbType, "INT"):
		return arrow.PrimitiveTypes.Int64, nil
	case strings.Contains(dbType, "FLOAT") || strings.Contains(dbType, "DOUBLE"):
		return arrow.PrimitiveTypes.Float64, nil
	case strings.Contains(dbType, "NUMERIC") || strings.Contains(dbType, "DECIMAL"):
		precision, scale, ok := colType.DecimalSize()
		if !ok {
			return nil, fmt.Errorf("unable to read precision/scale for %s", colType.Name())
		}
		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
	case strings.Contains(dbType, "CHAR") || strings.Contains(dbType, "TEXT"):
		// 使用 LargeString 替代 String，支持超过 2GB 的数据
		return arrow.BinaryTypes.LargeString, nil
	case strings.Contains(dbType, "BYTEA"):
		// 使用 LargeBinary 替代 Binary，支持超过 2GB 的数据
		return arrow.BinaryTypes.LargeBinary, nil
	case strings.Contains(dbType, "TIMESTAMP"):
		// 使用 LargeString 替代 String
		return arrow.BinaryTypes.LargeString, nil
	case strings.Contains(dbType, "DATE"):
		// 使用 LargeString 替代 String
		return arrow.BinaryTypes.LargeString, nil
	default:
		// 使用 LargeString 替代 String
		return arrow.BinaryTypes.LargeString, nil
	}
}

func (v *VastbaseStrategy) CreateTemporaryTableIfNotExists(tableName string, schema *arrow.Schema) error {
	var exists bool
	err := v.DB.QueryRow(`SELECT EXISTS(
		SELECT 1 FROM information_schema.tables 
		WHERE table_schema = current_schema() AND table_name = $1)`, tableName).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	var columns []string
	for _, field := range schema.Fields() {
		columns = append(columns, fmt.Sprintf("\"%s\" %s", field.Name, convertArrowTypeToVastbase(field.Type)))
	}
	createSQL := fmt.Sprintf("CREATE TABLE \"%s\" (%s)", tableName, strings.Join(columns, ", "))
	_, err = v.DB.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create Vastbase temp table: %v", err)
	}
	return nil
}

func convertArrowTypeToVastbase(t arrow.DataType) string {
	switch t.ID() {
	case arrow.INT8, arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "REAL"
	case arrow.FLOAT64:
		return "DOUBLE PRECISION"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.STRING:
		return "VARCHAR(255)"
	case arrow.LARGE_STRING:
		return "TEXT"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		d := t.(*arrow.Decimal128Type)
		return fmt.Sprintf("NUMERIC(%d,%d)", d.Precision, d.Scale)
	case arrow.BINARY:
		return "BYTEA"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DATE32:
		return "DATE"
	default:
		return "TEXT"
	}
}

func (v *VastbaseStrategy) GetTableInfo(database, tableName string, isExactQuery bool) (*ds.TableInfoResponse, error) {
	// 定义 SQL 查询
	var sqlQuery string

	if isExactQuery {
		// 精确查询时，只查询记录数
		// 参考 kingbase 实现，直接使用表名（表在默认的 public schema 中）
		sqlQuery = fmt.Sprintf("SELECT COUNT(*) FROM %s", pqQuoteIdentifier(tableName))
	} else {
		// 普通查询，获取表的相关信息（包括表大小）
		// 使用参数化查询来防止 SQL 注入
		sqlQuery = "SELECT schemaname as table_schema, relname as table_name, n_live_tup as table_rows, pg_total_relation_size(relid) as data_length " +
			"FROM pg_stat_user_tables WHERE relname = $1 AND schemaname = $2"
	}

	log.Logger.Infof("Get table info executing query: %s with parameters: database=%s, tableName=%s", sqlQuery, database, tableName)

	var rowCount int64
	var recordSize int64
	var tableSchema, resultTableName string

	if isExactQuery {
		// 如果是精确查询，只获取记录数
		if err := v.DB.QueryRow(sqlQuery).Scan(&rowCount); err != nil {
			log.Logger.Errorf("Error executing query: %v", err)
			return nil, fmt.Errorf("error executing query: %v", err)
		}
		recordSize = 0 // 精确查询不关心表的大小
		resultTableName = tableName
		tableSchema = "public"
	} else {
		// 检查是否启用估算模式
		if ShouldUseEstimationOnly() {
			// 直接使用估算方式，跳过 pg_stat_user_tables 查询
			log.Logger.Infof("Using estimation mode (use_estimation_only enabled), skipping pg_stat_user_tables query")
			// 使用临时 TableInfoResponse 结构来调用通用函数
			var tempResult TableInfoResponse
			FillTableInfoFromEstimation(v, database, tableName, "public", &tempResult)
			// 将结果转换到 Vastbase 的变量
			tableSchema = tempResult.TableSchema
			resultTableName = tempResult.TableName
			rowCount = int64(tempResult.TableRows)
			recordSize = tempResult.TableSize
		} else {
			// 普通查询，先尝试从 pg_stat_user_tables 获取
			err := v.DB.QueryRow(sqlQuery, tableName, "public").Scan(&tableSchema, &resultTableName, &rowCount, &recordSize)
			if err != nil || resultTableName == "" || rowCount == 0 {
				// 查询失败或返回空结果，使用估算方式
				LogQueryFailure("pg_stat_user_tables", err, resultTableName, int32(rowCount))
				// 使用临时 TableInfoResponse 结构来调用通用函数
				var tempResult TableInfoResponse
				FillTableInfoFromEstimation(v, database, tableName, "public", &tempResult)
				// 将结果转换到 Vastbase 的变量
				tableSchema = tempResult.TableSchema
				resultTableName = tempResult.TableName
				rowCount = int64(tempResult.TableRows)
				recordSize = tempResult.TableSize
			}
		}
	}

	// 获取表结构信息
	columns, err := v.getTableSchema("public", tableName)
	if err != nil {
		log.Logger.Warnf("Failed to load Vastbase schema: %v", err)
	}

	log.Logger.Infof("Table info: tableName=%s, rowCount=%d, recordSize=%d", resultTableName, rowCount, recordSize)

	return &ds.TableInfoResponse{
		TableName:   resultTableName,
		RecordCount: int32(rowCount),
		RecordSize:  recordSize,
		Columns:     columns,
	}, nil
}

func (v *VastbaseStrategy) getTableSchema(schema, table string) ([]*ds.ColumnItem, error) {
	query := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`
	rows, err := v.DB.Query(query, "public", table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*ds.ColumnItem
	for rows.Next() {
		var col ds.ColumnItem
		if err := rows.Scan(&col.Name, &col.DataType); err != nil {
			return nil, err
		}
		columns = append(columns, &col)
	}
	return columns, nil
}

func (v *VastbaseStrategy) BuildWithConditionQuery(
	tableName string,
	fields []string,
	filterNames []string,
	filterOperators []ds.FilterOperator,
	filterValues []*ds.FilterValue,
	sortRules []*ds.SortRule,
) (string, []interface{}, error) {
	quoteIdentifier := func(identifier string) string {
		return fmt.Sprintf("\"%s\"", identifier)
	}

	var queryBuilder strings.Builder
	queryTools := &QueryBuilder{}
	args := make([]interface{}, 0)
	paramIndex := 1

	// 构建 SELECT 子句
	queryBuilder.WriteString("SELECT ")
	if len(fields) > 0 {
		// 先去掉反引号，再加上双引号
		quotedFields := make([]string, len(fields))
		for i, f := range fields {
			trimmed := strings.TrimSpace(f)
			// 去除两端反引号
			trimmed = strings.Trim(trimmed, "`")
			// 如果已经有双引号则不重复加
			if strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"") {
				quotedFields[i] = trimmed
			} else {
				quotedFields[i] = fmt.Sprintf("\"%s\"", trimmed)
			}
		}
		queryBuilder.WriteString(strings.Join(quotedFields, ", "))
	} else {
		queryBuilder.WriteString("*")
	}
	queryBuilder.WriteString(" FROM ")
	queryBuilder.WriteString(quoteIdentifier(tableName))

	// 构建 WHERE 子句
	if len(filterNames) > 0 && len(filterOperators) > 0 && len(filterValues) > 0 {
		queryBuilder.WriteString(" WHERE ")

		conditions := make([]string, len(filterNames))
		for i := range filterNames {
			operator := filterOperators[i]
			filterName := filterNames[i]
			filterValue := filterValues[i]

			// 使用 ExtractQueryArgs 提取参数
			argsFromFilter := queryTools.ExtractQueryArgs([]*ds.FilterValue{filterValue})

			// 根据运算符类型处理不同的条件
			switch operator {
			case ds.FilterOperator_IN_OPERATOR:
				if len(argsFromFilter) > 0 {
					// filterName字段加上普通索引，加速查询
					err := AddIndexToFilterName(v.DB, pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE, v.info.DbName, tableName, filterName)
					if err != nil {
						return "", nil, err
					}
					inPlaceholders := make([]string, len(argsFromFilter))
					for j := range argsFromFilter {
						inPlaceholders[j] = fmt.Sprintf("$%d", paramIndex)
						args = append(args, argsFromFilter[j])
						paramIndex++
					}
					conditions[i] = fmt.Sprintf("%s IN (%s)", filterName, strings.Join(inPlaceholders, ", "))
				}

			case ds.FilterOperator_GREATER_THAN, ds.FilterOperator_LESS_THAN,
				ds.FilterOperator_GREATER_THAN_OR_EQUAL, ds.FilterOperator_LESS_THAN_OR_EQUAL,
				ds.FilterOperator_NOT_EQUAL:
				if len(argsFromFilter) > 0 {
					conditions[i] = fmt.Sprintf("%s %s $%d", filterName, queryTools.OperatorToString(operator), paramIndex)
					args = append(args, argsFromFilter[0]) // 提取第一个参数
					paramIndex++
				}

			case ds.FilterOperator_LIKE_OPERATOR:
				if len(argsFromFilter) > 0 {
					conditions[i] = fmt.Sprintf("%s LIKE $%d", filterName, paramIndex)
					args = append(args, argsFromFilter[0]) // 提取第一个参数
					paramIndex++
				}

			default:
				if len(argsFromFilter) > 0 {
					conditions[i] = fmt.Sprintf("%s = $%d", filterName, paramIndex)
					args = append(args, argsFromFilter[0]) // 提取第一个参数
					paramIndex++
				}
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

func (v *VastbaseStrategy) EnsureDatabaseExists(dbName string) error {
	createSQL := fmt.Sprintf("CREATE DATABASE \"%s\"", dbName)
	if _, err := v.DB.Exec(createSQL); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("failed to create Vastbase database '%s': %v", dbName, err)
	}
	return nil
}

func (v *VastbaseStrategy) CleanupOldTables(schema string, retentionDays int) error {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_name LIKE '20%'`
	rows, err := v.DB.Query(query, schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	cutoff := time.Now().AddDate(0, 0, -retentionDays).Format("20060102")
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		if len(table) >= 8 && table[:8] < cutoff {
			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\".\"%s\"", schema, table)
			if _, err := v.DB.Exec(dropSQL); err != nil {
				log.Logger.Warnf("Failed to drop Vastbase table %s.%s: %v", schema, table, err)
			}
		}
	}
	return nil
}

func (v *VastbaseStrategy) GetGroupCountInfo(tableName string, groupBy []string, filterNames []string, filterOperators []ds.FilterOperator, filterValues []*ds.FilterValue) (*ds.GroupCountResponse, error) {
	queryTools := &QueryBuilder{}
	query, args, err := queryTools.BuildGroupCountQuery(tableName, groupBy, filterNames, filterOperators, filterValues, ds.DataSourceType_DATA_SOURCE_TYPE_VASTBASE)
	if err != nil {
		return nil, err
	}
	rows, err := v.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return ProcessGroupCountResults(rows, tableName)
}

func (v *VastbaseStrategy) CheckTableExists(tableName string) (bool, error) {
	var exists bool
	err := v.DB.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = current_schema() AND table_name = $1)`, tableName).Scan(&exists)
	return exists, err
}

func pqQuoteIdentifier(id string) string {
	return "\"" + strings.ReplaceAll(id, "\"", "\"\"") + "\""
}

// buildVastbaseTLSDSN 构建支持三种 TLS 模式的 DSN
func buildVastbaseTLSDSN(info *ds.ConnectionInfo, tlsConfig *ds.DatasourceTlsConfig) (string, error) {
	// 基础 DSN
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		info.Host, info.Port, info.User, info.Password, info.DbName)

	// 如果没有 TLS 配置或 UseTls != 2，使用 disable 模式
	if tlsConfig == nil || tlsConfig.UseTls != 2 {
		dsn += " sslmode=disable"
		return dsn, nil
	}

	// 根据 Mode 设置不同的 sslmode
	var sslmode string
	switch tlsConfig.Mode {
	case 1:
		sslmode = "require" // 要求 SSL，但不验证证书
	case 2:
		sslmode = "verify-ca" // 验证 CA 证书
	case 3:
		sslmode = "verify-full" // 验证 CA 证书和主机名
	default:
		sslmode = "require"
	}
	dsn += fmt.Sprintf(" sslmode=%s", sslmode)

	// 处理 CA 证书（Mode 2 和 3 需要）
	if tlsConfig.CaCert != "" && tlsConfig.Mode >= 2 {
		caFile, err := writeCertToTempFileFromBase64(tlsConfig.CaCert, "vastbase-ca")
		if err != nil {
			return "", fmt.Errorf("failed to write CA cert: %v", err)
		}
		dsn += fmt.Sprintf(" sslrootcert=%s", caFile)
	}

	// 处理客户端证书（如果提供）
	if tlsConfig.ClientCert != "" {
		certFile, err := writeCertToTempFileFromBase64(tlsConfig.ClientCert, "vastbase-client-cert")
		if err != nil {
			return "", fmt.Errorf("failed to write client cert: %v", err)
		}
		dsn += fmt.Sprintf(" sslcert=%s", certFile)
	}

	// 处理客户端私钥（如果提供）
	if tlsConfig.ClientKey != "" {
		keyFile, err := writeCertToTempFileFromBase64(tlsConfig.ClientKey, "vastbase-client-key")
		if err != nil {
			return "", fmt.Errorf("failed to write client key: %v", err)
		}
		dsn += fmt.Sprintf(" sslkey=%s", keyFile)
	}

	return dsn, nil
}

// getTableRowCount 获取表的总行数
func (v *VastbaseStrategy) GetTableRowCount(database string, tableName string) (int32, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pqQuoteIdentifier(tableName))
	var rowCount int32
	if err := v.DB.QueryRow(countQuery).Scan(&rowCount); err != nil {
		return 0, err
	}
	return rowCount, nil
}

// estimateTableSize 通过采样数据估算表的大小
func (v *VastbaseStrategy) EstimateTableSize(database string, tableName string, totalRows int32) (int64, error) {
	// 获取100条数据估算平均行大小
	sampleQuery := fmt.Sprintf("SELECT * FROM %s LIMIT 100", pqQuoteIdentifier(tableName))
	rows, err := v.DB.Query(sampleQuery)
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

func (v *VastbaseStrategy) getTableRowCount(tableName string) (int32, error) {
	return v.GetTableRowCount("", tableName)
}

func (v *VastbaseStrategy) estimateTableSize(tableName string, totalRows int32) (int64, error) {
	return v.EstimateTableSize("", tableName, totalRows)
}
