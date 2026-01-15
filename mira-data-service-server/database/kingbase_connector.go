/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接kingbase

*
*/
package database

import (
	"data-service/common"
	"data-service/config"
	ds "data-service/generated/datasource"
	pb "data-service/generated/datasource"
	"data-service/log"
	"data-service/utils"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	_ "gitea.com/kingbase/gokb"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

var (
	kingbasePoolMap = make(map[string]*sql.DB)
	kingbaseMutex   sync.Mutex
)

// KingbaseStrategy is a struct that implements DatabaseStrategy for Kingbase database
type KingbaseStrategy struct {
	info *ds.ConnectionInfo
	DB   *sql.DB
}

func NewKingbaseStrategy(info *ds.ConnectionInfo) *KingbaseStrategy {
	return &KingbaseStrategy{
		info: info,
	}
}

func (k *KingbaseStrategy) RowsToArrowBatch(rows *sql.Rows, batchSize int) (arrow.Record, error) {
	if rows == nil {
		return nil, fmt.Errorf("no rows to convert")
	}

	// 创建内存分配器
	pool := memory.NewGoAllocator()

	// 获取列名
	cols, err := rows.Columns()
	if err != nil {
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
		log.Logger.Debugf("SQL type for column %s: %s", col, colTypes[i].DatabaseTypeName())
		arrowType, err := kingbaseTypeToArrowType(colTypes[i])
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
	// 检查是否还有剩余的行数据
	if rowCount == 0 {
		// 如果没有更多数据，返回 io.EOF 表示结束
		return nil, io.EOF
	}

	// 创建 Arrow 批次 (Record)
	record := builder.NewRecord()
	return record, nil
}

// 将 SQL 列类型转换为 Arrow 类型
func kingbaseTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, error) {
	// 获取数据库类型名称
	dbTypeName := strings.ToUpper(colType.DatabaseTypeName())

	// 处理浮点数类型
	if strings.Contains(dbTypeName, "FLOAT") || strings.Contains(dbTypeName, "DOUBLE") {
		return arrow.PrimitiveTypes.Float64, nil
	}

	if dbTypeName == "REAL" {
		return arrow.PrimitiveTypes.Float32, nil
	}

	if strings.Contains(dbTypeName, "DECIMAL") || strings.Contains(dbTypeName, "NUMERIC") {
		precision, scale, ok := colType.DecimalSize()
		if !ok {
			return nil, fmt.Errorf("unable to get precision and scale for column: %s", colType.Name())
		}
		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
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

func (k *KingbaseStrategy) ConnectToDBWithPass(info *ds.ConnectionInfo) error {
	dbType := utils.GetDbTypeName(info.Dbtype)

	// 构建连接池键，包含 TLS 配置信息
	tlsKey := "no-tls"
	if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
		tlsKey = fmt.Sprintf("tls-%d-%s", info.TlsConfig.Mode, info.TlsConfig.ServerName)
	}
	key := fmt.Sprintf("%s_%s_%d_%s_%s_%s", dbType, info.Host, info.Port, info.DbName, info.Password, tlsKey)

	kingbaseMutex.Lock()
	defer kingbaseMutex.Unlock()

	// 检查连接池是否已经存在
	if db, ok := kingbasePoolMap[key]; ok {
		k.DB = db
		log.Logger.Debugf("Reusing existing Kingbase connection pool")
		return nil
	}
	// 构造 Kingbase DSN 数据源名称，包含用户名、密码和连接信息
	// dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable timezone=UTC", info.Host, info.Port, info.User, info.Password, info.DbName)
	// 构建 TLS DSN - 整合 buildKingbaseTLSDSN 功能
	if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
		log.Logger.Infof("TlsConfig config: %+v", info.TlsConfig)
	} else {
		log.Logger.Infof("Using NON-TLS connection for Kingbase")
	}
	dsn, _, err := buildKingbaseTLSDSN(info, info.TlsConfig)
	if err != nil {
		return fmt.Errorf("failed to build TLS DSN: %v", err)
	}

	if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
		log.Logger.Infof("Connecting to Kingbase with TLS enabled")
	} else {
		log.Logger.Infof("Connecting to Kingbase without TLS")
	}

	conf := config.GetConfigMap()

	// 打开数据库连接
	db, err := sql.Open("kingbase", dsn)
	if err != nil {
		log.Logger.Errorf("Failed to connect to Kingbase: %v", err)
		return fmt.Errorf("failed to connect to Kingbase: %v", err)
	}

	// Print session time zone
	var tz string
	if err := db.QueryRow("SHOW TIME ZONE").Scan(&tz); err != nil {
		log.Logger.Warnf("Failed to fetch session time zone: %v", err)
	} else {
		log.Logger.Infof("Kingbase session time zone: %s", tz)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(conf.Dbms.MaxOpenConns) // 最大打开连接数
	db.SetMaxIdleConns(conf.Dbms.MaxIdleConns) // 最大空闲连接数

	// 成功连接后，保存数据库实例
	k.DB = db
	kingbasePoolMap[key] = db
	log.Logger.Info("Successfully connected to Kingbase with username and password")
	return nil
}

func (k *KingbaseStrategy) ConnectToDB() error {
	return nil
}

func (k *KingbaseStrategy) Query(sqlQuery string, args ...interface{}) (*sql.Rows, error) {
	log.Logger.Debugf("Executing query: %s with args: %v\n", sqlQuery, args)
	// 执行查询，args 用于绑定 SQL 查询中的占位符
	rows, err := k.DB.Query(sqlQuery, args...)
	if err != nil {
		log.Logger.Errorf("Query failed: %v\n", err)
		return nil, err
	}
	log.Logger.Debugf("%s Query executed successfully", sqlQuery)

	return rows, nil

}

func (k *KingbaseStrategy) Close() error {
	// 检查数据库连接是否已经初始化
	if k.DB != nil {
		err := k.DB.Close()
		if err != nil {
			log.Logger.Errorf("Failed to close Kingbase connection: %v", err)
			return err
		}
		log.Logger.Info("Kingbase connection closed successfully")
		return nil
	}
	// 如果数据库连接未初始化，直接返回 nil
	log.Logger.Warn("Attempted to close a non-initialized DB connection")
	return nil

}

func (k *KingbaseStrategy) GetJdbcUrl() string {
	// 构建 JDBC URL
	jdbcUrl := fmt.Sprintf(
		"jdbc:kingbase8://%s:%d/%s?user=%s&password=%s",
		k.info.Host,
		k.info.Port,
		k.info.DbName,
		k.info.User,     // 添加用户名
		k.info.Password, // 添加密码
	)
	return jdbcUrl
}

func (k *KingbaseStrategy) CreateTemporaryTableIfNotExists(tableName string, schema *arrow.Schema) error {

	// 检查表是否存在
	var exists bool
	err := k.DB.QueryRow(`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)`, tableName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %v", err)
	}

	if !exists {
		// 创建临时表
		createTableSQL := buildCreateKingbaseTableSQL(tableName, schema)
		log.Logger.Infof("createTableSQL: %s", createTableSQL)
		_, err = k.DB.Exec(createTableSQL)
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
func buildCreateKingbaseTableSQL(tableName string, schema *arrow.Schema) string {
	var columns []string
	for _, field := range schema.Fields() {
		columnType := convertArrowTypeToKingbaseType(field.Type)
		columns = append(columns, fmt.Sprintf("\"%s\" %s", field.Name, columnType))
	}
	return fmt.Sprintf("CREATE TABLE \"%s\" (%s)", tableName, strings.Join(columns, ", "))
}

// convertArrowTypeToSQLType 将 Arrow 数据类型转换为 SQL 数据类型
func convertArrowTypeToKingbaseType(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.INT8:
		return "SMALLINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "SMALLINT"
	case arrow.UINT16:
		return "INTEGER"
	case arrow.UINT32:
		return "BIGINT"
	case arrow.UINT64:
		return "NUMERIC(20)"
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
	case arrow.BINARY:
		return "BYTEA"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DATE32:
		return "DATE"
	case arrow.DATE64:
		return "TIMESTAMP"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		decimalType := arrowType.(arrow.FixedWidthDataType)
		precision := decimalType.(*arrow.Decimal128Type).Precision
		scale := decimalType.(*arrow.Decimal128Type).Scale
		return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
	default:
		return "TEXT" // 默认使用 TEXT 类型
	}
}

func (k *KingbaseStrategy) GetTableInfo(schemaName string, tableName string, isExactQuery bool) (*ds.TableInfoResponse, error) {
	// 定义 SQL 查询
	var sqlQuery string

	if isExactQuery {
		// 精确查询时，只查询记录数
		// 使用 fmt.Sprintf 动态构建查询语句，注意 schemaName 和 tableName 是直接插入的
		sqlQuery = fmt.Sprintf("SELECT COUNT(*) as table_rows FROM %s", tableName)
	} else {
		// 普通查询，获取表的相关信息
		// 使用参数化查询来防止 SQL 注入，确保 schemaName 和 tableName 不被直接拼接
		sqlQuery = "SELECT schemaname as table_schema, relname as table_name, n_live_tup as table_rows, pg_total_relation_size(relid) as data_length " +
			"FROM pg_stat_user_tables WHERE relname = $1 AND schemaname = $2"
	}

	// 记录日志
	log.Logger.Infof("Get table info executing query: %s with parameters: schemaName=%s, tableName=%s", sqlQuery, schemaName, tableName)

	var result TableInfoResponse

	if isExactQuery {
		// 如果是精确查询，只获取记录数
		err := k.DB.QueryRow(sqlQuery).Scan(&result.TableRows)
		if err != nil {
			log.Logger.Errorf("Error executing query: %v", err)
			return nil, fmt.Errorf("error executing query: %v", err)
		}
		result.TableSchema = ""      // 没有表模式
		result.TableName = tableName // 返回表名
		result.TableSize = 0         // 精确查询不关心表的大小
	} else {
		// 检查是否启用估算模式
		if ShouldUseEstimationOnly() {
			// 直接使用估算方式，跳过 pg_stat_user_tables 查询
			log.Logger.Infof("Using estimation mode (use_estimation_only enabled), skipping pg_stat_user_tables query")
			FillTableInfoFromEstimation(k, "", tableName, "public", &result)
		} else {
			// 普通查询，先尝试从 pg_stat_user_tables 获取
			err := k.DB.QueryRow(sqlQuery, tableName, "public").Scan(&result.TableSchema, &result.TableName, &result.TableRows, &result.TableSize)
			if err != nil || result.TableName == "" || result.TableRows == 0 {
				// 查询失败或返回空结果，使用估算方式
				LogQueryFailure("pg_stat_user_tables", err, result.TableName, result.TableRows)
				FillTableInfoFromEstimation(k, "", tableName, "public", &result)
			}
		}
	}

	// 获取表结构信息
	columns, err := k.getTableSchema(schemaName, tableName)
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
func (k *KingbaseStrategy) getTableSchema(schemaName, tableName string) ([]*ds.ColumnItem, error) {
	query := `SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
              FROM information_schema.columns 
              WHERE table_schema = $1 AND table_name = $2
              ORDER BY ordinal_position`

	rows, err := k.DB.Query(query, "public", tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*ds.ColumnItem
	for rows.Next() {
		var col ds.ColumnItem
		var maxLength sql.NullInt64
		var isNullable, columnDefault sql.NullString

		err := rows.Scan(&col.Name, &col.DataType, &maxLength, &isNullable, &columnDefault)
		if err != nil {
			return nil, err
		}

		columns = append(columns, &col)
	}

	return columns, nil
}

func (k *KingbaseStrategy) BuildWithConditionQuery(tableName string, fields []string, filterNames []string,
	filterOperators []ds.FilterOperator, filterValues []*ds.FilterValue, sortRules []*ds.SortRule) (string, []interface{}, error) {
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
					err := AddIndexToFilterName(k.DB, pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE, k.info.DbName, tableName, filterName)
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

func (k *KingbaseStrategy) EnsureDatabaseExists(dbName string) error {
	// 检查数据库是否存在
	query := fmt.Sprintf("SELECT datname FROM pg_database WHERE datname = '%s'", dbName)
	rows, err := k.DB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to check database existence in Kingbase: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		// 数据库已存在
		log.Logger.Infof("Kingbase database '%s' already exists.", dbName)
		return nil
	}

	// 创建数据库
	createQuery := fmt.Sprintf("CREATE DATABASE %s", dbName)
	_, err = k.DB.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("failed to create Kingbase database '%s': %v", dbName, err)
	}
	log.Logger.Infof("Kingbase database '%s' created successfully.", dbName)
	return nil
}

func (k *KingbaseStrategy) CheckTableExists(tableName string) (bool, error) {
	var exists bool
	err := k.DB.QueryRow(`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)`, tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (k *KingbaseStrategy) CleanupOldTables(dbName string, retentionDays int) error {
	// 切换到指定数据库
	useQuery := fmt.Sprintf("SET search_path TO \"%s\"", dbName)
	if _, err := k.DB.Exec(useQuery); err != nil {
		return fmt.Errorf("failed to switch to database '%s': %v", dbName, err)
	}

	// 获取当前日期并计算保留的截止日期
	cutoffDate := time.Now().AddDate(0, 0, -retentionDays).Format("20060102")

	// 查询符合条件的表
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_catalog = ? AND table_name LIKE ?`
	rows, err := k.Query(query, dbName, "20%")
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
			dropQuery := fmt.Sprintf("DROP TABLE \"%s\"", tableName)
			if _, err := k.DB.Exec(dropQuery); err != nil {
				log.Logger.Errorf("failed to drop table '%s': %v", tableName, err)
			} else {
				log.Logger.Infof("Dropped table '%s'", tableName)
			}
		}
	}

	return nil
}

func (k *KingbaseStrategy) GetGroupCountInfo(tableName string, groupBy []string, filterNames []string, filterOperators []ds.FilterOperator, filterValues []*ds.FilterValue) (*ds.GroupCountResponse, error) {
	queryTools := &QueryBuilder{}

	// 使用BuildGroupCountQuery生成SQL查询语句和参数
	query, args, err := queryTools.BuildGroupCountQuery(tableName, groupBy, filterNames, filterOperators,
		filterValues, ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE)
	if err != nil {
		return nil, fmt.Errorf("failed to build group count query: %v", err)
	}

	// 执行查询
	log.Logger.Debugf("Executing group count query: %s with args: %v", query, args)
	rows, err := k.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute group count query: %v", err)
	}
	defer rows.Close()

	return ProcessGroupCountResults(rows, tableName)
}

// buildKingbaseTLSDSN 构建带 TLS 的 Kingbase DSN
func buildKingbaseTLSDSN(info *ds.ConnectionInfo, tlsConfig *ds.DatasourceTlsConfig) (string, []string, error) {
	var tempFiles []string

	// 基础 DSN
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s TimeZone=Asia/Shanghai",
		info.Host, info.Port, info.User, info.Password, info.DbName)

	if tlsConfig == nil || tlsConfig.UseTls != 2 {
		dsn += " sslmode=disable"
		return dsn, tempFiles, nil
	}

	// 设置 SSL 模式
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
	dsn += fmt.Sprintf(" sslmode=%s", sslmode)

	// 处理 CA 证书
	if tlsConfig.CaCert != "" {
		caFile, err := writeCertToTempFileFromBase64(tlsConfig.CaCert, "kingbase-ca")
		if err != nil {
			return "", tempFiles, fmt.Errorf("failed to write CA cert: %v", err)
		}
		tempFiles = append(tempFiles, caFile)
		dsn += fmt.Sprintf(" sslrootcert=%s", caFile)
	}

	// 处理客户端证书
	if tlsConfig.ClientCert != "" {
		certFile, err := writeCertToTempFileFromBase64(tlsConfig.ClientCert, "kingbase-client-cert")
		if err != nil {
			return "", tempFiles, fmt.Errorf("failed to write client cert: %v", err)
		}
		tempFiles = append(tempFiles, certFile)
		dsn += fmt.Sprintf(" sslcert=%s", certFile)
	}

	// 处理客户端私钥
	if tlsConfig.ClientKey != "" {
		keyFile, err := writeCertToTempFileFromBase64(tlsConfig.ClientKey, "kingbase-client-key")
		if err != nil {
			return "", tempFiles, fmt.Errorf("failed to write client key: %v", err)
		}
		tempFiles = append(tempFiles, keyFile)
		dsn += fmt.Sprintf(" sslkey=%s", keyFile)
	}

	return dsn, tempFiles, nil
}

// writeCertToTempFileFromBase64 从 base64 字符串创建临时证书文件
func writeCertToTempFileFromBase64(certContent, prefix string) (string, error) {
	if certContent == "" {
		return "", nil
	}

	// 解码 base64 内容
	decoded, err := base64.StdEncoding.DecodeString(certContent)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64 cert: %v", err)
	}

	// 创建临时文件
	tempFile, err := ioutil.TempFile("", prefix+"-*.pem")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %v", err)
	}

	// 写入解码后的内容
	if _, err := tempFile.Write(decoded); err != nil {
		return "", fmt.Errorf("failed to write cert to temp file: %v", err)
	}

	if err := tempFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %v", err)
	}

	return tempFile.Name(), nil
}

func (k *KingbaseStrategy) getTableRowCount(tableName string) (int32, error) {
	return k.GetTableRowCount("", tableName)
}

func (k *KingbaseStrategy) estimateTableSize(tableName string, totalRows int32) (int64, error) {
	return k.EstimateTableSize("", tableName, totalRows)
}

// getTableRowCount 获取表的总行数
func (k *KingbaseStrategy) GetTableRowCount(database string, tableName string) (int32, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var rowCount int32
	if err := k.DB.QueryRow(countQuery).Scan(&rowCount); err != nil {
		return 0, err
	}
	return rowCount, nil
}

// estimateTableSize 通过采样数据估算表的大小
func (k *KingbaseStrategy) EstimateTableSize(database string, tableName string, totalRows int32) (int64, error) {
	// 获取100条数据估算平均行大小
	sampleQuery := fmt.Sprintf("SELECT * FROM %s LIMIT 100", tableName)
	rows, err := k.DB.Query(sampleQuery)
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
