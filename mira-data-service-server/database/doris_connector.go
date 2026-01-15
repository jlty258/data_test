/*
*

	@author: shiliang
	@date: 2025/09/23
	@note: Doris专用连接器，实现DatabaseStrategy接口，统一连接池管理

*
*/
package database

import (
	"context"
	"data-service/common"
	"data-service/config"
	ds "data-service/generated/datasource"
	"data-service/log"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dorisDB *sql.DB
	once    sync.Once
)

// DorisStrategy Doris专用策略，实现DatabaseStrategy接口
type DorisStrategy struct {
	Info *ds.ConnectionInfo
	DB   *sql.DB
}

// NewDorisStrategy 创建Doris策略（统一连接池）
func NewDorisStrategy(info *ds.ConnectionInfo) *DorisStrategy {
	return &DorisStrategy{
		Info: info,
	}
}

// ConnectToDB 实现DatabaseStrategy接口
func (d *DorisStrategy) ConnectToDB() error {
	return nil
}

// ConnectToDBWithPass 实现DatabaseStrategy接口
func (d *DorisStrategy) ConnectToDBWithPass(info *ds.ConnectionInfo) error {
	var err error
	once.Do(func() {
		// 只创建一次连接池
		var dsn string

		// TLS 分支
		log.Logger.Infof("TlsConfig: %+v", info.TlsConfig)
		if info.TlsConfig != nil && info.TlsConfig.UseTls == 2 {
			if tlsErr := setupTLSConfig(info.TlsConfig); tlsErr != nil {
				err = fmt.Errorf("failed to setup TLS configuration: %v", tlsErr)
				return
			}
			// Doris 使用 MySQL 协议，带上 tls 参数
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/?tls=%s&parseTime=true&loc=UTC",
				info.User, info.Password, info.Host, info.Port, common.MYSQL_TLS_CONFIG)
			log.Logger.Infof("Connecting to Doris (MySQL protocol) with TLS enabled")
		} else {
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true&loc=UTC",
				info.User, info.Password, info.Host, info.Port)
			log.Logger.Infof("Connecting to Doris (MySQL protocol) without TLS")
		}

		db, dbErr := sql.Open("mysql", dsn)
		if dbErr != nil {
			err = dbErr
			return
		}

		// 设置连接池参数
		config := config.GetConfigMap()
		db.SetMaxOpenConns(config.DorisConfig.MaxOpenConns)
		db.SetMaxIdleConns(config.DorisConfig.MaxIdleConns)
		db.SetConnMaxLifetime(time.Duration(config.DorisConfig.MaxLifeTime) * time.Minute)
		db.SetConnMaxIdleTime(time.Duration(config.DorisConfig.MaxIdleTime) * time.Minute)

		dorisDB = db
	})

	if err != nil {
		return err
	}

	d.DB = dorisDB
	d.Info = info // 保存连接信息

	// 切换到目标数据库
	if info.DbName != "" {
		useSQL := fmt.Sprintf("USE `%s`", info.DbName)
		_, err = d.DB.Exec(useSQL)
		if err != nil {
			log.Logger.Errorf("Failed to switch to database '%s': %v", info.DbName, err)
			return err
		}
		log.Logger.Infof("Switched to database: %s", info.DbName)
	}

	// 打印连接池状态
	d.PrintConnectionPoolStats()

	return nil
}

// Query 实现DatabaseStrategy接口
func (d *DorisStrategy) Query(sqlQuery string, args ...interface{}) (*sql.Rows, error) {
	log.Logger.Debugf("Executing query: %s with args: %v", sqlQuery, args)
	rows, err := d.DB.Query(sqlQuery, args...)
	if err != nil {
		log.Logger.Errorf("Query failed: %v", err)
		return nil, err
	}
	log.Logger.Debugf("Query executed successfully")
	return rows, nil
}

// Close 实现DatabaseStrategy接口
func (d *DorisStrategy) Close() error {
	if d.DB != nil {
		err := d.DB.Close()
		if err != nil {
			log.Logger.Errorf("Failed to close Doris connection: %v", err)
			return err
		}
		log.Logger.Info("Doris connection closed successfully")
	}
	return nil
}

// GetJdbcUrl 实现DatabaseStrategy接口
func (d *DorisStrategy) GetJdbcUrl() string {
	return ""
}

// RowsToArrowBatch 实现DatabaseStrategy接口
func (d *DorisStrategy) RowsToArrowBatch(rows *sql.Rows, batchSize int) (arrow.Record, error) {
	return nil, nil
}

// CreateTemporaryTableIfNotExists 实现DatabaseStrategy接口
func (d *DorisStrategy) CreateTemporaryTableIfNotExists(tableName string, schema *arrow.Schema) error {
	return nil
}

// GetTableInfo 实现DatabaseStrategy接口
func (d *DorisStrategy) GetTableInfo(database string, tableName string, isExactQuery bool) (*ds.TableInfoResponse, error) {
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
		if err := d.DB.QueryRow(sqlQuery).Scan(&result.TableRows); err != nil {
			return nil, err
		}
		result.TableSchema = ""      // 没有表模式
		result.TableName = tableName // 返回表名
		result.TableSize = 0         // 精确查询不关心表的大小
	} else {
		// 普通查询，返回更多的表信息
		if err := d.DB.QueryRow(sqlQuery).Scan(&result.TableSchema, &result.TableName, &result.TableRows, &result.TableSize); err != nil {
			return nil, err
		}
	}

	// 获取表结构信息
	columns, err := d.getTableSchema(database, tableName)
	if err != nil {
		log.Logger.Errorf("Failed to get table schema: %v", err)
		// 不返回错误，只记录日志，表结构信息为空
	}

	// 返回查询结果
	return &ds.TableInfoResponse{
		TableName:   result.TableName,
		RecordCount: result.TableRows,
		RecordSize:  result.TableSize,
		Columns:     columns,
	}, nil
}

// 获取表结构信息
func (d *DorisStrategy) getTableSchema(database, tableName string) ([]*ds.ColumnItem, error) {
	query := `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT
              FROM INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION`

	rows, err := d.DB.Query(query, database, tableName)
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

// BuildWithConditionQuery 实现DatabaseStrategy接口
func (d *DorisStrategy) BuildWithConditionQuery(
	tableName string,
	fields []string,
	filterNames []string,
	filterOperators []ds.FilterOperator,
	filterValues []*ds.FilterValue,
	sortRules []*ds.SortRule,
) (string, []interface{}, error) {
	// 实现条件查询构建逻辑
	// 这里可以根据需要实现具体的查询构建
	return "", nil, fmt.Errorf("BuildWithConditionQuery not implemented for Doris")
}

// EnsureDatabaseExists 实现DatabaseStrategy接口
func (d *DorisStrategy) EnsureDatabaseExists(dbName string) error {
	createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
	_, err := d.DB.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("failed to create Doris database '%s': %v", dbName, err)
	}
	log.Logger.Infof("Created Doris database '%s'", dbName)
	return nil
}

// CleanupOldTables 实现DatabaseStrategy接口
func (d *DorisStrategy) CleanupOldTables(dbName string, retentionDays int) error {
	// 实现旧表清理逻辑
	return fmt.Errorf("CleanupOldTables not implemented for Doris")
}

// GetGroupCountInfo 实现DatabaseStrategy接口
func (d *DorisStrategy) GetGroupCountInfo(tableName string, groupBy []string, filterNames []string, filterOperators []ds.FilterOperator, filterValues []*ds.FilterValue) (*ds.GroupCountResponse, error) {
	// 实现分组统计逻辑
	return nil, fmt.Errorf("GetGroupCountInfo not implemented for Doris")
}

// CheckTableExists 实现DatabaseStrategy接口
func (d *DorisStrategy) CheckTableExists(tableName string) (bool, error) {
	var exists bool
	err := d.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		common.MIRA_TMP_TASK_DB, tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// ExecInDB 在专用连接上执行写操作（先 USE，再 Exec）
func (d *DorisStrategy) ExecInDB(ctx context.Context, dbName string, sqlQuery string, args ...interface{}) (sql.Result, error) {
	if d.DB == nil {
		return nil, fmt.Errorf("nil DB")
	}
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if dbName != "" {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s`", dbName)); err != nil {
			return nil, fmt.Errorf("failed to USE database '%s': %v", dbName, err)
		}
	}

	return conn.ExecContext(ctx, sqlQuery, args...)
}

// QueryInDB: 在专用连接上执行读操作（先 USE，再 Query）
// 返回 rows 与 done 清理函数；调用方在处理完 rows 后必须调用 done()
func (d *DorisStrategy) QueryInDB(ctx context.Context, dbName string, sqlQuery string, args ...interface{}) (*sql.Rows, func(), error) {
	if d.DB == nil {
		return nil, func() {}, fmt.Errorf("nil DB")
	}
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return nil, func() {}, err
	}

	if dbName != "" {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s`", dbName)); err != nil {
			conn.Close()
			return nil, func() {}, fmt.Errorf("failed to USE database '%s': %v", dbName, err)
		}
	}

	rows, err := conn.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		conn.Close()
		return nil, func() {}, err
	}

	// 由调用方负责清理：先关 rows，再归还该物理连接
	done := func() {
		_ = rows.Close()
		_ = conn.Close()
	}
	return rows, done, nil
}

// PrintConnectionPoolStats 打印详细的连接池统计信息
func (d *DorisStrategy) PrintConnectionPoolStats() {
	if d.DB == nil {
		log.Logger.Warn("Database connection is nil, cannot get pool stats")
		return
	}

	stats := d.DB.Stats()
	config := config.GetConfigMap()

	log.Logger.Infof("=== Doris连接池详细状态 ===")

	log.Logger.Infof("运行状态:")
	log.Logger.Infof("  - 当前总连接数: %d", stats.OpenConnections)
	log.Logger.Infof("  - 使用中连接数: %d", stats.InUse)
	log.Logger.Infof("  - 空闲连接数: %d", stats.Idle)
	log.Logger.Infof("  - 等待连接请求数: %d", stats.WaitCount)
	log.Logger.Infof("  - 总等待时长: %v", stats.WaitDuration)
	log.Logger.Infof("  - 连接池利用率: %.1f%%", float64(stats.InUse)/float64(config.DorisConfig.MaxOpenConns)*100)

	// 健康状态检查
	if stats.WaitCount > 0 {
		log.Logger.Warnf("有%d个请求在等待连接，考虑增加最大连接数", stats.WaitCount)
	}

	if stats.OpenConnections == config.DorisConfig.MaxOpenConns {
		log.Logger.Warnf("连接池已满，当前连接数达到最大值%d", config.DorisConfig.MaxOpenConns)
	}

	idleRatio := float64(stats.Idle) / float64(stats.OpenConnections) * 100
	if stats.OpenConnections > 0 && idleRatio > 80 {
		log.Logger.Infof("空闲连接比例较高(%.1f%%)，可考虑减少最大空闲连接数", idleRatio)
	}

	log.Logger.Infof("=== 连接池状态结束 ===")
}
