/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接数据库接口

*
*/
package database

import (
	pb "data-service/generated/datasource"
	"database/sql"

	"github.com/apache/arrow/go/v15/arrow"
)

type DatabaseStrategy interface {
	ConnectToDB() error

	// 使用密码连接数据库
	ConnectToDBWithPass(info *pb.ConnectionInfo) error

	/* 执行select，args为sql查询中的占位符
	sqlQuery := "SELECT * FROM users WHERE age > ? AND city = ?"
	rows, err := mySQLStrategy.Query(sqlQuery, 25, "New York")
	*/
	Query(sqlQuery string, args ...interface{}) (*sql.Rows, error)
	// 执行insert

	// 关闭连接
	Close() error

	GetJdbcUrl() string

	RowsToArrowBatch(rows *sql.Rows, batchSize int) (arrow.Record, error)

	// 创建临时表
	CreateTemporaryTableIfNotExists(tableName string, schema *arrow.Schema) error

	// todo 返回数据资产的数据量，用来决策是否使用spark
	GetTableInfo(database string, tableName string, isExactQuery bool) (*pb.TableInfoResponse, error)

	BuildWithConditionQuery(tableName string,
		fields []string,
		filterNames []string,
		filterOperators []pb.FilterOperator,
		filterValues []*pb.FilterValue,
		sortRules []*pb.SortRule) (string, []interface{}, error)

	EnsureDatabaseExists(dbName string) error

	CleanupOldTables(dbName string, retentionDays int) error

	GetGroupCountInfo(tableName string, groupBy []string, filterNames []string, filterOperators []pb.FilterOperator, filterValues []*pb.FilterValue) (*pb.GroupCountResponse, error)

	CheckTableExists(tableName string) (bool, error)
}
