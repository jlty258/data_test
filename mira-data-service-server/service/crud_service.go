package service

import (
	"data-service/database"
	pb "data-service/generated/datasource"
	"fmt"

	"go.uber.org/zap"
)

// CrudService 定义数据库表操作接口
type CrudService interface {
	// TruncateTable 清空指定表的所有数据
	TruncateTable(tableName string) (*pb.TruncateTableResponse, error)
}

// DefaultCrudService 是 CrudService 接口的默认实现
type DefaultCrudService struct {
	logger     *zap.SugaredLogger
	dbStrategy database.DatabaseStrategy
}

// NewCrudService 创建一个新的 CrudService 实例
func NewCrudService(logger *zap.SugaredLogger, dbStrategy database.DatabaseStrategy) CrudService {
	return &DefaultCrudService{
		logger:     logger,
		dbStrategy: dbStrategy,
	}
}

// TruncateTable 实现表清空功能
func (s *DefaultCrudService) TruncateTable(tableName string) (*pb.TruncateTableResponse, error) {
	// 检查表是否存在，如果表存在，清空表，不存在则正常返回
	exists, err := s.dbStrategy.CheckTableExists(tableName)
	if err != nil {
		s.logger.Errorf("Failed to check table exists: %v", err)
		return &pb.TruncateTableResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to check table exists: %v", err),
		}, err
	}
	if !exists {
		s.logger.Infof("Table %s does not exist", tableName)
		return &pb.TruncateTableResponse{
			Success: true,
			Message: "Table does not exist",
		}, nil
	}

	// 根据数据库类型构造不同的 TRUNCATE 语句
	var sqlQuery string

	// 通过查看 dbStrategy 的具体类型来确定数据库类型
	switch s.dbStrategy.(type) {
	case *database.MySQLStrategy:
		// MySQL/TiDB/TDSQL 语法
		sqlQuery = fmt.Sprintf("TRUNCATE TABLE `%s`", tableName)
	case *database.KingbaseStrategy:
		// KingBase 语法
		sqlQuery = fmt.Sprintf("TRUNCATE TABLE \"%s\"", tableName)
	default:
		// 默认语法
		sqlQuery = fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	}

	s.logger.Infof("Truncate table %s with sql: %s", tableName, sqlQuery)

	// 执行 TRUNCATE 语句
	db := database.GetDB(s.dbStrategy)
	_, err = db.Exec(sqlQuery)
	if err != nil {
		s.logger.Errorf("Failed to truncate table %s: %v", tableName, err)
		return &pb.TruncateTableResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to truncate table: %v", err),
		}, err
	}

	s.logger.Infof("Successfully truncated table %s", tableName)
	return &pb.TruncateTableResponse{
		Success: true,
		Message: "Table truncated successfully",
	}, nil
}
