package strategies

import (
	"context"
	"data-integrate-test/config"
	"database/sql"
	"fmt"
)

// DatabaseStrategy 数据库策略接口
type DatabaseStrategy interface {
	// 连接数据库
	Connect(ctx context.Context) error
	
	// 获取数据库连接
	GetDB() *sql.DB
	
	// 获取数据库类型
	GetDBType() string
	
	// 获取连接信息
	GetConnectionInfo() *config.DatabaseConfig
	
	// 清理测试数据
	Cleanup(ctx context.Context, tableName string) error
	
	// 查询表行数
	GetRowCount(ctx context.Context, tableName string) (int64, error)
}

// DatabaseStrategyFactory 策略工厂
type DatabaseStrategyFactory struct{}

func NewDatabaseStrategyFactory() *DatabaseStrategyFactory {
	return &DatabaseStrategyFactory{}
}

func (f *DatabaseStrategyFactory) CreateStrategy(dbConfig *config.DatabaseConfig) (DatabaseStrategy, error) {
	switch dbConfig.Type {
	case "mysql":
		return NewMySQLStrategy(dbConfig), nil
	case "kingbase":
		return NewKingBaseStrategy(dbConfig), nil
	case "gbase":
		return NewGBaseStrategy(dbConfig), nil
	case "vastbase":
		return NewVastbaseStrategy(dbConfig), nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbConfig.Type)
	}
}

