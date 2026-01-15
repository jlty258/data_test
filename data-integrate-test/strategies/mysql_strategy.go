package strategies

import (
	"context"
	"data-integrate-test/config"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLStrategy struct {
	config *config.DatabaseConfig
	db     *sql.DB
}

func NewMySQLStrategy(config *config.DatabaseConfig) *MySQLStrategy {
	return &MySQLStrategy{config: config}
}

func (m *MySQLStrategy) Connect(ctx context.Context) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local",
		m.config.User, m.config.Password, m.config.Host, m.config.Port, m.config.Database)
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	
	m.db = db
	return nil
}

func (m *MySQLStrategy) GetDB() *sql.DB {
	return m.db
}

func (m *MySQLStrategy) GetDBType() string {
	return "mysql"
}

func (m *MySQLStrategy) GetConnectionInfo() *config.DatabaseConfig {
	return m.config
}

func (m *MySQLStrategy) Cleanup(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
	_, err := m.db.ExecContext(ctx, query)
	return err
}

func (m *MySQLStrategy) GetRowCount(ctx context.Context, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	var count int64
	err := m.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

