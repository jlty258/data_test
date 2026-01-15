package strategies

import (
	"context"
	"data-integrate-test/config"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type GBaseStrategy struct {
	config *config.DatabaseConfig
	db     *sql.DB
}

func NewGBaseStrategy(config *config.DatabaseConfig) *GBaseStrategy {
	return &GBaseStrategy{config: config}
}

func (g *GBaseStrategy) Connect(ctx context.Context) error {
	// GBase使用MySQL协议
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local",
		g.config.User, g.config.Password, g.config.Host, g.config.Port, g.config.Database)
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	
	g.db = db
	return nil
}

func (g *GBaseStrategy) GetDB() *sql.DB {
	return g.db
}

func (g *GBaseStrategy) GetDBType() string {
	return "gbase"
}

func (g *GBaseStrategy) GetConnectionInfo() *config.DatabaseConfig {
	return g.config
}

func (g *GBaseStrategy) Cleanup(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
	_, err := g.db.ExecContext(ctx, query)
	return err
}

func (g *GBaseStrategy) GetRowCount(ctx context.Context, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	var count int64
	err := g.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

