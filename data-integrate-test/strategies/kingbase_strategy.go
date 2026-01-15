package strategies

import (
	"context"
	"data-integrate-test/config"
	"database/sql"
	"fmt"
	_ "gitea.com/kingbase/gokb"
)

type KingBaseStrategy struct {
	config *config.DatabaseConfig
	db     *sql.DB
}

func NewKingBaseStrategy(config *config.DatabaseConfig) *KingBaseStrategy {
	return &KingBaseStrategy{config: config}
}

func (k *KingBaseStrategy) Connect(ctx context.Context) error {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		k.config.Host, k.config.Port, k.config.User, k.config.Password, k.config.Database)
	
	db, err := sql.Open("kingbase", dsn)
	if err != nil {
		return err
	}
	
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	
	k.db = db
	return nil
}

func (k *KingBaseStrategy) GetDB() *sql.DB {
	return k.db
}

func (k *KingBaseStrategy) GetDBType() string {
	return "kingbase"
}

func (k *KingBaseStrategy) GetConnectionInfo() *config.DatabaseConfig {
	return k.config
}

func (k *KingBaseStrategy) Cleanup(ctx context.Context, tableName string) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName)
	_, err := k.db.ExecContext(ctx, query)
	return err
}

func (k *KingBaseStrategy) GetRowCount(ctx context.Context, tableName string) (int64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)
	var count int64
	err := k.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

