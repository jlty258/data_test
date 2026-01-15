package gorm

import (
	"data-service/config"
	"data-service/database/gorm/models"
	ds "data-service/generated/datasource"
	"data-service/utils"
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var gormDB *gorm.DB

// InitGormConnection 初始化GORM连接
func InitGormConnection() error {
	conf := config.GetConfigMap()

	// 根据数据库类型构建不同的DSN
	var dsn string
	var dialector gorm.Dialector

	// 获取数据库类型
	dbType := utils.GetDbTypeFromName(conf.Dbms.Type)

	switch dbType {
	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_MYSQL), // MySQL
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_TIDB),  // TiDB (MySQL兼容)
		int32(ds.DataSourceType_DATA_SOURCE_TYPE_TDSQL): // TDSQL (MySQL兼容)
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			conf.Dbms.User,
			conf.Dbms.Password,
			conf.Dbms.Host,
			conf.Dbms.Port,
			conf.Dbms.Database,
		)
		dialector = mysql.Open(dsn)

	case int32(ds.DataSourceType_DATA_SOURCE_TYPE_KINGBASE): // Kingbase (PostgreSQL兼容)
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable timezone=UTC client_encoding=UTF8",
			conf.Dbms.Host,
			conf.Dbms.Port,
			conf.Dbms.User,
			conf.Dbms.Password,
			conf.Dbms.Database,
		)
		dialector = postgres.Open(dsn)

	default:
		return fmt.Errorf("unsupported database type: %s", conf.Dbms.Type)
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	gormDB = db

	// 自动迁移表结构
	err = db.AutoMigrate(
		&models.CleanupTask{},
	)
	if err != nil {
		return fmt.Errorf("failed to auto migrate: %v", err)
	}

	return nil
}

// GetGormDB 获取GORM数据库连接
func GetGormDB() *gorm.DB {
	return gormDB
}
