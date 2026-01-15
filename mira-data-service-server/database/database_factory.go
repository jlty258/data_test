/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 获取数据源连接工厂

*
*/
package database

import (
	pb "data-service/generated/datasource"
	"database/sql"
	"errors"
)

// DatabaseFactory creates a database strategy based on the database type
func DatabaseFactory(dbType pb.DataSourceType, info *pb.ConnectionInfo) (DatabaseStrategy, error) {
	switch dbType {
	case pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE:
		return NewKingbaseStrategy(info), nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL:
		return NewMySQLStrategy(info), nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_TDSQL:
		return NewMySQLStrategy(info), nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_TIDB:
		return NewMySQLStrategy(info), nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_DORIS:
		return NewDorisStrategy(info), nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_VASTBASE:
		return NewVastbaseStrategy(info), nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_GBASE:
		return NewGBaseStrategy(info), nil
	default:
		return nil, errors.New("unknown database type")
	}
}

func GetDB(dbStrategy DatabaseStrategy) *sql.DB {
	if mysqlStrategy, ok := dbStrategy.(*MySQLStrategy); ok {
		return mysqlStrategy.DB
	} else if kingbaseStrategy, ok := dbStrategy.(*KingbaseStrategy); ok {
		return kingbaseStrategy.DB
	} else if dorisStrategy, ok := dbStrategy.(*DorisStrategy); ok {
		return dorisStrategy.DB
	} else if vastbaseStrategy, ok := dbStrategy.(*VastbaseStrategy); ok {
		return vastbaseStrategy.DB
	} else if gbaseStrategy, ok := dbStrategy.(*GBaseStrategy); ok {
		return gbaseStrategy.DB
	} else {
		return nil
	}
}
