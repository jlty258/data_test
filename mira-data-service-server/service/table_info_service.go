package service

import (
	"data-service/config"
	"data-service/database"
	pb "data-service/generated/datasource"
	"data-service/utils"
	"fmt"

	"go.uber.org/zap"
)

type TableInfoService interface {
	GetTableInfo(requestId string, assetName string, chainId string, isExactQuery bool, alias string) (*pb.TableInfoResponse, error)
	GetTableInfoByDataSource(connInfo *pb.ConnectionInfo, tableName string, isExactQuery bool) (*pb.TableInfoResponse, error)
	GetInternalTableInfo(tableName string, dbName string, isExactQuery bool) (*pb.TableInfoResponse, error)
}

type tableInfoService struct {
	logger *zap.SugaredLogger
}

func NewTableInfoService(logger *zap.SugaredLogger) TableInfoService {
	return &tableInfoService{
		logger: logger,
	}
}

// 通过数据资产获取表信息
func (s *tableInfoService) GetTableInfo(requestId string, assetName string, chainId string, isExactQuery bool, alias string) (*pb.TableInfoResponse, error) {
	// 获取表大小信息
	connInfo, err := utils.GetDatasourceByAssetName(requestId, assetName, chainId, alias)
	if err != nil {
		s.logger.Errorf("Failed to get datasource by asset name: %v", err)
		return nil, err
	}
	s.logger.Infof("Connecting to database with info: %+v", connInfo)

	dbType := utils.ConvertDataSourceType(connInfo.Dbtype)
	if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_UNKNOWN {
		s.logger.Errorf("Failed to convert data source type: %v", connInfo.Dbtype)
		return nil, fmt.Errorf("invalid database type")
	}

	// 从数据源中读取arrow数据流
	dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
	if err != nil {
		s.logger.Errorf("Failed to create database strategy: %v", err)
		return nil, err
	}
	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		s.logger.Errorf("Failed to connect to database: %v", err)
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	tableInfo, err := dbStrategy.GetTableInfo(connInfo.DbName, connInfo.TableName, isExactQuery)
	if err != nil {
		s.logger.Errorf("Failed to get table info: %v", err)
		return nil, err
	}

	return tableInfo, nil
}

// 通过数据源信息获取表信息
func (s *tableInfoService) GetTableInfoByDataSource(connInfo *pb.ConnectionInfo, tableName string, isExactQuery bool) (*pb.TableInfoResponse, error) {
	dbType := utils.ConvertDataSourceType(connInfo.Dbtype)
	if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_UNKNOWN {
		s.logger.Errorf("Failed to convert data source type: %v", connInfo.Dbtype)
		return nil, fmt.Errorf("invalid database type")
	}

	// 从数据源中读取arrow数据流
	dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
	if err != nil {
		s.logger.Errorf("Failed to create database strategy: %v", err)
		return nil, err
	}
	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		s.logger.Errorf("Failed to connect to database: %v", err)
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	tableInfo, err := dbStrategy.GetTableInfo(connInfo.DbName, tableName, isExactQuery)
	if err != nil {
		s.logger.Errorf("Failed to get table info: %v", err)
		return nil, err
	}

	return tableInfo, nil

}

func (s *tableInfoService) GetInternalTableInfo(tableName string, dbName string, isExactQuery bool) (*pb.TableInfoResponse, error) {
	conf := config.GetConfigMap()
	connInfo := &pb.ConnectionInfo{
		Host:      conf.DorisConfig.Address,
		Port:      9030,
		User:      conf.DorisConfig.User,
		DbName:    dbName,
		Password:  conf.DorisConfig.Password,
		Dbtype:    int32(pb.DataSourceType_DATA_SOURCE_TYPE_DORIS),
		Columns:   []*pb.ColumnItem{},
		TableName: tableName,
	}

	return s.GetTableInfoByDataSource(connInfo, tableName, isExactQuery)
}
