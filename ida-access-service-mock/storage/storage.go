package storage

import (
	"context"
	"ida-access-service-mock/mirapb"
)

// Storage 存储接口
type Storage interface {
	// 数据源相关操作
	CreateDataSource(ctx context.Context, req *mirapb.CreateDataSourceRequest) (int32, error)
	GetDataSource(ctx context.Context, dbConnId int32) (*mirapb.GetPrivateDBConnInfoResp, error)
	ListDataSources(ctx context.Context) ([]*mirapb.GetPrivateDBConnInfoResp, error)

	// 资产相关操作
	CreateAsset(ctx context.Context, req *mirapb.CreateAssetRequest) (int32, error)
	GetAssetByEnName(ctx context.Context, assetEnName string) (*mirapb.AssetInfo, error)
	GetAssetById(ctx context.Context, assetId int32) (*mirapb.AssetInfo, error)
	ListAssets(ctx context.Context, pageNumber, pageSize int32, filters []*mirapb.Filter) ([]*mirapb.AssetItem, int64, error)
	UpdateAsset(ctx context.Context, req *mirapb.UpdateAssetRequest) (*mirapb.AssetInfo, error)
	DeleteAsset(ctx context.Context, assetId int32) error

	// 关闭连接
	Close() error
}

// StorageType 存储类型
type StorageType string

const (
	StorageTypeMemory StorageType = "memory"
	// StorageTypeMySQL  StorageType = "mysql" // 暂未实现
)

// StorageConfig 存储配置
type StorageConfig struct {
	Type StorageType
	// MySQL 配置（暂未实现）
	// MySQLHost     string
	// MySQLPort     int
	// MySQLUser     string
	// MySQLPassword string
	// MySQLDatabase string
}

// NewStorage 创建存储实例
func NewStorage(config StorageConfig) (Storage, error) {
	switch config.Type {
	case StorageTypeMemory:
		return NewMemoryStorage(), nil
	// case StorageTypeMySQL:
	// 	return NewMySQLStorage(config)
	default:
		return NewMemoryStorage(), nil
	}
}

