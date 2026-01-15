/*
*

	@author: shiliang
	@date: 2025/01/04
	@note: 表信息估算相关接口和辅助函数

*
*/
package database

import (
	"data-service/config"
	"data-service/log"
	"database/sql"
)

// TableInfoEstimator 表信息估算器接口
// 用于统一各个数据库连接器的表信息估算方法
type TableInfoEstimator interface {
	GetTableRowCount(database string, tableName string) (int32, error)
	EstimateTableSize(database string, tableName string, totalRows int32) (int64, error)
}

// TableInfoEstimationHelper 表信息估算辅助结构
type TableInfoEstimationHelper struct {
	DB *sql.DB
}

// EstimateTableInfo 估算表信息（行数和大小）
func (h *TableInfoEstimationHelper) EstimateTableInfo(
	database string,
	tableName string,
	getRowCountFunc func() (int32, error),
	estimateSizeFunc func(rowCount int32) (int64, error),
) (rowCount int32, size int64, err error) {
	// 获取行数
	rowCount, err = getRowCountFunc()
	if err != nil {
		log.Logger.Errorf("Failed to get table row count for size estimation: %v", err)
		return 0, 0, err
	}

	// 估算表大小
	if rowCount > 0 {
		size, err = estimateSizeFunc(rowCount)
		if err != nil {
			log.Logger.Warnf("Failed to estimate table size: %v", err)
			// 估算失败不影响返回，size保持为0
			return rowCount, 0, nil
		}
	}

	return rowCount, size, nil
}

// LogQueryFailure 记录查询失败日志，区分不同的失败原因
func LogQueryFailure(source string, err error, tableName string, rowCount int32) {
	if err != nil {
		log.Logger.Warnf("Failed to query from %s, will try to estimate table size: %v", source, err)
	} else if tableName == "" {
		log.Logger.Warnf("Query from %s returned empty table name, will try to estimate table size", source)
	} else {
		log.Logger.Warnf("Query from %s returned zero rows, will try to estimate table size", source)
	}
}

// ShouldUseEstimationOnly 检查配置是否启用估算模式
func ShouldUseEstimationOnly() bool {
	conf := config.GetConfigMap()
	return conf.Dbms.UseEstimationOnly
}

// FillTableInfoFromEstimation 根据估算结果填充表信息结构
func FillTableInfoFromEstimation(
	estimator TableInfoEstimator,
	database string,
	tableName string,
	defaultSchema string,
	result *TableInfoResponse,
) {
	// 设置基本信息
	result.TableSchema = defaultSchema
	result.TableName = tableName
	result.TableSize = 0

	// 获取行数
	rowCount, err := estimator.GetTableRowCount(database, tableName)
	if err != nil {
		log.Logger.Errorf("Failed to get table row count for size estimation: %v", err)
		result.TableRows = 0
		result.TableSize = 0
		return
	}

	result.TableRows = rowCount

	// 估算表大小
	if rowCount > 0 {
		size, err := estimator.EstimateTableSize(database, tableName, rowCount)
		if err != nil {
			log.Logger.Warnf("Failed to estimate table size: %v", err)
			// 估算失败不影响返回，size保持为0
			result.TableSize = 0
		} else {
			// 从配置读取系数，默认1.0
			conf := config.GetConfigMap()
			factor := 1.0
			if conf != nil && conf.Dbms.TableSizeFactor > 0 {
				factor = conf.Dbms.TableSizeFactor
			}
			// 应用估算系数
			result.TableSize = int64(float64(size) * factor)
		}
	}
}
