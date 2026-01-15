package service

import (
	"data-service/config"
	pb "data-service/generated/datasource"
)

// AdjustSparkConfigByTableInfo 根据表信息调整SparkConfig参数
func AdjustSparkConfigByTableInfo(tableInfo *pb.TableInfoResponse, baseConfig *pb.SparkConfig) *pb.SparkConfig {
	if tableInfo == nil {
		return getDefaultSparkConfig()
	}

	// 获取配置
	conf := config.GetConfigMap()
	executionConfigs := conf.SparkPodConfig.SparkExecutionConfigs

	// 根据记录数量选择合适的配置
	var selectedConfig *config.SparkExecutionConfig
	recordCount := tableInfo.GetRecordCount()

	// 按数据量级别选择配置
	if int64(recordCount) <= executionConfigs.SmallData.RecordCountThreshold {
		selectedConfig = &executionConfigs.SmallData
	} else if int64(recordCount) <= executionConfigs.MediumData.RecordCountThreshold {
		selectedConfig = &executionConfigs.MediumData
	} else if int64(recordCount) <= executionConfigs.LargeData.RecordCountThreshold {
		selectedConfig = &executionConfigs.LargeData
	} else if int64(recordCount) <= executionConfigs.XLargeData.RecordCountThreshold {
		selectedConfig = &executionConfigs.XLargeData
	} else {
		// 超过最大配置，使用最大配置
		selectedConfig = &executionConfigs.XLargeData
	}

	// 创建新的配置
	config := &pb.SparkConfig{
		DynamicAllocationEnabled:      selectedConfig.DynamicAllocationEnabled,
		DynamicAllocationMinExecutors: selectedConfig.DynamicAllocationMinExecutors,
		DynamicAllocationMaxExecutors: selectedConfig.DynamicAllocationMaxExecutors,
		ExecutorMemoryMB:              selectedConfig.ExecutorMemoryMB,
		ExecutorCores:                 selectedConfig.ExecutorCores,
		DriverMemoryMB:                selectedConfig.DriverMemoryMB,
		DriverCores:                   selectedConfig.DriverCores,
		Parallelism:                   selectedConfig.Parallelism,
		NumPartitions:                 selectedConfig.NumPartitions,
	}

	return config
}

// getDefaultSparkConfig 返回默认的Spark配置
func getDefaultSparkConfig() *pb.SparkConfig {
	return &pb.SparkConfig{
		DynamicAllocationEnabled:      true,
		DynamicAllocationMinExecutors: 1,
		DynamicAllocationMaxExecutors: 10,
		ExecutorMemoryMB:              1024, // 1GB
		ExecutorCores:                 2,
		DriverMemoryMB:                512, // 512MB
		DriverCores:                   1,
		Parallelism:                   10,
		NumPartitions:                 10,
	}
}
