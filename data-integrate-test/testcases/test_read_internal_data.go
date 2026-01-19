package testcases

import (
	"context"
	"fmt"
	"time"

	"data-integrate-test/utils"
	"data-integrate-test/validators"

	pb "data-integrate-test/generated/datasource"
)

// testReadInternalData 测试从内置数据库读取数据接口
func (te *TestExecutor) testReadInternalData(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "read_internal",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// ReadInternalData 需要数据已经写入到内置数据库
	// 如果表不存在，跳过测试并提示
	// 注意：实际使用中，应该先通过 WriteInternalData 写入数据

	// ReadInternalData 需要从内置数据库读取
	// 注意：ReadInternalData 使用配置的数据库（MySQL），不是Doris
	// 它使用 request.DbName 作为数据库名，但连接的是配置的MySQL
	// 表名格式为 jobInstanceId_tableName（如果提供了 JobInstanceId）
	// 实际使用中，应该先通过 WriteInternalData 写入数据到内置数据库
	
	// 获取数据库配置
	dbConfig := te.strategy.GetConnectionInfo()
	
	// 调用data-service的ReadInternalData接口
	// 注意：ReadInternalData 会连接到配置的MySQL数据库，使用 request.DbName 作为数据库名
	// 如果表不存在，这个测试会失败（这是预期的，需要先写入数据）
	req := &pb.InternalReadRequest{
		TableName:     te.tableName,
		DbName:        dbConfig.Database, // 使用MySQL数据库名
		DbFields:      te.getFieldNames(),
		JobInstanceId: "", // 不提供 JobInstanceId，直接使用表名
	}

	readStart := time.Now()
	responses, err := te.dataClient.ReadInternalData(ctx, req)
	readDuration := time.Since(readStart)

	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service ReadInternalData接口失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 统计行数（从所有Arrow响应中累计）
	actualCount := int64(0)
	for _, resp := range responses {
		if resp != nil {
			arrowBatch := resp.GetArrowBatch()
			// 跳过EOF标记和空批次
			if len(arrowBatch) == 0 {
				continue
			}
			// 检查是否是EOF标记
			if len(arrowBatch) == 3 && string(arrowBatch) == "EOF" {
				continue // EOF标记，跳过
			}
			count, err := utils.CountRowsFromArrow(arrowBatch)
			if err != nil {
				result.Passed = false
				result.Error = fmt.Errorf("解析Arrow数据失败: %w", err).Error()
				result.EndTime = time.Now()
				result.Duration = result.EndTime.Sub(result.StartTime)
				return result
			}
			actualCount += count
		}
	}

	// 验证行数
	validationResult, err := validator.ValidateReadResult(ctx, actualCount)
	if err != nil {
		result.Passed = false
		result.Error = err.Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	result.Passed = validationResult.Passed
	result.Actual = validationResult.Actual
	result.Diff = validationResult.Diff
	result.DiffPercent = validationResult.DiffPercent
	result.Message = fmt.Sprintf("%s (data-service调用耗时: %v)", validationResult.Message, readDuration)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result
}

