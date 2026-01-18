package testcases

import (
	"context"
	"fmt"
	"time"

	"data-integrate-test/clients"
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

	// 获取数据库配置
	dbConfig := te.strategy.GetConnectionInfo()

	// 调用data-service的ReadInternalData接口
	req := &pb.InternalReadRequest{
		TableName:     te.tableName,
		DbName:        dbConfig.Database,
		DbFields:      te.getFieldNames(),
		JobInstanceId: fmt.Sprintf("job_%d", time.Now().Unix()),
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

