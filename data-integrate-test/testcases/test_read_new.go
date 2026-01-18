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

// testReadNew 测试新版通用读接口
func (te *TestExecutor) testReadNew(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "read_new",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// 获取数据库配置
	dbConfig := te.strategy.GetConnectionInfo()

	// 构建外部数据源
	externalDS := &pb.ExternalDataSource{
		AssetName:   te.assetName,
		ChainInfoId: "test_chain_001",
		Alias:       "",
	}

	// 调用data-service的新版Read接口
	req := &pb.ReadRequest{
		DataSource: &pb.ReadRequest_External{
			External: externalDS,
		},
		Columns: te.getFieldNames(),
		// SortRules 和 FilterConditions 可以从 test.Params 中获取
	}

	readStart := time.Now()
	responses, err := te.dataClient.Read(ctx, req)
	readDuration := time.Since(readStart)

	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service新版Read接口失败: %w", err).Error()
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
	result.Message = fmt.Sprintf("%s (data-service新版Read接口调用耗时: %v)", validationResult.Message, readDuration)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result
}

