package testcases

import (
	"context"
	"fmt"
	"time"

	"data-integrate-test/clients"
	"data-integrate-test/utils"
	"data-integrate-test/validators"
)

// testRead 测试读取（ReadStreamingData接口）
func (te *TestExecutor) testRead(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "read",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// 调用data-service的ReadStreamingData接口
	// 注意：ChainInfoId 必须与创建资产时使用的保持一致（test_chain_001）
	// Alias 可以为空，但需要传递（data-service 会使用它）
	req := &clients.StreamReadRequest{
		AssetName:   te.assetName, // 使用资产英文名（AssetEnName）
		ChainInfoId: "test_chain_001", // 与创建资产时的 ChainInfoId 保持一致
		RequestId:   fmt.Sprintf("read_%d", time.Now().Unix()),
		DbFields:    te.getFieldNames(),
		Alias:       "", // 链账户别名，测试环境可以为空
	}

	readStart := time.Now()
	responses, err := te.dataClient.ReadStreamingData(ctx, req)
	readDuration := time.Since(readStart)

	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service读取接口失败: %w", err).Error()
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

