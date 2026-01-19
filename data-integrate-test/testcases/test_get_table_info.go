package testcases

import (
	"context"
	"fmt"
	"time"

	"data-integrate-test/validators"

	pb "data-integrate-test/generated/datasource"
)

// testGetTableInfo 测试获取表信息接口
func (te *TestExecutor) testGetTableInfo(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "get_table_info",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// 调用data-service的GetTableInfo接口
	req := &pb.TableInfoRequest{
		AssetName:   te.assetName,
		ChainInfoId: "test_chain_001",
		RequestId:   fmt.Sprintf("table_info_%d", time.Now().Unix()),
		IsExactQuery: true,
		Alias:       "",
	}

	callStart := time.Now()
	resp, err := te.dataClient.GetTableInfo(ctx, req)
	callDuration := time.Since(callStart)

	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service GetTableInfo接口失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 验证响应
	if resp == nil {
		result.Passed = false
		result.Error = "GetTableInfo返回空响应"
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 验证表名
	if resp.TableName != te.tableName {
		result.Passed = false
		result.Error = fmt.Sprintf("表名不匹配: 期望 %s, 实际 %s", te.tableName, resp.TableName)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 验证行数
	actualCount := int64(resp.RecordCount)
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
	result.Message = fmt.Sprintf("表名: %s, 行数: %d, 大小: %d 字节, 列数: %d (调用耗时: %v)",
		resp.TableName, resp.RecordCount, resp.RecordSize, len(resp.Columns), callDuration)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result
}

