package testcases

import (
	"context"
	"fmt"
	"time"

	"data-integrate-test/validators"

	pb "data-integrate-test/generated/datasource"
)

// testImportData 测试ImportData接口
func (te *TestExecutor) testImportData(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "import_data",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// 构建ImportData请求
	// ImportData 从外部数据源导入数据到Doris
	// 需要指定：
	// - ExternalDataSource (assetName, chainInfoId, alias)
	// - targetTableName (Doris中的目标表名)
	// - dbName (Doris数据库名)
	// - columns (要导入的字段，可选)
	// - keys (表键信息，可选)
	
	targetDbName := "mira_task_tmp" // Doris临时数据库
	if dbName, ok := test.Params["target_db"].(string); ok && dbName != "" {
		targetDbName = dbName
	}
	
	targetTableName := fmt.Sprintf("test_import_%s_%d", te.tableName, time.Now().Unix())
	if tableName, ok := test.Params["target_table"].(string); ok && tableName != "" {
		targetTableName = tableName
	}

	// 构建外部数据源
	externalDS := &pb.ExternalDataSource{
		AssetName:   te.assetName,
		ChainInfoId: "test_chain_001",
		Alias:       "",
	}

	// 构建导入目标
	importTarget := &pb.ImportTarget{
		External:        externalDS,
		TargetTableName: targetTableName,
		DbName:          targetDbName,
		Columns:         te.getFieldNames(), // 导入所有字段，如果为空则导入所有
		Keys:            []*pb.TableKey{},   // 可选：表键信息
	}

	// 构建ImportData请求
	req := &pb.ImportDataRequest{
		Targets: []*pb.ImportTarget{importTarget},
	}

	importStart := time.Now()
	importResp, err := te.dataClient.ImportData(ctx, req)
	importDuration := time.Since(importStart)

	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service ImportData接口失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 检查响应
	if importResp == nil {
		result.Passed = false
		result.Error = "ImportData响应为空"
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	if !importResp.Success {
		result.Passed = false
		if len(importResp.Results) > 0 && importResp.Results[0].ErrorMessage != "" {
			result.Error = fmt.Sprintf("导入失败: %s", importResp.Results[0].ErrorMessage)
		} else {
			result.Error = fmt.Sprintf("导入失败: %s", importResp.Message)
		}
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 检查导入结果
	if len(importResp.Results) == 0 {
		result.Passed = false
		result.Error = "导入结果为空"
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	importResult := importResp.Results[0]
	if !importResult.Success {
		result.Passed = false
		result.Error = fmt.Sprintf("导入失败: %s", importResult.ErrorMessage)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 验证导入的行数
	actualCount := importResult.AffectedRows
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
	result.Message = fmt.Sprintf("%s (导入耗时: %v, 目标表: %s.%s, 实际导入行数: %d)",
		validationResult.Message, importDuration, targetDbName, importResult.TargetTableName, actualCount)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result
}

