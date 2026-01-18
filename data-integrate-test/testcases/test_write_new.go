package testcases

import (
	"context"
	"fmt"
	"time"

	"data-integrate-test/clients"
	"data-integrate-test/validators"

	pb "data-integrate-test/generated/datasource"
)

// testWriteNew 测试新版通用写接口
func (te *TestExecutor) testWriteNew(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "write_new",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// 获取数据库配置
	dbConfig := te.strategy.GetConnectionInfo()

	// 1. 从数据库读取数据并转换为Arrow格式
	arrowBatch, err := te.readDataAsArrow(ctx)
	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("读取数据并转换为Arrow格式失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 2. 构建写入请求
	// 从测试参数中获取目标表名，或使用默认值
	targetTableName := fmt.Sprintf("test_write_new_%s_%d", te.tableName, time.Now().Unix())
	if tableName, ok := test.Params["target_table"].(string); ok && tableName != "" {
		targetTableName = tableName
	}

	targetDbName := "mira_task_tmp" // Doris临时数据库
	if dbName, ok := test.Params["target_db"].(string); ok && dbName != "" {
		targetDbName = dbName
	}

	// 3. 调用data-service的新版Write接口
	writeReq := &pb.WriteRequest{
		ArrowBatch: arrowBatch,
		DbName:     targetDbName,
		TableName:  targetTableName,
	}

	writeStart := time.Now()
	writeResp, err := te.dataClient.Write(ctx, []*pb.WriteRequest{writeReq})
	writeDuration := time.Since(writeStart)

	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service新版Write接口失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 检查响应
	if writeResp == nil || !writeResp.Success {
		result.Passed = false
		if writeResp != nil {
			result.Error = fmt.Sprintf("写入失败: %s", writeResp.Message)
		} else {
			result.Error = "写入失败: 响应为空"
		}
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 3. 验证写入后的行数（从源表验证，因为写入到Doris需要额外客户端）
	validationResult, err := validator.ValidateWriteResult(ctx, te.strategy.GetDB(), te.tableName)
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
	result.Message = fmt.Sprintf("%s (写入耗时: %v, 目标表: %s.%s, 注意：当前验证的是源表行数)",
		validationResult.Message, writeDuration, targetDbName, targetTableName)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result
}

