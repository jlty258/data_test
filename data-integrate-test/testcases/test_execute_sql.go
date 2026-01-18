package testcases

import (
	"context"
	"fmt"
	"io"
	"time"

	"data-integrate-test/clients"
	"data-integrate-test/utils"
	"data-integrate-test/validators"

	pb "data-integrate-test/generated/datasource"
)

// testExecuteSql 测试执行SQL接口
func (te *TestExecutor) testExecuteSql(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "execute_sql",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// 获取数据库配置
	dbConfig := te.strategy.GetConnectionInfo()

	// 构建SQL查询（从测试参数中获取，或使用默认查询）
	sqlQuery := fmt.Sprintf("SELECT * FROM %s", te.tableName)
	if sql, ok := test.Params["sql"].(string); ok && sql != "" {
		sqlQuery = sql
	}

	// 调用data-service的ExecuteSql接口
	req := &pb.ExecuteSqlRequest{
		Sql:           sqlQuery,
		DbName:        fmt.Sprintf("job_%d", time.Now().Unix()), // jobInstanceId作为dbName
		TargetTableName: "", // 不写入目标表
	}

	callStart := time.Now()
	stream, err := te.dataClient.ExecuteSql(ctx, req)
	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service ExecuteSql接口失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 接收流式响应
	var actualCount int64 = 0
	var dmlResult *pb.DmlResult
	for {
		resp, err := stream.Recv()
		if err != nil {
		if err == io.EOF {
			break
		}
			result.Passed = false
			result.Error = fmt.Errorf("接收ExecuteSql响应失败: %w", err).Error()
			result.EndTime = time.Now()
			result.Duration = result.EndTime.Sub(result.StartTime)
			return result
		}

		// 检查响应是否成功
		if !resp.Success {
			result.Passed = false
			result.Error = fmt.Sprintf("ExecuteSql执行失败: %s", resp.Message)
			result.EndTime = time.Now()
			result.Duration = result.EndTime.Sub(result.StartTime)
			return result
		}

		// 处理结果
		switch r := resp.Result.(type) {
		case *pb.ExecuteSqlResponse_ArrowBatch:
			// SELECT查询结果（Arrow格式）
			arrowBatch := r.ArrowBatch
			if len(arrowBatch) > 0 {
				// 检查是否是EOF标记
				if len(arrowBatch) == 3 && string(arrowBatch) == "EOF" {
					continue
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
		case *pb.ExecuteSqlResponse_DmlResult:
			// DML操作结果（INSERT/UPDATE/DELETE）
			dmlResult = r.DmlResult
			actualCount = dmlResult.AffectedRows
		}
	}

	callDuration := time.Since(callStart)

	// 验证结果
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

	msg := fmt.Sprintf("%s (调用耗时: %v", validationResult.Message, callDuration)
	if dmlResult != nil {
		msg += fmt.Sprintf(", 执行耗时: %s", dmlResult.ExecutionTime)
	}
	msg += ")"
	result.Message = msg
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result
}

