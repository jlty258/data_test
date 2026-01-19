package testcases

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

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

	// 在执行SQL前，先调用ImportData接口导入表到Doris
	targetDbName := "mira_task_tmp" // Doris临时数据库
	if dbName, ok := test.Params["target_db"].(string); ok && dbName != "" {
		targetDbName = dbName
	}
	
	targetTableName := fmt.Sprintf("test_execute_sql_%s_%d", te.tableName, time.Now().Unix())
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
		Columns:         te.getFieldNames(), // 导入所有字段
		Keys:            []*pb.TableKey{},   // 可选：表键信息
	}

	// 构建ImportData请求
	importReq := &pb.ImportDataRequest{
		Targets: []*pb.ImportTarget{importTarget},
	}

	// 先导入数据
	importResp, err := te.dataClient.ImportData(ctx, importReq)
	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("导入数据失败（执行SQL前需要先导入）: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 检查导入响应
	if importResp == nil || len(importResp.Results) == 0 {
		result.Passed = false
		result.Error = "ImportData响应为空或没有结果"
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 检查导入是否成功
	importResult := importResp.Results[0]
	if !importResult.Success {
		result.Passed = false
		if importResult.ErrorMessage != "" {
			result.Error = fmt.Sprintf("导入数据失败: %s", importResult.ErrorMessage)
		} else {
			result.Error = "导入数据失败"
		}
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 构建SQL查询（从测试参数中获取，或使用默认查询）
	// 使用导入后的表名
	sqlQuery := fmt.Sprintf("SELECT * FROM %s", targetTableName)
	if sql, ok := test.Params["sql"].(string); ok && sql != "" {
		// 如果SQL中使用了原始表名，替换为导入后的表名
		sqlQuery = sql
		// 将SQL中的原始表名替换为目标表名
		sqlQuery = strings.ReplaceAll(sqlQuery, te.tableName, targetTableName)
	}

	// 调用data-service的ExecuteSql接口
	// ExecuteSql 在 Doris 中执行，DbName 应该是 Doris 数据库名
	req := &pb.ExecuteSqlRequest{
		Sql:           sqlQuery,
		DbName:        targetDbName, // 使用导入的数据库名
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

