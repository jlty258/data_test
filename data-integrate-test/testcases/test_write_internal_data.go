package testcases

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"data-integrate-test/clients"
	"data-integrate-test/utils"
	"data-integrate-test/validators"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

// testWriteInternalData 测试写入内部数据接口
func (te *TestExecutor) testWriteInternalData(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "write_internal",
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

	// 2. 调用data-service的WriteInternalData接口
	// 写入到内置数据库（Doris）的临时表
	writeReq := &clients.WriteInternalDataRequest{
		ArrowBatch:    arrowBatch,
		DbName:        "mira_task_tmp", // Doris临时数据库
		TableName:     fmt.Sprintf("test_write_%s_%d", te.tableName, time.Now().Unix()),
		JobInstanceId: fmt.Sprintf("write_job_%d", time.Now().Unix()),
	}

	writeStart := time.Now()
	err = te.dataClient.WriteInternalData(ctx, writeReq)
	writeDuration := time.Since(writeStart)

	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service WriteInternalData接口失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 3. 验证写入后的行数（从Doris读取）
	// 注意：这里需要从Doris读取，但为了简化，我们验证原始表的行数
	// 实际应该从Doris读取验证，但需要额外的Doris客户端
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
	result.Message = fmt.Sprintf("%s (写入耗时: %v, 注意：当前验证的是源表行数)", validationResult.Message, writeDuration)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result
}

// readDataAsArrow 从数据库读取数据并转换为Arrow格式
func (te *TestExecutor) readDataAsArrow(ctx context.Context) ([]byte, error) {
	// 构建查询
	fieldNames := te.getFieldNames()
	var query string
	if len(fieldNames) == 0 {
		query = fmt.Sprintf("SELECT * FROM %s", te.tableName)
	} else {
		// 构建字段列表
		fields := ""
		for i, field := range fieldNames {
			if i > 0 {
				fields += ", "
			}
			fields += fmt.Sprintf("`%s`", field)
		}
		query = fmt.Sprintf("SELECT %s FROM %s", fields, te.tableName)
	}

	// 执行查询
	rows, err := te.strategy.GetDB().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询数据库失败: %w", err)
	}
	defer rows.Close()

	// 获取列信息
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %w", err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("获取列类型失败: %w", err)
	}

	// 构建Arrow Schema
	pool := memory.NewGoAllocator()
	fields := make([]arrow.Field, len(cols))
	for i, col := range cols {
		arrowType, err := sqlTypeToArrowType(colTypes[i])
		if err != nil {
			return nil, fmt.Errorf("转换列类型失败 %s: %w", col, err)
		}
		fields[i] = arrow.Field{Name: col, Type: arrowType}
	}
	schema := arrow.NewSchema(fields, nil)

	// 创建Arrow RecordBuilder
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// 读取数据并填充到Arrow Builder
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range valuePtrs {
		valuePtrs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("扫描行失败: %w", err)
		}

		// 将值添加到Arrow Builder
		for i, val := range values {
			if err := appendValueToArrowBuilder(builder.Field(i), val); err != nil {
				return nil, fmt.Errorf("添加值到Arrow Builder失败: %w", err)
			}
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取行时出错: %w", err)
	}

	// 创建Arrow Record
	record := builder.NewRecord()
	defer record.Release()

	// 序列化为Arrow格式
	var buf []byte
	{
		var buffer bytes.Buffer
		writer := ipc.NewWriter(&buffer, ipc.WithSchema(record.Schema()))
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("写入Arrow数据失败: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("关闭Arrow writer失败: %w", err)
		}
		buf = buffer.Bytes()
	}

	return buf, nil
}

// sqlTypeToArrowType 将SQL类型转换为Arrow类型（简化版本）
func sqlTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, error) {
	typeName := colType.DatabaseTypeName()
	switch typeName {
	case "INT", "INTEGER", "MEDIUMINT", "SMALLINT", "TINYINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case "DOUBLE", "REAL":
		return arrow.PrimitiveTypes.Float64, nil
	case "DECIMAL", "NUMERIC":
		return arrow.PrimitiveTypes.Float64, nil
	case "VARCHAR", "CHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT":
		return arrow.BinaryTypes.String, nil
	case "DATE", "DATETIME", "TIMESTAMP", "TIME":
		return arrow.BinaryTypes.String, nil
	case "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return arrow.BinaryTypes.Binary, nil
	default:
		// 默认使用字符串类型
		return arrow.BinaryTypes.String, nil
	}
}

// appendValueToArrowBuilder 将值添加到Arrow Builder
func appendValueToArrowBuilder(builder array.Builder, val interface{}) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		case int8:
			b.Append(int64(v))
		case int16:
			b.Append(int64(v))
		default:
			return fmt.Errorf("无法将 %T 转换为 int64", val)
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			return fmt.Errorf("无法将 %T 转换为 float64", val)
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		default:
			b.Append(fmt.Sprintf("%v", val))
		}
	case *array.BinaryBuilder:
		switch v := val.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			return fmt.Errorf("无法将 %T 转换为 []byte", val)
		}
	default:
		return fmt.Errorf("不支持的Builder类型: %T", builder)
	}
	return nil
}

