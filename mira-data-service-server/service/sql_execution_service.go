package service

import (
	"bytes"
	"data-service/common"
	"data-service/config"
	pb "data-service/generated/datasource"
	log "data-service/log"
	"data-service/utils"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// SqlExecutionService SQL执行服务
type SqlExecutionService struct {
	dorisService IDorisService
}

// NewSqlExecutionService 创建SQL执行服务
func NewSqlExecutionService(dbName string) (*SqlExecutionService, error) {
	dorisService, err := NewDorisService(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to create doris service: %v", err)
	}

	return &SqlExecutionService{
		dorisService: dorisService,
	}, nil
}

// ExecuteStreamingSql 执行流式SQL查询
func (s *SqlExecutionService) ExecuteStreamingSql(sql string, stream grpc.ServerStreamingServer[pb.ExecuteSqlResponse]) error {
	// 非 SELECT 语句（DDL/DML）直接执行并返回影响行数
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(sqlUpper, "SELECT") {
		affected, err := s.dorisService.ExecuteUpdate(sql)
		if err != nil {
			return fmt.Errorf("failed to execute sql: %v", err)
		}
		resp := &pb.ExecuteSqlResponse{
			Success: true,
			Message: fmt.Sprintf("Statement executed successfully, affected rows: %d", affected),
			Result: &pb.ExecuteSqlResponse_DmlResult{
				DmlResult: &pb.DmlResult{AffectedRows: affected},
			},
		}
		return stream.Send(resp)
	}

	// 执行SQL获取列类型信息
	rows, done, err := s.dorisService.ExecuteSQL(sql)
	if err != nil {
		return fmt.Errorf("failed to execute sql: %v", err)
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	// 获取列信息和类型
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %v", err)
	}

	batchSize := config.GetConfigMap().Dbms.StreamDataSize
	var batchRows [][]interface{}
	var sentData bool
	totalRecords := int64(0)

	// 流式返回数据
	for rows.Next() {
		// 准备数据容器
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Logger.Errorf("Failed to scan row: %v", err)
			continue
		}

		// 添加到批次
		batchRows = append(batchRows, values)
		totalRecords++

		if len(batchRows) >= batchSize {
			if err := s.sendArrowBatch(columns, columnTypes, batchRows, stream); err != nil {
				return fmt.Errorf("failed to send arrow batch: %v", err)
			}
			batchRows = batchRows[:0] // 清空批次
			sentData = true
		}
	}

	// 检查迭代期间是否发生错误（例如 Doris 中途断链）
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %v", err)
	}

	// 发送剩余的批次
	if len(batchRows) > 0 {
		if err := s.sendArrowBatch(columns, columnTypes, batchRows, stream); err != nil {
			return fmt.Errorf("failed to send final arrow batch: %v", err)
		}
		sentData = true
	}

	log.Logger.Infof("Query executed successfully, total records: %d", totalRecords)

	if !sentData {
		// 查询到空集，返回空Arrow批次
		response := &pb.ExecuteSqlResponse{
			Success: true,
			Message: "Query returned no results",
			Result: &pb.ExecuteSqlResponse_ArrowBatch{
				ArrowBatch: []byte{},
			},
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send empty response: %v", err)
		}
	}

	// // 发送EOF标志
	// eofResponse := &pb.ExecuteSqlResponse{
	// 	Success: true,
	// 	Message: "Query completed",
	// 	Result: &pb.ExecuteSqlResponse_ArrowBatch{
	// 		ArrowBatch: []byte("EOF"),
	// 	},
	// }

	// return stream.Send(eofResponse)
	return nil
}

// sendArrowBatch 发送Arrow格式的数据批次
func (s *SqlExecutionService) sendArrowBatch(columns []string, columnTypes []*sql.ColumnType, rows [][]interface{}, stream grpc.ServerStreamingServer[pb.ExecuteSqlResponse]) error {
	if len(rows) == 0 {
		// 空批次，发送空Arrow数据
		response := &pb.ExecuteSqlResponse{
			Success: true,
			Message: "Empty batch",
			Result: &pb.ExecuteSqlResponse_ArrowBatch{
				ArrowBatch: []byte{},
			},
		}
		return stream.Send(response)
	}

	// 创建内存分配器
	pool := memory.NewGoAllocator()

	// 逻辑构建Arrow Schema
	fields, err := s.buildArrowSchemaFromColumnTypes(columns, columnTypes)
	if err != nil {
		return fmt.Errorf("failed to build arrow schema: %v", err)
	}
	schema := arrow.NewSchema(fields, nil)

	// 创建Arrow RecordBuilder
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// 填充数据到Arrow Builder
	for _, row := range rows {
		for colIdx, value := range row {
			if colIdx < len(columns) {
				if err := utils.AppendValueToBuilder(builder.Field(colIdx), value); err != nil {
					log.Logger.Errorf("Failed to append value for column %d: %v", colIdx, err)
					return fmt.Errorf("failed to append value for column %d: %v", colIdx, err)
				}
			}
		}
	}

	// 创建Arrow Record
	record := builder.NewRecord()
	defer record.Release()

	// 序列化Arrow数据
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write arrow record: %v", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close arrow writer: %v", err)
	}

	// 发送Arrow批次
	response := &pb.ExecuteSqlResponse{
		Success: true,
		Message: fmt.Sprintf("Arrow batch with %d rows", len(rows)),
		Result: &pb.ExecuteSqlResponse_ArrowBatch{
			ArrowBatch: buf.Bytes(),
		},
	}

	return stream.Send(response)
}

// buildArrowSchemaFromColumnTypes 从数据库列类型构建Arrow Schema
func (s *SqlExecutionService) buildArrowSchemaFromColumnTypes(columns []string, columnTypes []*sql.ColumnType) ([]arrow.Field, error) {
	var fields []arrow.Field

	for i, columnName := range columns {
		if i >= len(columnTypes) {
			// 如果列类型信息不足，使用默认的字符串类型
			fields = append(fields, arrow.Field{
				Name: columnName,
				Type: arrow.BinaryTypes.String,
			})
			continue
		}

		// 使用DorisService已有的类型转换逻辑
		arrowType, err := s.convertDorisColumnTypeToArrowType(columnTypes[i])
		if err != nil {
			log.Logger.Warnf("Failed to convert column type for %s: %v, using string type", columnName, err)
			arrowType = arrow.BinaryTypes.String
		}

		fields = append(fields, arrow.Field{
			Name: columnName,
			Type: arrowType,
		})
	}

	return fields, nil
}

// convertDorisColumnTypeToArrowType 转换Doris列类型到Arrow类型
func (s *SqlExecutionService) convertDorisColumnTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, error) {
	// 获取数据库类型名称
	dbTypeName := strings.ToUpper(colType.DatabaseTypeName())

	return common.ConvertDorisTypeToArrowType(dbTypeName)
}

// ExecuteSqlWithTableOutput 执行SQL并将结果写入目标表
func (s *SqlExecutionService) ExecuteSqlWithTableOutput(sql, targetTableName string, stream grpc.ServerStreamingServer[pb.ExecuteSqlResponse]) error {
	// 1. 先获取列信息：拼接 LIMIT 1
	limitedSQL := fmt.Sprintf("SELECT * FROM (%s) sub LIMIT 0", sql)
	rows, done, err := s.dorisService.ExecuteSQL(limitedSQL)
	if err != nil {
		return fmt.Errorf("failed to execute SQL: %v", err)
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	// 2. 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %v", err)
	}

	dbName := s.dorisService.GetDBName()
	if err := s.dorisService.SwitchDatabase(dbName); err != nil {
		return fmt.Errorf("failed to switch database: %v", err)
	}

	// 3. 在Doris中创建目标表
	if err := s.createTargetTable(dbName, targetTableName, columns, columnTypes); err != nil {
		return fmt.Errorf("failed to create target table: %v", err)
	}

	// 4. 执行查询结果并插入到目标表
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = fmt.Sprintf("`%s`", col)
	}
	InsertSQL := fmt.Sprintf(`
		INSERT INTO %s (%s) 
		SELECT %s FROM (%s) sub
	`, targetTableName, strings.Join(quotedColumns, ", "), strings.Join(quotedColumns, ", "), sql)

	totalRows, err := s.dorisService.ExecuteUpdate(InsertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert into target table: %v", err)
	}

	// 5. 发送成功响应
	response := &pb.ExecuteSqlResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully inserted %d rows into table %s", totalRows, targetTableName),
		Result: &pb.ExecuteSqlResponse_DmlResult{
			DmlResult: &pb.DmlResult{
				AffectedRows: totalRows,
			},
		},
	}

	return stream.Send(response)
}

// createTargetTable 创建目标表
func (s *SqlExecutionService) createTargetTable(dbName string, tableName string, columns []string, columnTypes []*sql.ColumnType) error {
	var columnDefs []string

	// 使用UUID生成5位随机字符串作为主键字段名的一部分
	uuidStr := uuid.New().String()
	// 取UUID的前5位字符（去掉连字符）
	randomSuffix := strings.ReplaceAll(uuidStr[:8], "-", "")[:5]
	primaryKeyFieldName := fmt.Sprintf("pk_%s", randomSuffix)
	primaryKeyField := fmt.Sprintf("`%s` BIGINT NOT NULL AUTO_INCREMENT", primaryKeyFieldName)
	columnDefs = append(columnDefs, primaryKeyField)

	for i, colName := range columns {
		colType := columnTypes[i]
		dorisType := common.ConvertSqlTypeToDorisType(colType)
		columnDef := fmt.Sprintf("`%s` %s", colName, dorisType)
		columnDefs = append(columnDefs, columnDef)
	}

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			%s
		)
		ENGINE=OLAP
		UNIQUE KEY (`+"`%s`"+`)
		DISTRIBUTED BY HASH(`+"`%s`"+`) BUCKETS AUTO
		PROPERTIES (
			"replication_num" = "1"
		)
	`, dbName, tableName, strings.Join(columnDefs, ",\n\t\t"), primaryKeyFieldName, primaryKeyFieldName)

	_, err := s.dorisService.ExecuteUpdate(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create target table: %v", err)
	}

	return nil
}

// insertBatchToTable 批量插入数据到目标表
func (s *SqlExecutionService) insertBatchToTable(tableName string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// 构建INSERT语句
	placeholders := strings.Repeat("?,", len(columns))
	placeholders = placeholders[:len(placeholders)-1] // 移除最后一个逗号

	var valuePlaceholders []string
	var allValues []interface{}

	for _, row := range rows {
		valuePlaceholders = append(valuePlaceholders, fmt.Sprintf("(%s)", placeholders))
		allValues = append(allValues, row...)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName,
		strings.Join(columns, ","),
		strings.Join(valuePlaceholders, ","))

	_, err := s.dorisService.ExecuteUpdate(insertSQL, allValues...)
	return err
}
