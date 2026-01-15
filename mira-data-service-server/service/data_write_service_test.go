package service

import (
	"bytes"
	"data-service/common"
	"data-service/generated/datasource"
	"data-service/mocks"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// 创建测试用的Arrow数据
func createTestArrowData() []byte {
	pool := memory.NewGoAllocator()

	// 创建schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	// 创建数据
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// 添加数据
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	builder.Field(2).(*array.Int32Builder).AppendValues([]int32{25, 30, 35}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// 序列化为字节
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	defer writer.Close()

	writer.Write(record)
	return buf.Bytes()
}

// 创建测试用的Schema
func createTestSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
}

func TestDataWriteService_ParseArrowSchema(t *testing.T) {
	service := &DataWriteService{}

	// 创建测试用的Arrow数据
	arrowData := createTestArrowData()

	// 测试解析schema
	schema, err := service.parseArrowSchema(arrowData)

	assert.NoError(t, err)
	assert.NotNil(t, schema)
	assert.Equal(t, 3, len(schema.Fields()))
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "name", schema.Field(1).Name)
	assert.Equal(t, "age", schema.Field(2).Name)
}

func TestDataWriteService_WriteCSVHeaders(t *testing.T) {
	// 创建临时文件
	tempFile, err := os.CreateTemp("", "test_*.csv")
	assert.NoError(t, err)
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	writer := csv.NewWriter(tempFile)

	processor := &StreamProcessor{
		writer: writer,
		schema: createTestSchema(),
	}

	service := &DataWriteService{}
	err = service.writeCSVHeaders(processor)

	assert.NoError(t, err)
	writer.Flush()

	// 验证文件内容
	content, err := os.ReadFile(tempFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, "id,name,age\n", string(content))
}

func TestDataWriteService_GetArrowValueAsString(t *testing.T) {
	service := &DataWriteService{}

	// 测试字符串类型
	strBuilder := array.NewStringBuilder(memory.NewGoAllocator())
	defer strBuilder.Release()
	strBuilder.AppendValues([]string{"test"}, nil)
	strArray := strBuilder.NewArray()
	defer strArray.Release()

	value := service.getArrowValueAsString(strArray, 0)
	assert.Equal(t, "test", value)

	// 测试整数类型
	intBuilder := array.NewInt64Builder(memory.NewGoAllocator())
	defer intBuilder.Release()
	intBuilder.AppendValues([]int64{123}, nil)
	intArray := intBuilder.NewArray()
	defer intArray.Release()

	value = service.getArrowValueAsString(intArray, 0)
	assert.Equal(t, "123", value)

	// 测试空值
	nullBuilder := array.NewStringBuilder(memory.NewGoAllocator())
	defer nullBuilder.Release()
	nullBuilder.AppendNull()
	nullArray := nullBuilder.NewArray()
	defer nullArray.Release()

	value = service.getArrowValueAsString(nullArray, 0)
	assert.Equal(t, "", value)
}

func TestDataWriteService_CreateTableInDoris(t *testing.T) {
	// 创建gomock控制器
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// 创建mock服务
	mockDorisService := mocks.NewMockIDorisService(ctrl)
	service := &DataWriteService{}

	// 设置mock期望
	mockDorisService.EXPECT().
		ExecuteUpdate(gomock.Any()).
		Return(int64(1), nil).
		Times(1)

	// 执行测试
	schema := createTestSchema()
	err := service.createTableInDoris(mockDorisService, "test_db", "test_table", schema)

	assert.NoError(t, err)
}

func TestDataWriteService_InitializeFirstRequest(t *testing.T) {
	// 创建gomock控制器
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// 创建临时文件
	tempFile, err := os.CreateTemp("", "test_*.csv")
	assert.NoError(t, err)
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	writer := csv.NewWriter(tempFile)

	processor := &StreamProcessor{
		writer: writer,
	}

	// 创建mock服务并预先设置
	mockDorisService := mocks.NewMockIDorisService(ctrl)
	service := &DataWriteService{
		dorisService: mockDorisService, // 预先设置mock
	}

	// 设置mock期望
	mockDorisService.EXPECT().
		ExecuteUpdate(gomock.Any()).
		Return(int64(1), nil).
		Times(1)

	// 创建测试请求
	arrowData := createTestArrowData()
	req := &datasource.WriteRequest{
		TableName:  "test_table",
		DbName:     "test_db",
		ArrowBatch: arrowData,
	}

	// 执行测试
	err = service.initializeFirstRequest(req, processor)

	assert.NoError(t, err)
	assert.Equal(t, "test_table", processor.tableName)
	assert.Equal(t, "test_db", processor.dbName)
	assert.NotNil(t, processor.schema)
	assert.NotNil(t, processor.dorisService)
}

func TestDataWriteService_InitializeFirstRequest_ValidationErrors(t *testing.T) {
	service := &DataWriteService{}
	processor := &StreamProcessor{}

	// 测试空表名
	req := &datasource.WriteRequest{
		TableName: "",
		DbName:    "test_db",
	}

	err := service.initializeFirstRequest(req, processor)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table name is required")

	// 测试空数据库名
	req = &datasource.WriteRequest{
		TableName: "test_table",
		DbName:    "",
	}

	err = service.initializeFirstRequest(req, processor)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database name is required")

	// 测试空Arrow数据
	req = &datasource.WriteRequest{
		TableName:  "test_table",
		DbName:     "test_db",
		ArrowBatch: []byte{},
	}

	err = service.initializeFirstRequest(req, processor)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "first request must contain arrow data")
}

func TestDataWriteService_ProcessArrowBatch(t *testing.T) {
	// 创建临时文件
	tempFile, err := os.CreateTemp("", "test_*.csv")
	assert.NoError(t, err)
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	writer := csv.NewWriter(tempFile)

	processor := &StreamProcessor{
		writer: writer,
		schema: createTestSchema(),
	}

	service := &DataWriteService{}

	// 先写入CSV头部
	err = service.writeCSVHeaders(processor)
	assert.NoError(t, err)

	// 创建测试数据
	arrowData := createTestArrowData()

	// 执行测试
	err = service.processArrowBatch(arrowData, processor)

	assert.NoError(t, err)
	writer.Flush()

	// 验证文件内容
	content, err := os.ReadFile(tempFile.Name())
	assert.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	assert.Equal(t, 4, len(lines)) // 1个header + 3行数据

	// 验证表头
	assert.Equal(t, "id,name,age", lines[0])

	// 验证数据行
	assert.Equal(t, "1,Alice,25", lines[1])
	assert.Equal(t, "2,Bob,30", lines[2])
	assert.Equal(t, "3,Charlie,35", lines[3])
}

func TestDataWriteService_CreateStreamProcessor(t *testing.T) {
	service := &DataWriteService{}

	// 确保DATA_DIR存在
	if err := os.MkdirAll(common.DATA_DIR, 0755); err != nil {
		t.Skipf("Cannot create DATA_DIR: %v", err)
	}

	processor, err := service.createStreamProcessor()

	assert.NoError(t, err)
	assert.NotNil(t, processor)
	assert.NotNil(t, processor.tempFile)
	assert.NotNil(t, processor.writer)
	assert.True(t, strings.HasPrefix(filepath.Base(processor.filePath), "arrow_stream_"))
	assert.True(t, strings.HasSuffix(processor.filePath, ".csv"))

	// 清理
	processor.cleanup()
}

func TestStreamProcessor_Cleanup(t *testing.T) {
	// 创建临时文件
	tempFile, err := os.CreateTemp("", "test_*.csv")
	assert.NoError(t, err)
	tempFile.Close()

	writer := csv.NewWriter(tempFile)

	processor := &StreamProcessor{
		tempFile: tempFile,
		filePath: tempFile.Name(),
		writer:   writer,
	}

	// 执行清理
	processor.cleanup()

	// 验证文件被删除
	_, err = os.Stat(tempFile.Name())
	assert.True(t, os.IsNotExist(err))
}
