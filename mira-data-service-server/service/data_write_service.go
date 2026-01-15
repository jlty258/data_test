package service

import (
	"bytes"
	"data-service/common"
	"data-service/config"
	"data-service/generated/datasource"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "data-service/log"

	"os/exec"

	"github.com/google/uuid"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/compress"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"google.golang.org/grpc"
)

// DataWriteService 数据写入服务
type DataWriteService struct {
	dorisService IDorisService
	initOnce     sync.Once
	initError    error
}

// NewDataWriteService 创建数据写入服务
func NewDataWriteService() (*DataWriteService, error) {
	return &DataWriteService{}, nil
}

// 1. 主要的流处理函数
func (s *DataWriteService) ProcessWriteStream(stream grpc.ClientStreamingServer[datasource.WriteRequest, datasource.WriteResponse]) error {
	// 初始化流处理器
	processor, err := s.createStreamProcessor()
	if err != nil {
		return fmt.Errorf("failed to create stream processor: %v", err)
	}
	defer processor.cleanupIfNotDebug()

	// 处理流数据
	if err := s.processStreamData(stream, processor); err != nil {
		return fmt.Errorf("failed to process stream data: %v", err)
	}

	// 导入数据到Doris
	if err := s.importToDoris(processor); err != nil {
		return fmt.Errorf("failed to import to Doris: %v", err)
	}

	// 返回成功响应
	return stream.SendAndClose(&datasource.WriteResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully imported data to table %s.%s", processor.dbName, processor.tableName),
	})
}

// 2. 流处理器结构体
type StreamProcessor struct {
	tempFile      *os.File
	filePath      string
	parquetWriter *pqarrow.FileWriter
	schema        *arrow.Schema
	tableName     string
	dbName        string
	dorisService  IDorisService
}

// 3. 创建流处理器
func (s *DataWriteService) createStreamProcessor() (*StreamProcessor, error) {
	fileName := fmt.Sprintf("arrow_stream_%s.parquet", uuid.New().String())
	filePath := filepath.Join(common.DATA_DIR, fileName)

	tempFile, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %v", err)
	}
	return &StreamProcessor{
		tempFile: tempFile,
		filePath: filePath,
	}, nil
}

// 4. 清理资源
func (p *StreamProcessor) cleanup() {
	if p.parquetWriter != nil {
		_ = p.parquetWriter.Close()
	}
	if p.tempFile != nil {
		p.tempFile.Close()
	}
	if p.filePath != "" {
		if err := os.Remove(p.filePath); err != nil {
			log.Logger.Warnf("Failed to remove file %s: %v", p.filePath, err)
		}
	}
}

func (p *StreamProcessor) cleanupIfNotDebug() {
	// 获取配置中的日志级别
	config := config.GetConfigMap()
	logLevel := config.LoggerConfig.Level
	if logLevel == "" {
		logLevel = "info" // 默认info级别
	}

	// 如果是debug级别，跳过cleanup
	if logLevel == "debug" {
		log.Logger.Debugf("Skipping cleanup due to debug log level")
		return
	}

	// 否则执行cleanup
	log.Logger.Debugf("Executing cleanup (log level: %s)", logLevel)
	p.cleanup()
}

// 5. 处理流数据
func (s *DataWriteService) processStreamData(stream grpc.ClientStreamingServer[datasource.WriteRequest, datasource.WriteResponse], processor *StreamProcessor) error {
	first := true
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Logger.Infof("Received EOF, write stream for database finished: %s, table: %s", processor.dbName, processor.tableName)
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive request: %v", err)
		}

		if first {
			if err := s.initializeFirstRequest(req, processor); err != nil {
				return fmt.Errorf("failed to initialize first request: %v", err)
			}
			first = false
		}

		if err := s.processArrowBatch(req.ArrowBatch, processor); err != nil {
			return fmt.Errorf("failed to process arrow batch: %v", err)
		}
	}

	return nil
}

// 6. 初始化第一个请求
func (s *DataWriteService) initializeFirstRequest(req *datasource.WriteRequest, processor *StreamProcessor) error {
	processor.tableName = req.TableName
	processor.dbName = req.DbName

	if processor.tableName == "" {
		return fmt.Errorf("table name is required")
	}
	if processor.dbName == "" {
		return fmt.Errorf("database name is required")
	}

	// 创建Doris服务
	s.initOnce.Do(func() {
		dorisService, err := NewDorisService(common.MIRA_TMP_TASK_DB)
		if err != nil {
			s.initError = err
			return
		}
		s.dorisService = dorisService
	})

	if s.initError != nil {
		return fmt.Errorf("failed to create doris service: %v", s.initError)
	}

	processor.dorisService = s.dorisService

	// 检查是否存在数据库
	if err := s.dorisService.EnsureDorisDatabaseExists(processor.dbName); err != nil {
		return fmt.Errorf("failed to ensure Doris database exists: %v", err)
	}

	// 解析schema
	if len(req.ArrowBatch) == 0 {
		return fmt.Errorf("first request must contain arrow data")
	}

	schema, err := s.parseArrowSchema(req.ArrowBatch)
	if err != nil {
		return fmt.Errorf("failed to parse arrow schema: %v", err)
	}
	processor.schema = schema

	// 创建表
	if err := s.createTableInDoris(processor.dorisService, processor.dbName, processor.tableName, schema); err != nil {
		return fmt.Errorf("failed to create table in Doris: %v", err)
	}

	// 初始化 Parquet 写入器
	parquetProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.DefaultWriterProps()

	pw, err := pqarrow.NewFileWriter(processor.schema, processor.tempFile, parquetProps, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %v", err)
	}
	processor.parquetWriter = pw
	return nil
}

// 7. 解析Arrow Schema
func (s *DataWriteService) parseArrowSchema(arrowBatch []byte) (*arrow.Schema, error) {
	reader := bytes.NewReader(arrowBatch)
	ipcReader, err := ipc.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %v", err)
	}
	defer ipcReader.Release()

	return ipcReader.Schema(), nil
}

// 8. 处理Arrow批次
func (s *DataWriteService) processArrowBatch(arrowBatch []byte, processor *StreamProcessor) error {
	if len(arrowBatch) == 0 {
		return nil
	}
	reader := bytes.NewReader(arrowBatch)
	ipcReader, err := ipc.NewReader(reader)
	if err != nil {
		return fmt.Errorf("failed to create arrow reader: %v", err)
	}
	defer ipcReader.Release()

	for ipcReader.Next() {
		record := ipcReader.Record()
		if err := processor.parquetWriter.Write(record); err != nil {
			record.Release()
			return fmt.Errorf("failed to write parquet record: %v", err)
		}
		record.Release()
	}
	return nil
}

// 9. 导入到Doris
func (s *DataWriteService) importToDoris(processor *StreamProcessor) error {
	if processor.parquetWriter != nil {
		_ = processor.parquetWriter.Close()
	}
	if processor.tempFile != nil {
		_ = processor.tempFile.Close()
	}
	return s.importLocalFileToDoris(processor.dbName, processor.tableName, processor.filePath, processor.schema)
}

// getArrowValueAsString 将Arrow值转换为字符串
func (s *DataWriteService) getArrowValueAsString(col arrow.Array, index int64) string {
	if col.IsNull(int(index)) {
		return ""
	}

	switch arr := col.(type) {
	case *array.String:
		return arr.Value(int(index))
	case *array.LargeString: // 新增
		return arr.Value(int(index))
	case *array.Int8:
		return fmt.Sprintf("%d", arr.Value(int(index)))
	case *array.Int16:
		return fmt.Sprintf("%d", arr.Value(int(index)))
	case *array.Int32:
		return fmt.Sprintf("%d", arr.Value(int(index)))
	case *array.Int64:
		return fmt.Sprintf("%d", arr.Value(int(index)))
	case *array.Uint8:
		return fmt.Sprintf("%d", arr.Value(int(index)))
	case *array.Uint16:
		return fmt.Sprintf("%d", arr.Value(int(index)))
	case *array.Uint32:
		return fmt.Sprintf("%d", arr.Value(int(index)))
	case *array.Uint64:
		return fmt.Sprintf("%d", arr.Value(int(index)))
	case *array.Float32:
		return fmt.Sprintf("%f", arr.Value(int(index)))
	case *array.Float64:
		return fmt.Sprintf("%f", arr.Value(int(index)))
	case *array.Boolean:
		return fmt.Sprintf("%t", arr.Value(int(index)))
	case *array.Timestamp:
		return fmt.Sprintf("%v", arr.Value(int(index)))
	case *array.Date32:
		return fmt.Sprintf("%v", arr.Value(int(index)))
	case *array.Date64:
		return fmt.Sprintf("%v", arr.Value(int(index)))
	case *array.Decimal128:
		v := arr.Value(int(index))
		scale := arr.DataType().(*arrow.Decimal128Type).Scale
		return v.ToString(scale)
	case *array.Decimal256:
		v := arr.Value(int(index))
		scale := arr.DataType().(*arrow.Decimal256Type).Scale
		return v.ToString(scale)
	default:
		// 对于未知类型，尝试获取值
		if arr, ok := col.(arrow.Array); ok {
			return fmt.Sprintf("%v", arr)
		}
		return "unknown_type"
	}
}

// createTableInDoris 在Doris中创建表
func (s *DataWriteService) createTableInDoris(dorisService IDorisService, dbName, tableName string, schema *arrow.Schema) error {
	// 构建列定义
	var columnDefs []string

	// 使用UUID生成5位随机字符串作为主键字段名的一部分
	uuidStr := uuid.New().String()
	// 取UUID的前5位字符（去掉连字符）
	randomSuffix := strings.ReplaceAll(uuidStr[:8], "-", "")[:5]
	primaryKeyFieldName := fmt.Sprintf("pk_%s", randomSuffix)
	primaryKeyField := fmt.Sprintf("`%s` BIGINT NOT NULL AUTO_INCREMENT", primaryKeyFieldName)
	columnDefs = append(columnDefs, primaryKeyField)

	for _, field := range schema.Fields() {
		dorisType := common.ConvertArrowTypeToDorisType(field.Type)
		columnDef := fmt.Sprintf("`%s` %s", field.Name, dorisType)
		columnDefs = append(columnDefs, columnDef)
	}

	// 构建CREATE TABLE SQL
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

	// 执行CREATE TABLE
	_, err := dorisService.ExecuteUpdate(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create Doris table: %v", err)
	}

	return nil
}

// importLocalFileToDoris 使用doris-streamloader导入本地文件
func (s *DataWriteService) importLocalFileToDoris(dbName, tableName, filePath string, schema *arrow.Schema) error {
	// 添加空指针检查
	if schema == nil {
		return fmt.Errorf("send arrow batch schema cannot be nil")
	}

	conf := config.GetConfigMap()
	if conf == nil {
		return fmt.Errorf("config is nil")
	}

	feURL := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		conf.DorisConfig.Address, conf.DorisConfig.Port, dbName, tableName)

	// label 用于幂等；使用随机函数生成
	randomStr, err := common.GenerateRandomString(10)
	if err != nil {
		return fmt.Errorf("failed to generate random string for label: %v", err)
	}
	label := fmt.Sprintf("import_%d_%s", time.Now().UnixNano(), randomStr)

	// 认证信息；若无密码则会是 "root:"
	auth := fmt.Sprintf("%s:%s", conf.DorisConfig.User, conf.DorisConfig.Password)

	// 构建 curl 命令
	args := []string{
		"--location-trusted",
		"-u", auth,
		"-H", "Expect: 100-continue",
		"-H", "format:parquet", // 某些版本也支持 file_type:parquet
		"-H", fmt.Sprintf("label:%s", label),
		"-T", filePath,
		feURL,
	}

	cmd := exec.Command("curl", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	log.Logger.Infof("Executing curl stream load: curl %s", strings.Join(args, " "))

	if err := cmd.Run(); err != nil {
		log.Logger.Errorf("curl stderr: %s", stderr.String())
		log.Logger.Errorf("curl stdout: %s", stdout.String())
		return fmt.Errorf("curl stream load failed: %v, stderr: %s", err, stderr.String())
	}

	log.Logger.Infof("curl stream load output: %s", stdout.String())

	// Doris FE 返回 JSON，沿用现有解析与校验
	if err := checkStreamloaderOutput(stdout.String()); err != nil {
		return err
	}
	return nil
}
