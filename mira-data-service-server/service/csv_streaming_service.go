/*
*

	@author: shiliang
	@date: 2024/12/19
	@note: CSV流式读取服务

*
*/
package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"data-service/common"
	"data-service/config"
	"data-service/generated/datasource"
	"data-service/log"
	"data-service/oss"
	"data-service/utils"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

// CSVStreamingService CSV流式读取服务
type CSVStreamingService struct {
	chunkService *ChunkService
	ossClient    oss.ClientInterface
	dorisService IDorisService // 添加DorisService
}

// NewCSVStreamingService 创建CSV流式读取服务
func NewCSVStreamingService(chunkService *ChunkService, ossClient oss.ClientInterface, dorisService IDorisService) *CSVStreamingService {
	return &CSVStreamingService{
		chunkService: chunkService,
		ossClient:    ossClient,
		dorisService: dorisService,
	}
}

// StreamCSVFileFromOSS 从OSS下载CSV文件并分块流式读取
func (s *CSVStreamingService) StreamCSVFileFromOSS(tableName, jobInstanceId string, columns []string, stream datasource.DataSourceService_ReadDataSourceStreamingServer) error {
	// 1) 列出并排序 CSV 分片
	bucketName := common.BATCH_DATA_BUCKET_NAME
	keys, err := s.listAndSortCSVParts(bucketName, jobInstanceId)
	if err != nil {
		return fmt.Errorf("failed to list and sort csv parts: %v", err)
	}
	if len(keys) == 0 {
		return fmt.Errorf("no csv parts found for jobInstanceId %s in bucket %s", jobInstanceId, bucketName)
	}

	// 2) 分块流式读取本地文件
	return s.streamCSVPartsSequentially(bucketName, keys, tableName, jobInstanceId, columns, stream)
}

// cleanupTempFile 清理临时文件
func (s *CSVStreamingService) cleanupTempFile(filePath string) {
	if err := os.Remove(filePath); err != nil {
		// 记录日志但不返回错误，因为这是清理操作
		log.Logger.Warnf("Failed to cleanup temp file %s: %v", filePath, err)
	}
}

// readChunkData 读取指定分块的数据
func (s *CSVStreamingService) readChunkData(filePath string, chunk *common.ChunkInfo) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 定位到分块起始位置
	if _, err := file.Seek(chunk.StartOffset, io.SeekStart); err != nil {
		return nil, err
	}

	// 读取分块数据
	data := make([]byte, chunk.ChunkSize)
	n, err := io.ReadFull(file, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	return data[:n], nil
}

// streamCSVPartsSequentially 按顺序逐个处理 CSV 分片文件
func (s *CSVStreamingService) streamCSVPartsSequentially(bucketName string, keys []string, tableName, jobInstanceId string, columns []string, stream datasource.DataSourceService_ReadDataSourceStreamingServer) error {
	var totalRecords int64
	var sentData bool

	// 先全部下载到本地
	localFiles := make([]string, 0, len(keys))
	for i, key := range keys {
		// 检查流状态
		if stream.Context().Err() != nil {
			log.Logger.Errorf("Stream context error during download: %v", stream.Context().Err())
			// 清理已下载文件
			for _, f := range localFiles {
				s.cleanupTempFile(f)
			}
			return fmt.Errorf("stream connection lost: %v", stream.Context().Err())
		}

		log.Logger.Infof("Downloading CSV part %d/%d: %s", i+1, len(keys), key)

		var localFilePath string
		err := utils.WithRetryCtx(stream.Context(), 5, 200*time.Millisecond, 10*time.Second, func() error {
			p, e := s.downloadSinglePart(stream.Context(), bucketName, key, tableName, jobInstanceId, i)
			if e != nil {
				return e
			}
			localFilePath = p
			return nil
		}, utils.IsRetryableNetErr)
		if err != nil {
			// 清理已下载文件
			for _, f := range localFiles {
				s.cleanupTempFile(f)
			}
			return fmt.Errorf("failed to download part %s after retries: %v", key, err)
		}

		localFiles = append(localFiles, localFilePath)
	}

	// 确保函数退出时统一清理本地文件
	defer func() {
		for _, f := range localFiles {
			s.cleanupTempFile(f)
		}
	}()

	// 再按顺序读取本地文件
	for i, localFilePath := range localFiles {
		// 检查流状态
		if stream.Context().Err() != nil {
			log.Logger.Errorf("Stream context error during processing: %v", stream.Context().Err())
			return fmt.Errorf("stream connection lost: %v", stream.Context().Err())
		}

		log.Logger.Infof("Processing downloaded CSV part %d/%d: %s", i+1, len(localFiles), localFilePath)

		partRecords, err := s.processCSVPart(localFilePath, tableName, jobInstanceId, columns, stream)
		if err != nil {
			return fmt.Errorf("failed to process part %s: %v", localFilePath, err)
		}

		totalRecords += partRecords
		sentData = true

		// 添加发送间隔，避免过快发送
		time.Sleep(10 * time.Millisecond)
	}

	if !sentData {
		// 发送空的Arrow批次
		emptyBatch, err := utils.ConvertToEmptyArrowBatch(nil)
		if err != nil {
			return fmt.Errorf("failed to create empty arrow batch: %v", err)
		}
		response := &datasource.ArrowResponse{
			ArrowBatch: emptyBatch,
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send empty response: %v", err)
		}
	}

	log.Logger.Infof("Successfully streamed all CSV parts, total records: %d", totalRecords)
	return nil
}

// downloadSinglePart 下载单个分片文件到本地
func (s *CSVStreamingService) downloadSinglePart(ctx context.Context, bucketName, objectName, tableName, jobInstanceId string, partIndex int) (string, error) {
	tempDir := common.DATA_DIR
	localFileName := fmt.Sprintf("csv_part_%s_%s_%d_%d.csv", tableName, jobInstanceId, partIndex, time.Now().Unix())
	localFilePath := filepath.Join(tempDir, localFileName)

	// 从OSS获取对象
	reader, err := s.ossClient.GetObject(ctx, bucketName, objectName, &oss.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get object from OSS: %v", err)
	}
	defer reader.Close()

	// 创建本地文件
	file, err := os.Create(localFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %v", err)
	}
	defer file.Close()

	// 将OSS对象内容复制到本地文件
	_, err = io.Copy(file, reader)
	if err != nil {
		return "", fmt.Errorf("failed to copy object content to local file: %v", err)
	}

	return localFilePath, nil
}

// processCSVPart 处理单个 CSV 分片文件，按行读取并发送
func (s *CSVStreamingService) processCSVPart(csvFilePath, tableName, jobInstanceId string, columns []string, stream datasource.DataSourceService_ReadDataSourceStreamingServer) (int64, error) {
	// 打开CSV文件
	file, err := os.Open(csvFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer file.Close()

	// Debug：打印文件前5行
	s.debugLogFirstLines(csvFilePath, 5)

	// 创建CSV读取器
	reader := csv.NewReader(file)
	reader.Comma = '\x01' // ← 分隔符
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true

	var totalRecords int64
	var lineCount int64
	var batchLines []string
	conf := config.GetConfigMap()

	// 如果是第一个分片，读取并跳过表头
	headers, err := reader.Read()
	if err != nil {
		return 0, fmt.Errorf("failed to read CSV headers: %v", err)
	}
	log.Logger.Debugf("CSV headers: %v", headers)
	lineCount++ // 表头也算一行

	// 按行读取CSV文件
	chunkID := int64(0) // 初始化包编号
	for {
		record, err := reader.Read()
		if err == io.EOF {
			// 文件读取完毕，发送最后一批数据
			if len(batchLines) > 0 {
				if err := s.sendBatchData(stream, batchLines, jobInstanceId, tableName, columns, chunkID); err != nil {
					return totalRecords, fmt.Errorf("failed to send final batch: %v", err)
				}
				totalRecords += int64(len(batchLines))
			}
			break
		}
		if err != nil {
			return totalRecords, fmt.Errorf("failed to read CSV line %d: %v", lineCount+1, err)
		}

		lineCount++
		// cleanCSVRecord(record)
		batchLines = append(batchLines, strings.Join(record, string([]byte{0x01})))

		// 当达到5000行时，发送一批数据
		if len(batchLines) >= conf.StreamConfig.BatchLines {
			if err := s.sendBatchData(stream, batchLines, jobInstanceId, tableName, columns, chunkID); err != nil {
				return totalRecords, fmt.Errorf("failed to send batch at line %d: %v", lineCount, err)
			}
			totalRecords += int64(len(batchLines))
			log.Logger.Debugf("Sent batch of %d lines, total sent: %d", len(batchLines), totalRecords)

			// 清空批次，准备下一批
			batchLines = nil
			chunkID++ // 递增包编号

			// 添加小延迟，避免过快发送
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Logger.Infof("Processed CSV part: %s, total lines: %d, total records sent: %d", csvFilePath, lineCount, totalRecords)
	return totalRecords, nil
}

// sendBatchData 发送一批CSV行数据
func (s *CSVStreamingService) sendBatchData(stream datasource.DataSourceService_ReadDataSourceStreamingServer, batchLines []string, jobInstanceId string, tableName string, columns []string, chunkID int64) error {
	// 将CSV行数据转换为Arrow格式
	arrowBatch, err := s.convertCSVLinesToArrow(batchLines, jobInstanceId, tableName, columns)
	if err != nil {
		return fmt.Errorf("failed to convert CSV lines to arrow: %v", err)
	}
	defer arrowBatch.Release()

	// 序列化Arrow数据
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(arrowBatch.Schema()))
	if err := writer.Write(arrowBatch); err != nil {
		return fmt.Errorf("failed to write arrow record: %v", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close arrow writer: %v", err)
	}

	// 发送数据（带重试机制）
	if err := s.sendWithRetry(stream, &datasource.ArrowResponse{
		ArrowBatch: buf.Bytes(),
	}, chunkID); err != nil {
		return fmt.Errorf("failed to send batch data: %v", err)
	}

	log.Logger.Debugf("Successfully sent batch %d with %d lines", chunkID, len(batchLines))
	return nil
}

// sendWithRetry 带重试机制的数据发送
func (s *CSVStreamingService) sendWithRetry(stream datasource.DataSourceService_ReadDataSourceStreamingServer, response *datasource.ArrowResponse, chunkID int64) error {
	maxRetries := 3
	retryDelay := 100 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// 检查流状态
		if stream.Context().Err() != nil {
			return fmt.Errorf("stream context error: %v", stream.Context().Err())
		}

		// 尝试发送
		err := stream.Send(response)
		if err == nil {
			// 发送成功
			if attempt > 0 {
				log.Logger.Infof("Chunk %d sent successfully after %d retries", chunkID, attempt)
			}
			return nil
		}

		// 发送失败，记录错误
		log.Logger.Warnf("Failed to send chunk %d (attempt %d/%d): %v", chunkID, attempt+1, maxRetries+1, err)

		// 如果是最后一次尝试，返回错误
		if attempt == maxRetries {
			return fmt.Errorf("failed to send chunk %d after %d attempts: %v", chunkID, maxRetries+1, err)
		}

		// 等待一段时间后重试
		time.Sleep(retryDelay)
		retryDelay *= 2 // 指数退避

		// 检查流状态
		if stream.Context().Err() != nil {
			return fmt.Errorf("stream context error during retry: %v", stream.Context().Err())
		}
	}

	return fmt.Errorf("unexpected retry loop exit for chunk %d", chunkID)
}

// convertCSVLinesToArrow 将CSV行数据转换为Arrow格式
func (s *CSVStreamingService) convertCSVLinesToArrow(csvLines []string, jobInstanceId string, tableName string, columns []string) (arrow.Record, error) {
	// 1) 解析 CSV 行（支持引号/转义/逗号）
	r := csv.NewReader(strings.NewReader(strings.Join(csvLines, "\n")))
	r.Comma = '\x01' // ← 分隔符
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV lines: %v", err)
	}

	// 2) 获取表结构
	tableColumns, err := s.dorisService.GetDorisTableSchema(s.extractDbNameFromJobInstanceId(jobInstanceId), tableName)
	if err != nil {
		log.Logger.Warnf("Get table schema from %s failed: %v, fallback to %s", s.extractDbNameFromJobInstanceId(jobInstanceId), err, common.MIRA_TMP_TASK_DB)
		tableColumns, err = s.dorisService.GetDorisTableSchema(common.MIRA_TMP_TASK_DB, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get table schema: %v", err)
		}
	}

	// 3) 构建 Arrow Schema
	schema, err := s.buildArrowSchemaFromColumns(tableColumns, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to build arrow schema: %v", err)
	}

	// 4) 创建 Arrow Record
	record, err := s.createArrowRecordFromCSV(records, schema, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow record: %v", err)
	}
	log.Logger.Debugf("convertCSVLinesToArrow: schemaFields=%d, reqColumns=%d", len(schema.Fields()), len(columns))
	return record, nil
}

// extractDbNameFromJobInstanceId 从jobInstanceId中提取数据库名
func (s *CSVStreamingService) extractDbNameFromJobInstanceId(jobInstanceId string) string {
	if idx := strings.LastIndex(jobInstanceId, "@"); idx > 0 {
		suffix := jobInstanceId[idx+1:]
		if len(suffix) == common.SUFFIX_RANDOM_LENGTH &&
			regexp.MustCompile(`(?i)^[0-9a-f]+$`).MatchString(suffix) {
			return jobInstanceId[:idx]
		}
	}
	return jobInstanceId
}

// buildArrowSchemaFromColumns 根据列信息构建Arrow Schema
func (s *CSVStreamingService) buildArrowSchemaFromColumns(tableColumns []*datasource.ColumnItem, selectedColumns []string) (*arrow.Schema, error) {
	var fields []arrow.Field

	// 如果没有指定列，使用所有列
	if len(selectedColumns) == 0 {
		for _, col := range tableColumns {
			arrowType, err := s.convertDorisTypeToArrowType(col.DataType)
			if err != nil {
				return nil, fmt.Errorf("failed to convert column type %s: %v", col.Name, err)
			}
			fields = append(fields, arrow.Field{
				Name: col.Name,
				Type: arrowType,
			})
		}
	} else {
		// 使用指定的列
		for _, colName := range selectedColumns {
			var col *datasource.ColumnItem
			for _, tc := range tableColumns {
				if tc.Name == colName {
					col = tc
					break
				}
			}
			if col == nil {
				return nil, fmt.Errorf("column %s not found in table", colName)
			}

			arrowType, err := s.convertDorisTypeToArrowType(col.DataType)
			if err != nil {
				return nil, fmt.Errorf("failed to convert column type %s: %v", col.Name, err)
			}
			fields = append(fields, arrow.Field{
				Name: col.Name,
				Type: arrowType,
			})
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// createArrowRecordFromCSV 从CSV数据创建Arrow Record
func (s *CSVStreamingService) createArrowRecordFromCSV(records [][]string, schema *arrow.Schema, columns []string) (arrow.Record, error) {
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// 当 columns 为空时，用 schema 的列数
	colCount := len(columns)
	if colCount == 0 {
		colCount = len(schema.Fields())
	}

	for colIdx := 0; colIdx < colCount; colIdx++ {
		field := schema.Field(colIdx)
		fieldBuilder := builder.Field(colIdx)

		// 统一获取并标准化当前单元格值：去空格，并将 \N / \\N / NULL 视为缺失，返回空字符串
		getRaw := func(rowIdx int) string {
			if colIdx >= len(records[rowIdx]) {
				return ""
			}
			return strings.TrimSpace(normalizeNullToken(records[rowIdx][colIdx]))
		}

		switch field.Type.ID() {
		case arrow.STRING:
			sb := fieldBuilder.(*array.StringBuilder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				sb.Append(raw)
			}
		case arrow.LARGE_STRING:
			sb := fieldBuilder.(*array.LargeStringBuilder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				sb.Append(raw)
			}
		case arrow.INT8:
			b := fieldBuilder.(*array.Int8Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseInt(raw, 10, 8)
				if err != nil {
					return nil, fmt.Errorf("column %d parse int8 failed: %v", colIdx, err)
				}
				b.Append(int8(v))
			}
		case arrow.INT16:
			b := fieldBuilder.(*array.Int16Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseInt(raw, 10, 16)
				if err != nil {
					return nil, fmt.Errorf("column %d parse int16 failed: %v", colIdx, err)
				}
				b.Append(int16(v))
			}
		case arrow.INT32:
			b := fieldBuilder.(*array.Int32Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseInt(raw, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("column %d parse int32 failed: %v", colIdx, err)
				}
				b.Append(int32(v))
			}
		case arrow.INT64:
			b := fieldBuilder.(*array.Int64Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("column %d parse int64 failed: %v", colIdx, err)
				}
				b.Append(v)
			}
		case arrow.UINT8:
			b := fieldBuilder.(*array.Uint8Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseUint(raw, 10, 8)
				if err != nil {
					return nil, fmt.Errorf("column %d parse uint8 failed: %v", colIdx, err)
				}
				b.Append(uint8(v))
			}
		case arrow.UINT16:
			b := fieldBuilder.(*array.Uint16Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseUint(raw, 10, 16)
				if err != nil {
					return nil, fmt.Errorf("column %d parse uint16 failed: %v", colIdx, err)
				}
				b.Append(uint16(v))
			}
		case arrow.UINT32:
			b := fieldBuilder.(*array.Uint32Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseUint(raw, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("column %d parse uint32 failed: %v", colIdx, err)
				}
				b.Append(uint32(v))
			}
		case arrow.UINT64:
			b := fieldBuilder.(*array.Uint64Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseUint(raw, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("column %d parse uint64 failed: %v", colIdx, err)
				}
				b.Append(v)
			}
		case arrow.FLOAT32:
			b := fieldBuilder.(*array.Float32Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				f, err := strconv.ParseFloat(raw, 32)
				if err != nil {
					return nil, fmt.Errorf("column %d parse float32 failed: %v", colIdx, err)
				}
				b.Append(float32(f))
			}
		case arrow.FLOAT64:
			b := fieldBuilder.(*array.Float64Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				f, err := strconv.ParseFloat(raw, 64)
				if err != nil {
					return nil, fmt.Errorf("column %d parse float64 failed: %v", colIdx, err)
				}
				b.Append(f)
			}
		case arrow.BOOL:
			b := fieldBuilder.(*array.BooleanBuilder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				v, err := strconv.ParseBool(raw)
				if err != nil {
					return nil, fmt.Errorf("column %d parse bool failed: %v", colIdx, err)
				}
				b.Append(v)
			}
		case arrow.TIME32:
			b := fieldBuilder.(*array.Time32Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				if err := utils.AppendValueToBuilder(fieldBuilder, raw); err != nil {
					return nil, fmt.Errorf("column %d parse time32 failed: %v", colIdx, err)
				}
			}
		case arrow.TIMESTAMP:
			b := fieldBuilder.(*array.TimestampBuilder)
			unit := field.Type.(*arrow.TimestampType).Unit
			formats := []string{
				"2006-01-02 15:04:05",
				"2006-01-02 15:04:05.000",
				"2006-01-02T15:04:05Z07:00",
				"2006-01-02",
			}
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				var parsed time.Time
				var err error
				for _, f := range formats {
					parsed, err = time.ParseInLocation(f, raw, time.Local)
					if err == nil {
						break
					}
				}
				if err != nil {
					return nil, fmt.Errorf("column %d parse timestamp failed: %v", colIdx, err)
				}
				switch unit {
				case arrow.Second:
					b.Append(arrow.Timestamp(parsed.Unix()))
				case arrow.Millisecond:
					b.Append(arrow.Timestamp(parsed.UnixNano() / 1e6))
				case arrow.Microsecond:
					b.Append(arrow.Timestamp(parsed.UnixNano() / 1e3))
				case arrow.Nanosecond:
					b.Append(arrow.Timestamp(parsed.UnixNano()))
				default:
					return nil, fmt.Errorf("unsupported timestamp unit: %v", unit)
				}
			}
		case arrow.DATE32:
			b := fieldBuilder.(*array.Date32Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				t, err := time.Parse("2006-01-02", raw)
				if err != nil {
					return nil, fmt.Errorf("column %d parse date32 failed: %v", colIdx, err)
				}
				days := int32(t.Unix() / 86400)
				b.Append(arrow.Date32(days))
			}
		case arrow.DATE64:
			b := fieldBuilder.(*array.Date64Builder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				t, err := time.Parse("2006-01-02", raw)
				if err != nil {
					return nil, fmt.Errorf("column %d parse date64 failed: %v", colIdx, err)
				}
				milliseconds := t.UnixMilli()
				b.Append(arrow.Date64(milliseconds))
			}
		case arrow.BINARY:
			b := fieldBuilder.(*array.BinaryBuilder)
			for rowIdx := 0; rowIdx < len(records); rowIdx++ {
				raw := getRaw(rowIdx)
				if raw == "" {
					b.AppendNull()
					continue
				}
				data, err := hex.DecodeString(raw)
				if err != nil {
					return nil, fmt.Errorf("column %d parse binary failed: %v", colIdx, err)
				}
				b.Append(data)
			}
		default:
			// 其他类型统一按字符串写入
			if err := func() error {
				for rowIdx := 0; rowIdx < len(records); rowIdx++ {
					raw := ""
					if colIdx < len(records[rowIdx]) {
						raw = normalizeNullToken(records[rowIdx][colIdx]) // 保持与其他类型一致的标准化
					}
					if err := utils.AppendValueToBuilder(fieldBuilder, raw); err != nil {
						return fmt.Errorf("failed to append value to column %d: %v", colIdx, err)
					}
				}
				return nil
			}(); err != nil {
				return nil, err
			}
		}
	}

	return builder.NewRecord(), nil
}

// convertDorisTypeToArrowType 将Doris数据类型转换为Arrow数据类型
func (s *CSVStreamingService) convertDorisTypeToArrowType(dorisType string) (arrow.DataType, error) {
	upperType := strings.ToUpper(strings.TrimSpace(dorisType))

	switch upperType {
	case "INT", "INTEGER":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "VARCHAR", "STRING", "TEXT":
		return arrow.BinaryTypes.String, nil
	case "DOUBLE", "FLOAT":
		return arrow.PrimitiveTypes.Float64, nil
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "TIMESTAMP":
		return arrow.FixedWidthTypes.Timestamp_ns, nil
	default:
		// 处理 DECIMAL 和 NUMERIC 类型（支持带精度的格式）
		if strings.Contains(upperType, "DECIMAL") || strings.Contains(upperType, "NUMERIC") {
			// 尝试从类型字符串中提取精度和标度
			precision, scale := int32(38), int32(10) // 默认值

			// 正则提取 DECIMAL(precision, scale)
			re := regexp.MustCompile(`\((\d+),\s*(\d+)\)`)
			matches := re.FindStringSubmatch(upperType)
			if len(matches) == 3 {
				if p, err := strconv.ParseInt(matches[1], 10, 32); err == nil {
					precision = int32(p)
				}
				if s, err := strconv.ParseInt(matches[2], 10, 32); err == nil {
					scale = int32(s)
				}
			}

			return &arrow.Decimal128Type{Precision: precision, Scale: scale}, nil
		}
		// 默认使用字符串类型
		return arrow.BinaryTypes.String, nil
	}
}

// convertRowsToArrowRecord 将CSV行数据转换为Arrow Record
func (s *CSVStreamingService) convertRowsToArrowRecord(rows [][]string, schema *arrow.Schema, columns []*datasource.ColumnItem, headers []string) (arrow.Record, error) {
	if len(rows) == 0 {
		// 返回空记录
		return array.NewRecord(schema, []arrow.Array{}, 0), nil
	}

	// 创建列名到索引的映射
	headerMap := make(map[string]int)
	for i, header := range headers {
		headerMap[strings.ToLower(header)] = i
	}

	// 为每列创建数组构建器
	builders := make([]array.Builder, len(columns))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(memory.DefaultAllocator, field.Type)
		defer builders[i].Release()
	}

	// 逐行处理数据
	for _, row := range rows {
		for i, col := range columns {
			// 根据列名在CSV行中找到对应的值
			colIndex, exists := headerMap[strings.ToLower(col.Name)]
			var value string
			if exists && colIndex < len(row) {
				value = row[colIndex]
			} else {
				value = "" // 列不存在时使用空值
			}

			// 根据列类型转换并添加值
			if err := s.appendValueToBuilder(builders[i], value, col.DataType); err != nil {
				return nil, fmt.Errorf("failed to append value to column %s: %v", col.Name, err)
			}
		}
	}

	// 构建数组
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	return array.NewRecord(schema, arrays, int64(len(rows))), nil
}

// appendValueToBuilder 将值添加到对应的数组构建器中
func (s *CSVStreamingService) appendValueToBuilder(builder array.Builder, value string, dataType string) error {
	switch strings.ToUpper(dataType) {
	case "INT", "INTEGER":
		if value == "" {
			builder.(*array.Int32Builder).AppendNull()
		} else {
			if intVal, err := strconv.Atoi(value); err == nil {
				builder.(*array.Int32Builder).Append(int32(intVal))
			} else {
				builder.(*array.Int32Builder).AppendNull()
			}
		}
	case "BIGINT":
		if value == "" {
			builder.(*array.Int64Builder).AppendNull()
		} else {
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				builder.(*array.Int64Builder).Append(intVal)
			} else {
				builder.(*array.Int64Builder).AppendNull()
			}
		}
	case "VARCHAR", "STRING", "TEXT":
		builder.(*array.StringBuilder).Append(value)
	case "DOUBLE", "FLOAT":
		if value == "" {
			builder.(*array.Float64Builder).AppendNull()
		} else {
			if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
				builder.(*array.Float64Builder).Append(floatVal)
			} else {
				builder.(*array.Float64Builder).AppendNull()
			}
		}
	case "BOOLEAN", "BOOL":
		if value == "" {
			builder.(*array.BooleanBuilder).AppendNull()
		} else {
			boolVal := strings.ToLower(value) == "true" || value == "1"
			builder.(*array.BooleanBuilder).Append(boolVal)
		}
	default:
		// 默认作为字符串处理
		builder.(*array.StringBuilder).Append(value)
	}

	return nil
}

// listAndSortCSVParts 列出 jobInstanceId 下的 CSV 分片并按文件名末尾数字升序排序
func (s *CSVStreamingService) listAndSortCSVParts(bucketName, jobInstanceId string) ([]string, error) {
	prefix := fmt.Sprintf("%s/", jobInstanceId)
	keys, err := s.ossClient.ListObjects(context.Background(), bucketName, prefix, true)
	if err != nil {
		return nil, err
	}

	// 只匹配文件名末尾的数字，不管前缀是什么
	re := regexp.MustCompile(`(\d+)\.csv$`)

	type item struct {
		key   string
		index int
	}
	var parts []item
	for _, k := range keys {
		m := re.FindStringSubmatch(k)
		if len(m) == 0 {
			continue
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		parts = append(parts, item{key: k, index: n})
	}

	sort.Slice(parts, func(i, j int) bool { return parts[i].index < parts[j].index })

	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, p.key)
	}
	log.Logger.Infof("Found %d csv parts for jobInstanceId %s", len(out), jobInstanceId)
	return out, nil
}

// debugLogFirstLines 调试打印文件的前 n 行（不改变原有读取流程）
func (s *CSVStreamingService) debugLogFirstLines(filePath string, n int) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Logger.Debugf("Open file for preview failed: %v", err)
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	const maxCapacity = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, maxCapacity)

	var lines []string
	for i := 0; i < n && sc.Scan(); i++ {
		line := sc.Text()
		if len(line) > 1000 {
			line = line[:1000] + "...(truncated)"
		}
		lines = append(lines, line)
	}
	if err := sc.Err(); err != nil {
		log.Logger.Debugf("Scan file for preview failed: %v", err)
		return
	}
	log.Logger.Debugf("Preview first %d lines of %s:\n%s", len(lines), filePath, strings.Join(lines, "\n"))
}

// 将常见的 NULL 记号标准化为空字符串
func normalizeNullToken(s string) string {
	t := strings.TrimSpace(s)
	if t == `\N` || t == `\\N` || strings.EqualFold(t, "NULL") {
		return ""
	}
	return s
}

func cleanCSVRecord(record []string) {
	for i := range record {
		if record[i] == "" {
			continue
		}
		v := record[i]
		v = strings.ReplaceAll(v, "\r\n", "")
		v = strings.ReplaceAll(v, "\n", "")
		v = strings.ReplaceAll(v, "\r", "")
		record[i] = v
	}
}
