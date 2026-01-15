package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
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
	"github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
)

type ParquetStreamingService struct {
	chunkService *ChunkService
	ossClient    oss.ClientInterface
}

func NewParquetStreamingService(chunkService *ChunkService, ossClient oss.ClientInterface) *ParquetStreamingService {
	return &ParquetStreamingService{
		chunkService: chunkService,
		ossClient:    ossClient,
	}
}

// StreamParquetFileFromOSS 从OSS按顺序读取 parquet 分片并流式返回 Arrow 批次
func (s *ParquetStreamingService) StreamParquetFileFromOSS(tableName, jobInstanceId string, columns []string, stream datasource.DataSourceService_ReadDataSourceStreamingServer) error {
	bucketName := common.BATCH_DATA_BUCKET_NAME
	keys, err := s.listAndSortParquetParts(bucketName, jobInstanceId)
	if err != nil {
		return fmt.Errorf("failed to list and sort parquet parts: %v", err)
	}
	if len(keys) == 0 {
		return fmt.Errorf("no parquet parts found for jobInstanceId %s in bucket %s", jobInstanceId, bucketName)
	}

	// 第一阶段：下载所有文件
	log.Logger.Infof("Starting to download %d Parquet parts", len(keys))
	var localFilePaths []string
	for i, key := range keys {
		if stream.Context().Err() != nil {
			log.Logger.Errorf("Stream context error: %v", stream.Context().Err())
			// 清理已下载的文件
			for _, path := range localFilePaths {
				if rmErr := os.Remove(path); rmErr != nil {
					log.Logger.Warnf("Failed to cleanup temp file %s: %v", path, rmErr)
				}
			}
			return fmt.Errorf("stream connection lost: %v", stream.Context().Err())
		}
		log.Logger.Infof("Downloading Parquet part %d/%d: %s", i+1, len(keys), key)

		var localFilePath string
		err := utils.WithRetryCtx(
			stream.Context(),
			5,                    // 最大重试3次
			200*time.Millisecond, // 基础延迟
			10*time.Second,       // 最大延迟
			func() error {
				var downloadErr error
				localFilePath, downloadErr = s.downloadSinglePart(stream.Context(), bucketName, key, tableName, jobInstanceId, i)
				return downloadErr
			},
			utils.IsRetryableNetErr, // 只对网络错误重试
		)
		if err != nil {
			// 清理已下载的文件
			for _, path := range localFilePaths {
				if rmErr := os.Remove(path); rmErr != nil {
					log.Logger.Warnf("Failed to cleanup temp file %s: %v", path, rmErr)
				}
			}
			return fmt.Errorf("failed to download part %s: %v", key, err)
		}
		localFilePaths = append(localFilePaths, localFilePath)
	}
	log.Logger.Infof("Successfully downloaded all %d Parquet parts", len(localFilePaths))

	// 第二阶段：处理所有文件
	log.Logger.Infof("Starting to process %d Parquet parts", len(localFilePaths))
	var totalRecords int64
	for i, localFilePath := range localFilePaths {
		if stream.Context().Err() != nil {
			log.Logger.Errorf("Stream context error: %v", stream.Context().Err())
			// 清理所有临时文件
			for _, path := range localFilePaths {
				if rmErr := os.Remove(path); rmErr != nil {
					log.Logger.Warnf("Failed to cleanup temp file %s: %v", path, rmErr)
				}
			}
			return fmt.Errorf("stream connection lost: %v", stream.Context().Err())
		}
		log.Logger.Infof("Processing Parquet part %d/%d: %s", i+1, len(localFilePaths), localFilePath)

		n, err := s.processParquetPart(localFilePath, columns, stream)
		if err != nil {
			// 清理所有临时文件
			for _, path := range localFilePaths {
				if rmErr := os.Remove(path); rmErr != nil {
					log.Logger.Warnf("Failed to cleanup temp file %s: %v", path, rmErr)
				}
			}
			return fmt.Errorf("failed to process part %s: %v", localFilePath, err)
		}
		totalRecords += n
	}

	// 清理所有临时文件
	log.Logger.Infof("Cleaning up %d temporary files", len(localFilePaths))
	for _, path := range localFilePaths {
		if rmErr := os.Remove(path); rmErr != nil {
			log.Logger.Warnf("Failed to cleanup temp file %s: %v", path, rmErr)
		}
	}

	log.Logger.Infof("Successfully streamed all Parquet parts for jobInstanceId %s, total records: %d", jobInstanceId, totalRecords)
	return nil
}

func (s *ParquetStreamingService) listAndSortParquetParts(bucketName, jobInstanceId string) ([]string, error) {
	prefix := fmt.Sprintf("%s/", jobInstanceId)
	keys, err := s.ossClient.ListObjects(context.Background(), bucketName, prefix, true)
	if err != nil {
		return nil, err
	}
	// 匹配类似 ..._0.parquet / ...-0.parquet / ...0.parquet
	re := regexp.MustCompile(`(\d+)\.parquet$`)
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
	log.Logger.Infof("Found %d parquet parts for jobInstanceId %s", len(out), jobInstanceId)
	return out, nil
}

func (s *ParquetStreamingService) downloadSinglePart(ctx context.Context, bucketName, objectName, tableName, jobInstanceId string, partIndex int) (string, error) {
	tempDir := common.DATA_DIR
	localFileName := fmt.Sprintf("parquet_part_%s_%s_%d_%d.parquet", tableName, jobInstanceId, partIndex, time.Now().Unix())
	localFilePath := filepath.Join(tempDir, localFileName)

	reader, err := s.ossClient.GetObject(ctx, bucketName, objectName, &oss.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get object from OSS: %v", err)
	}
	defer reader.Close()

	file, err := os.Create(localFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %v", err)
	}
	defer file.Close()

	if _, err = io.Copy(file, reader); err != nil {
		return "", fmt.Errorf("failed to copy object content to local file: %v", err)
	}
	return localFilePath, nil
}

func (s *ParquetStreamingService) processParquetPart(filePath string, columns []string, stream datasource.DataSourceService_ReadDataSourceStreamingServer) (int64, error) {
	mem := memory.NewGoAllocator()
	pf, err := file.OpenParquetFile(filePath, false) // 返回 source.ParquetFile
	if err != nil {
		return 0, fmt.Errorf("failed to open parquet file: %v", err)
	}
	defer pf.Close()

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return 0, fmt.Errorf("failed to create pqarrow reader: %v", err)
	}

	table, err := reader.ReadTable(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to read parquet as table: %v", err)
	}
	defer table.Release()

	// 逐批（record batch）读取并发送
	batchSize := config.GetConfigMap().StreamConfig.ParquetBatchSize
	tr := array.NewTableReader(table, int64(batchSize))
	defer tr.Release()

	var total int64
	var chunkID int64
	for tr.Next() {
		rec := tr.Record()
		if rec == nil {
			continue
		}
		// 可选投影
		sendRec := rec
		if len(columns) > 0 {
			proj, err := projectRecordByNames(rec, columns)
			if err != nil {
				rec.Release()
				return total, fmt.Errorf("failed to project record: %v", err)
			}
			rec.Release()
			sendRec = proj
		} else {
			// 与后续统一的生命周期
			rec.Retain()
			rec.Release()
		}

		if err := sendArrowRecord(stream, sendRec, chunkID); err != nil {
			sendRec.Release()
			return total, fmt.Errorf("failed to send record: %v", err)
		}
		total += int64(sendRec.NumRows())
		sendRec.Release()
		chunkID++
	}
	return total, nil
}

func projectRecordByNames(rec arrow.Record, cols []string) (arrow.Record, error) {
	schema := rec.Schema()
	var fields []arrow.Field
	var arrays []arrow.Array
	for _, name := range cols {
		indices := schema.FieldIndices(name)
		if len(indices) == 0 {
			return nil, fmt.Errorf("column %s not found in record", name)
		}
		idx := indices[0]
		fields = append(fields, schema.Field(idx))
		arr := rec.Column(idx)
		arr.Retain()
		arrays = append(arrays, arr)
	}
	newSchema := arrow.NewSchema(fields, nil)
	return array.NewRecord(newSchema, arrays, rec.NumRows()), nil
}

func sendArrowRecord(stream datasource.DataSourceService_ReadDataSourceStreamingServer, rec arrow.Record, chunkID int64) error {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		return fmt.Errorf("failed to write arrow record: %v", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close arrow writer: %v", err)
	}
	if err := stream.Send(&datasource.ArrowResponse{ArrowBatch: buf.Bytes()}); err != nil {
		return fmt.Errorf("failed to send arrow response: %v", err)
	}
	log.Logger.Debugf("Successfully sent parquet batch %d with %d rows", chunkID, rec.NumRows())
	return nil
}
