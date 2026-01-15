package service

import (
	"context"
	"fmt"

	"data-service/common"
	"data-service/log"
	"data-service/oss"

	pb "data-service/generated/datasource"

	"google.golang.org/grpc"
)

// ReadService 处理数据读取相关的服务
type ReadService struct {
	ossClient     oss.ClientInterface
	dorisService  IDorisService
	chunkService  *ChunkService
	streamService *CSVStreamingService
	parquetStream *ParquetStreamingService
}

// NewReadService 创建一个新的ReadService实例
func NewReadService(ossClient oss.ClientInterface) (*ReadService, error) {
	dorisService, err := NewDorisService(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return nil, fmt.Errorf("failed to create doris service: %v", err)
	}

	chunkService := NewChunkService(ossClient)
	streamService := NewCSVStreamingService(chunkService, ossClient, dorisService)
	parquetStream := NewParquetStreamingService(chunkService, ossClient)

	return &ReadService{
		ossClient:     ossClient,
		dorisService:  dorisService,
		chunkService:  chunkService,
		streamService: streamService,
		parquetStream: parquetStream,
	}, nil
}

// ProcessReadRequest 处理读取请求
func (s *ReadService) ProcessReadRequest(request *pb.ReadRequest, g grpc.ServerStreamingServer[pb.ArrowResponse]) error {
	var tableName string
	randomSuffix, err := common.GenerateRandomString(common.SUFFIX_RANDOM_LENGTH)
	if err != nil {
		return fmt.Errorf("failed to generate random suffix: %v", err)
	}

	var enhancedJobInstanceId string
	// 将ReadRequest转换为ReadDataSourceStreamingRequest
	streamingRequest := &pb.ReadDataSourceStreamingRequest{
		Columns:          request.Columns,
		SortRules:        request.SortRules,
		FilterConditions: request.FilterConditions,
	}

	// 根据数据源类型设置相应的字段和生成jobInstanceId
	switch {
	case request.GetExternal() != nil:
		enhancedJobInstanceId = request.GetExternal().AssetName + "_" + randomSuffix

		// 导入数据
		importService := NewImportService()
		importResult, err := importService.ImportData(context.Background(), &pb.ImportDataRequest{
			Targets: []*pb.ImportTarget{
				{
					External:        request.GetExternal(),
					DbName:          enhancedJobInstanceId,
					TargetTableName: enhancedJobInstanceId + "_" + "internal",
					Columns:         request.Columns,
					Keys:            request.Keys,
				},
			},
		})

		if err != nil {
			return fmt.Errorf("failed to import data: %v", err)
		}
		log.Logger.Debugf("importResult: %v", importResult)

		tableName = importResult.Results[0].TargetTableName
		// 后续请求变成Doris数据源
		streamingRequest.DataSource = &pb.ReadDataSourceStreamingRequest_Doris{
			Doris: &pb.DorisDataSource{
				DbName:    enhancedJobInstanceId,
				TableName: tableName,
			},
		}

	case request.GetInternal() != nil:
		enhancedJobInstanceId = request.GetInternal().TableName + "_" + randomSuffix
		streamingRequest.DataSource = &pb.ReadDataSourceStreamingRequest_Internal{
			Internal: request.GetInternal(),
		}
	case request.GetDoris() != nil:
		enhancedJobInstanceId = request.GetDoris().DbName + "@" + randomSuffix
		streamingRequest.DataSource = &pb.ReadDataSourceStreamingRequest_Doris{
			Doris: request.GetDoris(),
		}
	default:
		return fmt.Errorf("no valid data source specified")
	}

	streamingRequest.JobInstanceId = enhancedJobInstanceId

	// 5.执行完成后清理资源
	defer func() {
		// 只在外部数据源时进行清理
		if request.GetExternal() != nil {
			// 删除Doris数据库（也可以放清理任务中）
			if err := s.dorisService.DropDatabase(enhancedJobInstanceId); err != nil {
				log.Logger.Warnf("Failed to drop database %s: %v", enhancedJobInstanceId, err)
			}

			// 删除Minio上的文件
			if err := s.cleanupMinioFiles(enhancedJobInstanceId); err != nil {
				log.Logger.Warnf("Failed to cleanup minio files for job %s: %v", enhancedJobInstanceId, err)
			}

			log.Logger.Infof("Cleaned up external data source resources for job %s", enhancedJobInstanceId)
		} else if request.GetDoris() != nil {
			// Doris数据源：只删除Minio文件
			if err := s.cleanupMinioFiles(enhancedJobInstanceId); err != nil {
				log.Logger.Warnf("Failed to cleanup minio files for job %s: %v", enhancedJobInstanceId, err)
			}

			log.Logger.Infof("Cleaned up Doris data source resources for job %s", enhancedJobInstanceId)
		} else {
			// 内部数据源：不进行清理
			log.Logger.Infof("Internal data source, no cleanup needed for job %s", enhancedJobInstanceId)
		}
	}()

	// 1.从数据源拉取数据到doris并导出到minio
	// bug 如果相同的任务读同一张表，会出现多读数据，需要加子路径做隔离
	tableName, err = s.dorisService.ProcessDataSourceAndExport(streamingRequest, enhancedJobInstanceId)
	if err != nil {
		return fmt.Errorf("failed to process data source and export: %v", err)
	}
	log.Logger.Infof("Successfully exported table %s to MinIO for jobInstanceId: %s", tableName, enhancedJobInstanceId)

	// 2.执行导出parquet文件到minio
	// 3.从minio流式读取arrow文件
	if err := s.parquetStream.StreamParquetFileFromOSS(tableName, enhancedJobInstanceId, request.Columns, g); err != nil {
		log.Logger.Errorf("failed to stream parquet file from OSS: %v", err)
		return fmt.Errorf("failed to stream parquet file from OSS: %v", err)
	}

	return nil
}

// cleanupMinioFiles 清理Minio上的文件
func (s *ReadService) cleanupMinioFiles(jobInstanceId string) error {
	// 从批量数据桶中删除相关文件
	err := s.ossClient.DeleteObjectsByJobInstanceId(context.Background(), common.BATCH_DATA_BUCKET_NAME, jobInstanceId)
	if err != nil {
		return fmt.Errorf("failed to delete objects from minio: %v", err)
	}

	return nil
}
