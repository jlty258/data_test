package main

import (
	"bytes"
	"context"
	"data-service/common"
	"data-service/config"
	"data-service/database"
	pb "data-service/generated/datasource"
	log2 "data-service/log"
	"data-service/oss"
	"data-service/server/routes"
	"data-service/service"
	"data-service/utils"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type Server struct {
	logger           *zap.SugaredLogger
	ossClient        oss.ClientInterface
	k8sService       service.K8sService
	tableInfoService service.TableInfoService
	pb.UnimplementedDataSourceServiceServer
}

var mu sync.Mutex

func (s Server) WriterExternalData(ctx context.Context, request *pb.WriterExternalDataRequest) (*pb.Response, error) {
	connInfo, err := utils.GetDatasourceByAssetName(request.RequestId, request.AssetName,
		request.ChainInfoId, request.Alias)
	if err != nil {
		return nil, fmt.Errorf("failed to get product data set: %v", err)
	}
	// 处理请求，拿取数据
	reader := bytes.NewReader(request.ArrowBatch)
	// 使用 Arrow 的内存分配器
	pool := memory.NewGoAllocator()
	// 使用 IPC 文件读取器解析数据
	ipcReader, err := ipc.NewReader(reader, ipc.WithAllocator(pool))
	if err != nil {
		log.Fatalf("Failed to create Arrow IPC reader: %v", err)
	}
	defer ipcReader.Release()

	// 获取表结构信息
	schema := ipcReader.Schema()

	dbTypeName := utils.GetDbTypeName(connInfo.Dbtype)
	dbType := utils.ConvertDBType(dbTypeName)
	dbStrategy, err := database.DatabaseFactory(dbType, connInfo)

	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	db := database.GetDB(dbStrategy)
	tableName := request.TableName
	if tableName == "" {
		tableName = connInfo.TableName
	}
	log2.Logger.Infof("Operate table: %s", tableName)
	// 检查表是否存在，如果不存在就创建
	_ = dbStrategy.CreateTemporaryTableIfNotExists(tableName, schema)

	if err := database.InsertArrowDataInBatches(db, tableName, schema, ipcReader, dbType); err != nil {
		return nil, fmt.Errorf("failed to insert Arrow data: %v", err)
	}

	return &pb.Response{
		Success: true,
		Message: fmt.Sprintf("Data insert into successfully on Host:%s, Database:%s, Table:%s",
			connInfo.Host, connInfo.DbName, connInfo.TableName),
	}, nil
}

func (s Server) WriteInternalData(g grpc.ClientStreamingServer[pb.WriterInternalDataRequest, pb.Response]) error {
	conf := config.GetConfigMap()
	dbType := utils.ConvertDBType(conf.Dbms.Type)

	for {
		request, err := g.Recv()
		if err != nil {
			// 区分客户端主动断开连接和网络异常
			if err == io.EOF {
				log2.Logger.Infof("Client stream completed normally, sending final response")
				return g.SendAndClose(&pb.Response{
					Success: true,
					Message: "All data processed successfully",
				})
			}
			log2.Logger.Errorf("Error receiving request: %v", err)
			return g.SendAndClose(&pb.Response{
				Success: false,
				Message: fmt.Sprintf("Error processing stream: %v", err),
			})
		}
		connInfo := &pb.ConnectionInfo{Host: conf.Dbms.Host,
			Port:     conf.Dbms.Port,
			User:     conf.Dbms.User,
			DbName:   request.DbName,
			Password: conf.Dbms.Password,
		}
		log2.Logger.Debugf("Connect to database: %s", connInfo)
		// 处理请求，拿取数据
		reader := bytes.NewReader(request.ArrowBatch)
		// 使用 Arrow 的内存分配器
		pool := memory.NewGoAllocator()

		// 使用 IPC 文件读取器解析数据
		ipcReader, err := ipc.NewReader(reader, ipc.WithAllocator(pool))
		if err != nil {
			log.Fatalf("Failed to create Arrow IPC reader: %v", err)
		}
		// defer ipcReader.Release()

		// 获取表结构信息
		schema := ipcReader.Schema()
		log2.Logger.Infof("Table schema: %v", schema)
		dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
		if err != nil {
			ipcReader.Release() // 立即释放资源
			return fmt.Errorf("failed to create database strategy: %v", err)
		}
		if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
			ipcReader.Release() // 立即释放资源
			return fmt.Errorf("failed to connect to database: %v", err)
		}
		db := database.GetDB(dbStrategy)

		tableName := request.TableName
		if tableName == "" {
			tableName = connInfo.TableName
		}
		if request.JobInstanceId != "" {
			tableName = request.JobInstanceId + "_" + tableName
		}
		log2.Logger.Infof("Operate table: %s", tableName)

		// 创建结果通道
		resultCh := make(chan error, 1)

		// 创建表操作并提交到队列
		op := &database.TableOperation{
			TableName:  tableName,
			Schema:     schema,
			DbType:     dbType,
			DB:         db,
			IpcReader:  ipcReader,
			DbStrategy: dbStrategy,
			ResultCh:   resultCh,
		}

		queue := database.GetOperationQueue()
		queue <- op

		// 等待操作完成
		err = <-resultCh
		if err != nil {
			log2.Logger.Errorf("Failed to process table operation: %v", err)
			return fmt.Errorf("failed to process table operation: %v", err)
		}

	}

}

func (s Server) ReadInternalData(request *pb.InternalReadRequest, g grpc.ServerStreamingServer[pb.ArrowResponse]) error {
	conf := config.GetConfigMap()
	dbType := utils.ConvertDBType(conf.Dbms.Type)
	connInfo := &pb.ConnectionInfo{Host: conf.Dbms.Host,
		Port:     conf.Dbms.Port,
		User:     conf.Dbms.User,
		DbName:   request.DbName,
		Password: conf.Dbms.Password,
	}
	// 连接数据库
	dbStrategy, _ := database.DatabaseFactory(dbType, connInfo)
	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	tableName := request.TableName
	if request.JobInstanceId != "" {
		tableName = request.JobInstanceId + "_" + tableName
	}
	query, args, err := dbStrategy.BuildWithConditionQuery(tableName, request.DbFields,
		request.FilterNames, request.FilterOperators, request.FilterValues, request.SortRules)
	if err != nil {
		return fmt.Errorf("error building query: %v", err)
	}
	s.logger.Debugf("query: %s", query)
	s.logger.Debugf("args: %v", args)
	rows, err := dbStrategy.Query(query, args...)
	if err != nil {
		s.logger.Errorf("error executing query: %v", err)
		return fmt.Errorf("error executing query: %v", err)
	}
	if rows == nil {
		s.logger.Warn("Query returned nil rows")
		return fmt.Errorf("unexpected nil rows")
	}
	defer func() {
		s.logger.Debug("Closing rows...")
		rows.Close() // 函数结束时关闭游标
	}()
	s.logger.Debug("query database success")

	var sentData bool
	totalRecords := int64(0)
	// 在循环前计算批次大小
	adjustedBatchSize, err := database.CalculateEstimatedBatchSize(rows, common.STREAM_DATA_SIZE)
	if err != nil {
		return fmt.Errorf("failed to calculate estimated batch size: %v", err)
	}
	// 返回arrow流
	for {
		// 使用计算好的批次大小进行循环读取
		record, err := dbStrategy.RowsToArrowBatch(rows, adjustedBatchSize)
		if err == io.EOF {
			s.logger.Debug("All data read, sending EOF marker.")
			break
		}
		if err != nil {
			s.logger.Errorf("error receiving stream: %v", err)
			return fmt.Errorf("error reading Arrow batch: %v", err)
		}

		// 发送数据
		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %v", err)
		}

		response := &pb.ArrowResponse{
			ArrowBatch: buf.Bytes(),
		}
		if err := g.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %v", err)
		}

		totalRecords += record.NumRows()
		sentData = true
		record.Release()
	}
	s.logger.Debugf("query database success, total records: %d", totalRecords)

	if !sentData {
		// 查询到空集，返回空arrow
		buf, err := utils.ConvertToEmptyArrowBatch(connInfo.Columns)
		if err != nil {
			return fmt.Errorf("failed to convert to empty arrow batch: %v", err)
		}
		response := &pb.ArrowResponse{
			ArrowBatch: buf,
		}
		s.logger.Warn("Query returned nil rows")
		if err := g.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %v", err)
		}
		s.logger.Infof("Send empty arrow batch")
	}

	// 数据发送完成后，发送 EOF 标志
	eofResponse := &pb.ArrowResponse{
		ArrowBatch: []byte("EOF"), // 特殊标志，用于告知客户端结束
	}
	if err := g.Send(eofResponse); err != nil {
		return fmt.Errorf("failed to send EOF response: %v", err)
	}
	s.logger.Debug("send EOF response success")
	s.logger.Debug("Stream processing completed successfully")
	return nil
}

func (s Server) ReadStreamingData(request *pb.StreamReadRequest, g grpc.ServerStreamingServer[pb.ArrowResponse]) error {
	if request.GetRequestId() == "" {
		newUUID := uuid.New().String()
		request.RequestId = newUUID
		log2.Logger.Infow("Generated new UUID for request", "requestId", newUUID)
	}

	// 先通过 TableInfoService 拿到表的总条数
	tableInfo, err := s.tableInfoService.GetTableInfo(
		request.GetRequestId(),
		request.AssetName,
		request.ChainInfoId,
		true, // isExactQuery：这里按“精确”查询处理
		request.Alias,
	)
	if err != nil {
		s.logger.Errorf("failed to get table info: %v", err)
		return fmt.Errorf("failed to get table info: %v", err)
	}
	expectedTotal := int64(tableInfo.RecordCount)

	// 获取要连接的数据库信息
	connInfo, err := utils.GetDatasourceByAssetName(request.GetRequestId(), request.AssetName,
		request.ChainInfoId, request.Alias)
	if err != nil {
		log2.Logger.Errorf("Failed to get datasource by asset name: %v", err)
		return status.Error(codes.Internal, "Failed to get datasource by asset name")
	}
	if connInfo == nil {
		log2.Logger.Errorf("Failed to get valid datasource information")
		return status.Error(codes.Internal, "Failed to get valid datasource information")
	}

	dbType := utils.ConvertDataSourceType(connInfo.Dbtype)
	log2.Logger.Infof("Connecting to database with info: %+v", dbType)
	log2.Logger.Infof("Connecting to database with info: %+v", connInfo)

	// 从数据源中读取arrow数据流
	dbStrategy, _ := database.DatabaseFactory(dbType, connInfo)
	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		return fmt.Errorf("faild to connect to database: %v", err)
	}
	query, args, err := dbStrategy.BuildWithConditionQuery(connInfo.TableName, request.DbFields,
		request.FilterNames, request.FilterOperators, request.FilterValues, request.SortRules)
	if err != nil {
		return fmt.Errorf("error building query: %v", err)
	}
	rows, err := dbStrategy.Query(query, args...)
	if err != nil {
		s.logger.Errorf("error executing query: %v", err)
		return fmt.Errorf("error executing query: %v", err)
	}
	if rows == nil {
		s.logger.Warn("Query returned nil rows")
		return fmt.Errorf("unexpected nil rows")
	}
	defer func() {
		s.logger.Debug("Closing rows...")
		rows.Close() // 函数结束时关闭游标
	}()
	s.logger.Debug("query database success, request: asset=%s", request.AssetName)
	var sentData bool
	totalRecords := int64(0)
	// 在循环前计算批次大小
	adjustedBatchSize, err := database.CalculateEstimatedBatchSize(rows, common.STREAM_DATA_SIZE)
	if err != nil {
		return fmt.Errorf("failed to calculate estimated batch size: %v", err)
	}
	// 返回arrow流
	for {
		// 使用计算好的批次大小进行循环读取
		record, err := dbStrategy.RowsToArrowBatch(rows, adjustedBatchSize)
		if err == io.EOF {
			s.logger.Debug("All data read, sending EOF marker.")
			break
		}
		if err != nil {
			s.logger.Errorf("error receiving stream: %v", err)
			return fmt.Errorf("error reading Arrow batch: %v", err)
		}

		// 发送数据
		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %v", err)
		}

		response := &pb.ArrowResponse{
			ArrowBatch: buf.Bytes(),
		}
		if err := g.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %v", err)
		}

		totalRecords += record.NumRows()
		sentData = true
		record.Release()
	}
	s.logger.Infof("query database success, total records: %d, expected: %d", totalRecords, expectedTotal)

	var filterFlag = true
	if len(request.FilterNames) == 0 {
		s.logger.Debug("FilterNames is empty")
		filterFlag = false
	}
	// 如果不是过滤查询，则对比发送条数和表总条数，不一致则报错
	if !filterFlag && expectedTotal > 0 && totalRecords != expectedTotal {
		s.logger.Errorf("row count mismatch, expected %d, actually sent %d", expectedTotal, totalRecords)
		return fmt.Errorf("row count mismatch, expected %d, actually sent %d", expectedTotal, totalRecords)
	}

	if !sentData {
		// 查询到空集，返回空arrow
		buf, err := utils.ConvertToEmptyArrowBatch(connInfo.Columns)
		if err != nil {
			return fmt.Errorf("failed to convert to empty arrow batch: %v", err)
		}
		response := &pb.ArrowResponse{
			ArrowBatch: buf,
		}
		s.logger.Warn("Query returned nil rows")
		if err := g.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %v", err)
		}
		s.logger.Infof("Send empty arrow batch")
	}

	// 数据发送完成后，发送 EOF 标志
	eofResponse := &pb.ArrowResponse{
		ArrowBatch: []byte("EOF"), // 特殊标志，用于告知客户端结束
	}
	if err := g.Send(eofResponse); err != nil {
		return fmt.Errorf("failed to send EOF response: %v", err)
	}
	s.logger.Debug("send EOF response success")
	s.logger.Debug("Stream processing completed successfully")
	return nil
}

func (s Server) WriteOSSData(g grpc.ClientStreamingServer[pb.OSSWriteRequest, pb.Response]) error {
	var (
		bucketName string
		objectName string
		writer     *ipc.Writer
		schema     *arrow.Schema
		tmpFile    *os.File
	)

	// 统一循环接收所有包（包括第一个初始化包）
	first := true
	for {
		log2.Logger.Debugf("Waiting for next chunk...")
		req, err := g.Recv()
		if err == io.EOF {
			log2.Logger.Infof("Received EOF, breaking loop")
			break
		}
		if err != nil {
			log2.Logger.Errorf("Error receiving chunk: %v", err)
			return fmt.Errorf("failed to receive chunk: %v", err)
		}

		// 第一个包：初始化
		if first {
			log2.Logger.Infof("Processing first chunk")
			bucketName = req.GetBucketName()
			objectName = req.GetObjectName()
			log2.Logger.Infof("Bucket: %s, Object: %s", bucketName, objectName)

			// 创建临时文件
			tmpFile, err = os.CreateTemp(common.DATA_DIR, "arrow_stream_*.arrow")
			if err != nil {
				log2.Logger.Errorf("Failed to create temp file: %v", err)
				return fmt.Errorf("failed to create temp file: %v", err)
			}
			log2.Logger.Infof("Created temp file: %s", tmpFile.Name())

			// 反序列化首个 chunk 获取 schema
			firstChunk := req.GetChunk()
			if len(firstChunk) == 0 {
				log2.Logger.Errorf("First chunk is empty")
				return fmt.Errorf("first chunk is empty")
			}
			log2.Logger.Infof("First chunk size: %d bytes", len(firstChunk))

			reader, err := ipc.NewReader(bytes.NewReader(firstChunk))
			if err != nil {
				log2.Logger.Errorf("Failed to create arrow reader: %v", err)
				return fmt.Errorf("failed to create arrow reader: %v", err)
			}
			schema = reader.Schema()
			writer = ipc.NewWriter(tmpFile, ipc.WithSchema(schema))

			// 写入首个 chunk
			for reader.Next() {
				rec := reader.Record()
				if err := writer.Write(rec); err != nil {
					log2.Logger.Errorf("Failed to write record to arrow stream: %v", err)
					return fmt.Errorf("failed to write record to arrow stream: %v", err)
				}
			}
			reader.Release()
			first = false
			log2.Logger.Debug("First chunk processed successfully")
			continue
		}

		// 后续包：只处理 chunk
		log2.Logger.Infof("Processing subsequent chunk")
		chunk := req.GetChunk()
		if len(chunk) == 0 {
			log2.Logger.Warnf("Received empty chunk, skipping")
			continue
		}
		log2.Logger.Infof("Chunk size: %d bytes", len(chunk))

		reader, err := ipc.NewReader(bytes.NewReader(chunk))
		if err != nil {
			log2.Logger.Errorf("Failed to create arrow reader: %v", err)
			return fmt.Errorf("failed to create arrow reader: %v", err)
		}
		for reader.Next() {
			rec := reader.Record()
			if err := writer.Write(rec); err != nil {
				log2.Logger.Errorf("Failed to write record to arrow stream: %v", err)
				return fmt.Errorf("failed to write record to arrow stream: %v", err)
			}
		}
		reader.Release()
		log2.Logger.Infof("Chunk processed successfully")
	}

	// 关闭 writer 和文件
	log2.Logger.Infof("Closing writer and file")
	writer.Close()
	tmpFile.Close()

	// 上传临时文件到 OSS
	log2.Logger.Infof("Uploading file to OSS: %s/%s", bucketName, objectName)
	fileForUpload, err := os.Open(tmpFile.Name())
	if err != nil {
		log2.Logger.Errorf("Failed to open temp file for upload: %v", err)
		return fmt.Errorf("failed to open temp file for upload: %v", err)
	}
	defer fileForUpload.Close()

	stat, err := fileForUpload.Stat()
	if err != nil {
		log2.Logger.Errorf("Failed to stat temp file: %v", err)
		return fmt.Errorf("failed to stat temp file: %v", err)
	}

	_, err = s.ossClient.PutObject(context.Background(), bucketName, objectName, fileForUpload, stat.Size(), &oss.PutOptions{
		ContentType: "application/vnd.apache.arrow.stream",
	})
	if err != nil {
		log2.Logger.Errorf("Failed to upload object stream to MinIO: %v", err)
		return fmt.Errorf("failed to upload object stream to MinIO: %v", err)
	}

	log2.Logger.Infof("Successfully uploaded file to OSS")
	return g.SendAndClose(&pb.Response{
		Success: true,
		Message: fmt.Sprintf("File streamed to MinIO successfully: %s", objectName),
	})
}

func (s Server) WriteOSSFileData(g grpc.ClientStreamingServer[pb.OSSWriteRequest, pb.Response]) error {
	// 接收初始化请求，获取存储桶和对象名称
	req, err := g.Recv()
	if err == io.EOF {
		return fmt.Errorf("empty request")
	}
	if err != nil {
		return fmt.Errorf("failed to receive initial request: %v", err)
	}
	bucketName, objectName := req.GetBucketName(), req.GetObjectName()
	log2.Logger.Infof("Received request to write to OSS bucket %s, object %s", bucketName, objectName)
	// 创建临时文件
	tempFile, err := os.CreateTemp("", "tempfile-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())
	// 循环接收文件块并写入临时文件
	for {
		req, err := g.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive chunk: %v", err)
		}
		chunk := req.GetChunk()
		if _, err := tempFile.Write(chunk); err != nil {
			return fmt.Errorf("failed to write chunk to temporary file: %v", err)
		}
	}
	// 获取文件大小
	fileInfo, err := tempFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file size: %v", err)
	}
	fileSize := fileInfo.Size()
	// 重置文件指针到文件开头
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to the start of the file: %v", err)
	}
	// 将文件上传至 MinIO
	_, err = s.ossClient.PutObject(context.Background(), bucketName, objectName, tempFile, fileSize, &oss.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to MinIO: %v", err)
	}
	log2.Logger.Infof("File uploaded to MinIO successfully")
	// 返回成功响应
	return g.SendAndClose(&pb.Response{
		Success: true,
		Message: fmt.Sprintf("File uploaded to MinIO successfully"),
	})
}

/**
 * @Description 从OSS中读取数据并通过gRPC进行流式传输。只读取一个object，客户端可以根据情况选择并发使用此接口
 * @Param request (*pb.OSSReadRequest): 包含bucket和object的gRPC请求对象。
 * @Param g (grpc.ServerStreamingServer[pb.OSSReadResponse]): 用于发送响应的gRPC流。
 * @return  error: 如果操作成功则返回nil，否则返回错误信息。
 **/
func (s Server) ReadOSSData(request *pb.OSSReadRequest, g grpc.ServerStreamingServer[pb.OSSReadResponse]) error {
	bucketName, objectName := request.GetBucketName(), request.GetObjectName()
	log2.Logger.Infof("Received request to read from OSS bucket %s, object %s", bucketName, objectName)

	tempFile, err := os.CreateTemp("", "minio_object_*.arrow")
	if err != nil {
		log2.Logger.Errorf("Failed to create temporary file: %v", err)
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	log2.Logger.Infof("Creating temporary file: %s", tempFile.Name())
	defer os.Remove(tempFile.Name())

	object, err := s.ossClient.GetObject(context.Background(), bucketName, objectName, &oss.GetOptions{})
	if err != nil {
		log2.Logger.Errorf("Failed to get object from MinIO: %v", err)
		return fmt.Errorf("failed to get object from MinIO: %v", err)
	}
	log2.Logger.Infof("Getting object from MinIO: %s", objectName)
	defer object.Close()

	if _, err := io.Copy(tempFile, object); err != nil {
		log2.Logger.Errorf("Failed to write object to temporary file: %v", err)
		return fmt.Errorf("failed to write object to temporary file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %v", err)
	}
	log2.Logger.Infof("Downloaded file saved to temporary path: %s", tempFile.Name())

	file, err := os.Open(tempFile.Name())
	log2.Logger.Infof("Opening downloaded file: %s", tempFile.Name())
	if err != nil {
		log2.Logger.Errorf("Failed to open downloaded file: %v", err)
		return fmt.Errorf("failed to open downloaded file: %v", err)
	}
	defer file.Close()

	pool := memory.NewGoAllocator()
	ipcReader, err := ipc.NewFileReader(file, ipc.WithAllocator(pool))
	log2.Logger.Infof("Creating Arrow IPC reader")
	if err != nil {
		log2.Logger.Errorf("Failed to create Arrow IPC reader: %v", err)
		return fmt.Errorf("failed to create Arrow IPC reader: %v", err)
	}
	defer ipcReader.Close()

	buffer := &bytes.Buffer{}
	writer := ipc.NewWriter(buffer, ipc.WithSchema(ipcReader.Schema()))
	defer writer.Close()
	log2.Logger.Infof("Buffer content: %v", buffer.Bytes())
	for i := 0; i < ipcReader.NumRecords(); i++ {
		log2.Logger.Infof("Processing record batch %d", i)
		record, err := ipcReader.Record(i)
		// utils.PrintRecord(record)
		if err != nil {
			log2.Logger.Errorf("Failed to read record batch %d: %v", i, err)
			return fmt.Errorf("failed to read record batch %d: %v", i, err)
		}
		if record == nil || record.NumRows() == 0 {
			continue
		}

		if err := writer.Write(record); err != nil {
			log2.Logger.Errorf("Failed to write record to Arrow IPC stream: %v", err)
			return fmt.Errorf("failed to write record to Arrow IPC stream: %v", err)
		}
		if buffer.Len() >= common.MAX_CHUNK_SIZE {
			log2.Logger.Infof("Sending chunk (buffer size: %d)", buffer.Len())
			if err := g.Send(&pb.OSSReadResponse{Chunk: buffer.Bytes()}); err != nil {
				return fmt.Errorf("failed to send chunk: %v", err)
			}
			log2.Logger.Infof("sending buffer success")
			buffer.Reset()
		}
	}

	if buffer.Len() > 0 {
		log2.Logger.Infof("Sending remaining chunk (buffer size: %d)", buffer.Len())
		if err := g.Send(&pb.OSSReadResponse{Chunk: buffer.Bytes()}); err != nil {
			return fmt.Errorf("failed to send remaining chunk: %v", err)
		}
		log2.Logger.Infof("sending buffer success")
	}

	return g.Send(&pb.OSSReadResponse{
		Success: true,
		Message: fmt.Sprintf("File read from MinIO and streamed successfully: %s", objectName),
	})
}

func (s Server) GetTableInfo(ctx context.Context, request *pb.TableInfoRequest) (*pb.TableInfoResponse, error) {
	return s.tableInfoService.GetTableInfo(request.RequestId, request.AssetName, request.ChainInfoId, request.IsExactQuery, request.Alias)
}

func (s Server) GetInternalTableInfo(ctx context.Context, request *pb.InternalTableInfoRequest) (*pb.TableInfoResponse, error) {
	tableName := request.TableName
	// if request.JobInstanceId != "" {
	// 	tableName = request.JobInstanceId + "_" + tableName
	// }
	return s.tableInfoService.GetInternalTableInfo(tableName, request.DbName, request.IsExactQuery)
}

func (s Server) GetGroupCountInfo(ctx context.Context, request *pb.GroupCountRequest) (*pb.GroupCountResponse, error) {
	conf := config.GetConfigMap()
	dbType := utils.ConvertDBType(conf.Dbms.Type)
	connInfo := &pb.ConnectionInfo{Host: conf.Dbms.Host,
		Port:     conf.Dbms.Port,
		User:     conf.Dbms.User,
		DbName:   request.DbName,
		Password: conf.Dbms.Password,
	}
	log2.Logger.Debugf("Connect to database: %s", connInfo)

	dbStrategy, _ := database.DatabaseFactory(dbType, connInfo)
	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	return dbStrategy.GetGroupCountInfo(request.TableName, request.GroupByFields, request.FilterNames, request.FilterOperators, request.FilterValues)
}

func (s Server) TruncateTable(ctx context.Context, request *pb.TruncateTableRequest) (*pb.TruncateTableResponse, error) {
	conf := config.GetConfigMap()
	dbType := utils.ConvertDBType(conf.Dbms.Type)

	connInfo := &pb.ConnectionInfo{Host: conf.Dbms.Host,
		Port:     conf.Dbms.Port,
		User:     conf.Dbms.User,
		DbName:   common.MPC_TEMP_DB_NAME,
		Password: conf.Dbms.Password,
	}

	dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create database strategy: %v", err)
	}

	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	crudService := service.NewCrudService(log2.Logger, dbStrategy)

	return crudService.TruncateTable(request.TableName)
}

func (s Server) PushJobResultToExternalDB(ctx context.Context, request *pb.PushJobResultRequest) (*pb.PushJobResultResponse, error) {
	// 打印请求参数
	log2.Logger.Infof("PushJobResultToExternalDB called with parameters:")
	log2.Logger.Infof("  JobInstanceId: %s", request.JobInstanceId)
	log2.Logger.Infof("  ChainInfoId: %s", request.ChainInfoId)
	log2.Logger.Infof("  PartyId: %s", request.PartyId)
	log2.Logger.Infof("  DataId: %s", request.DataId)
	log2.Logger.Infof("  IsEncrypted: %t", request.IsEncrypted)
	return service.NewJobResultService().PushJobResultToExternalDB(ctx, request)
}

func (s Server) ExecuteDorisSQL(ctx context.Context, request *pb.ExecuteDorisSQLRequest) (*pb.ExecuteDorisSQLResponse, error) {
	dorisService, err := service.NewDorisService(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return nil, fmt.Errorf("failed to create doris service: %v", err)
	}

	// 将[]string转换为[]interface{}
	var args []interface{}
	for _, arg := range request.Args {
		args = append(args, arg)
	}

	rows, done, err := dorisService.ExecuteSQL(request.Sql, args...)
	if rows == nil && err == nil {
		return &pb.ExecuteDorisSQLResponse{
			Success: true,
			Message: "Query executed successfully, but no rows returned",
			Rows:    nil,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute doris sql: %v", err)
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	columns, _ := rows.Columns()
	var resultRows []*pb.DorisSQLRow
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		rowMap := make(map[string]string)
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else if val != nil {
				rowMap[col] = fmt.Sprintf("%v", val)
			} else {
				rowMap[col] = ""
			}
		}
		resultRows = append(resultRows, &pb.DorisSQLRow{Columns: rowMap})
	}
	return &pb.ExecuteDorisSQLResponse{
		Success: true,
		Message: "Query executed successfully",
		Rows:    resultRows,
	}, nil
}

func (s Server) ImportCsvFileToDoris(ctx context.Context, request *pb.ImportCsvFileToDorisRequest) (*pb.Response, error) {
	dorisService, err := service.NewDorisService(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return nil, fmt.Errorf("failed to create doris service: %v", err)
	}
	err = dorisService.ImportCsvFileToDoris(request)
	if err != nil {
		return nil, fmt.Errorf("failed to import csv file to doris: %v", err)
	}
	return &pb.Response{
		Success: true,
		Message: "CSV file imported to Doris successfully",
	}, nil
}

func (s Server) ExportCsvFileFromDoris(ctx context.Context, request *pb.ExportCsvFileFromDorisRequest) (*pb.ExportCsvFileFromDorisResponse, error) {
	dorisService, err := service.NewDorisService(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return nil, fmt.Errorf("failed to create doris service: %v", err)
	}
	err = dorisService.ExportParquetFileFromDoris(request)
	if err != nil {
		return nil, fmt.Errorf("failed to export parquet file from doris: %v", err)
	}
	return &pb.ExportCsvFileFromDorisResponse{
		BucketName:    common.BATCH_DATA_BUCKET_NAME,
		JobInstanceId: request.JobInstanceId,
	}, nil
}

func (s Server) ExportDorisDataToMiraDB(ctx context.Context, request *pb.ExportDorisDataToMiraDBRequest) (*pb.Response, error) {
	dorisService, err := service.NewDorisService(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return nil, fmt.Errorf("failed to create doris service: %v", err)
	}
	err = dorisService.ExportDorisDataToMiraDB(request)
	if err != nil {
		return nil, fmt.Errorf("failed to export doris data to mira db: %v", err)
	}
	return &pb.Response{
		Success: true,
		Message: "Doris data exported to MiraDB successfully",
	}, nil
}

func (s Server) ImportMiraDBDataToDoris(ctx context.Context, request *pb.ImportMiraDBDataToDorisRequest) (*pb.ImportMiraDBDataToDorisResponse, error) {
	dorisService, err := service.NewDorisService(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return nil, fmt.Errorf("failed to create doris service: %v", err)
	}
	internalTableName, err := dorisService.ImportMiraDBDataToDoris(request)
	if err != nil {
		return nil, fmt.Errorf("failed to import mira db data to doris: %v", err)
	}
	return &pb.ImportMiraDBDataToDorisResponse{
		DorisTableName: internalTableName,
	}, nil
}

func (s Server) CleanTmpData(ctx context.Context, request *pb.CleanTmpDataRequest) (*pb.Response, error) {
	cleanupService := service.NewCleanupTaskService()
	err := cleanupService.ExecuteCleanupTask(request.JobInstanceId)
	if err != nil {
		return nil, fmt.Errorf("failed to clean doris tmp data: %v", err)
	}

	return &pb.Response{
		Success: true,
		Message: "Doris temporary data cleaned successfully",
	}, nil
}

func (s Server) GetRetryCleanupTask(ctx context.Context, request *pb.GetRetryCleanupTasksRequest) (*pb.GetRetryCleanupTasksResponse, error) {
	cleanupService := service.NewCleanupTaskService()
	// 获取分页参数
	page := int(request.Page)
	if page <= 0 {
		page = 1
	}
	pageSize := int(request.PageSize)
	if pageSize <= 0 {
		pageSize = 10
	}

	tasks, totalCount, err := cleanupService.GetRetryCleanupTasks(page, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get retry cleanup tasks: %v", err)
	}

	// 转换为proto消息
	var taskInfos []*pb.CleanupTaskInfo
	for _, task := range tasks {
		taskInfo := &pb.CleanupTaskInfo{
			Id:            uint32(task.ID),
			JobInstanceId: task.JobInstanceID,
			Status:        string(task.Status),
		}
		taskInfos = append(taskInfos, taskInfo)
	}

	return &pb.GetRetryCleanupTasksResponse{
		Tasks:      taskInfos,
		TotalCount: int32(totalCount),
		Page:       int32(page),
		PageSize:   int32(pageSize),
	}, nil
}

func (s Server) mustEmbedUnimplementedDataSourceServiceServer() {
	//TODO implement me
	panic("implement me")
}

func (s Server) SubmitBatchJob(ctx context.Context, request *pb.BatchReadRequest) (*pb.BatchResponse, error) {
	return s.k8sService.SubmitBatchJob(ctx, request)
}

func (s Server) GetJobStatus(ctx context.Context, request *pb.JobStatusRequest) (*pb.BatchResponse, error) {
	return s.k8sService.GetJobStatus(ctx, request)
}

func (s Server) ReadDataSourceStreaming(request *pb.ReadDataSourceStreamingRequest, g grpc.ServerStreamingServer[pb.ArrowResponse]) error {
	dorisService, err := service.NewDorisService(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return fmt.Errorf("failed to create doris service: %v", err)
	}

	var tableName string
	randomSuffix, err := common.GenerateRandomString(8)
	if err != nil {
		return fmt.Errorf("failed to generate random suffix: %v", err)
	}
	enhancedJobInstanceId := request.JobInstanceId + "_" + randomSuffix
	// 1.从数据源拉取数据到doris并导出到minio
	tableName, err = dorisService.ProcessDataSourceAndExport(request, enhancedJobInstanceId)
	if err != nil {
		return fmt.Errorf("failed to process data source and export: %v", err)
	}

	// 2.执行导出parquet文件到minio
	// 3.从minio流式读取arrow文件
	chunkService := service.NewChunkService(s.ossClient)
	parquetStreamingService := service.NewParquetStreamingService(chunkService, s.ossClient)

	if err := parquetStreamingService.StreamParquetFileFromOSS(tableName, enhancedJobInstanceId, request.Columns, g); err != nil {
		return fmt.Errorf("failed to stream parquet file from OSS: %v", err)
	}

	// 清理导出文件
	err = s.ossClient.DeleteObjectsByJobInstanceId(context.Background(), common.BATCH_DATA_BUCKET_NAME, enhancedJobInstanceId)
	if err != nil {
		log2.Logger.Errorf("failed to delete objects by job instance id: %v", err)
	}

	// 创建清理任务，用于后续清理 Doris 临时数据
	cleanupService := service.NewCleanupTaskService()
	cleanupTask, err := cleanupService.GetOrCreateCleanupTask(enhancedJobInstanceId, "doris_table")
	if err != nil {
		log2.Logger.Warnf("Failed to create cleanup task for job %s: %v", enhancedJobInstanceId, err)
		// 不阻断主流程，只记录警告日志
	} else {
		log2.Logger.Infof("Cleanup task created/retrieved for job %s, task ID: %d", enhancedJobInstanceId, cleanupTask.ID)
	}

	return nil
}

func (s Server) ExecuteSql(request *pb.ExecuteSqlRequest, g grpc.ServerStreamingServer[pb.ExecuteSqlResponse]) error {
	// 创建SQL执行服务
	sqlService, err := service.NewSqlExecutionService(request.DbName)
	if err != nil {
		return fmt.Errorf("failed to create sql execution service: %v", err)
	}

	// 如果有目标表名，将查询结果写入到目标表
	if request.TargetTableName != "" {
		return sqlService.ExecuteSqlWithTableOutput(request.Sql, request.TargetTableName, g)
	}

	// 否则执行普通的流式SQL查询
	return sqlService.ExecuteStreamingSql(request.Sql, g)

}

func (s Server) Read(request *pb.ReadRequest, g grpc.ServerStreamingServer[pb.ArrowResponse]) error {
	readService, err := service.NewReadService(s.ossClient)
	if err != nil {
		g.SetTrailer(metadata.Pairs("x-error-code", strconv.Itoa(common.ErrCodeInternalError)))
		return status.Error(codes.Internal, fmt.Sprintf("failed to create read service: %v", err))
	}

	if err := readService.ProcessReadRequest(request, g); err != nil {
		code := common.ErrCodeUnknown
		if c, ok := common.GetErrorCode(err); ok {
			code = c
		}
		g.SetTrailer(metadata.Pairs("x-error-code", strconv.Itoa(code)))

		switch code {
		case common.ErrCodeOSSStreamReadFailed:
			st := status.New(codes.Unavailable, err.Error())
			// 添加业务错误码到details
			errorInfo := &errdetails.ErrorInfo{
				Reason: "OSS_STREAM_READ_FAILED",
				Metadata: map[string]string{
					"error_code": strconv.Itoa(code),
					"error_msg":  common.ErrCodeMessage[code],
				},
			}
			if any, err := anypb.New(errorInfo); err == nil {
				st, _ = st.WithDetails(any)
			}
			g.SetTrailer(metadata.Pairs("x-error-code", strconv.Itoa(code)))
			return st.Err()
		default:
			return status.Error(codes.Internal, err.Error())
		}
	}

	return nil
}

func (s Server) Write(stream grpc.ClientStreamingServer[pb.WriteRequest, pb.WriteResponse]) error {
	// 创建数据写入服务
	writeService, err := service.NewDataWriteService()
	if err != nil {
		return fmt.Errorf("failed to create data write service: %v", err)
	}

	// 处理写入流
	return writeService.ProcessWriteStream(stream)
}

func (s Server) ImportData(ctx context.Context, request *pb.ImportDataRequest) (*pb.ImportDataResponse, error) {
	start := time.Now()
	importService := service.NewImportService()
	response, err := importService.ImportData(ctx, request)
	ObserveDuration("ImportData", start, err == nil)
	return response, err
}

func main() {
	// 程序退出时关闭 IDA 服务
	defer utils.CloseIDAService()

	// 启动 pprof 服务器
	go func() {
		log2.Logger.Info(http.ListenAndServe("localhost:6060", nil))
	}()

	// 创建初始化器
	init := &Initializer{}
	// 执行初始化
	if err := init.Init(); err != nil {
		log2.Logger.Fatalf("Failed to initialize: %v", err)
	}

	addr := ":" + fmt.Sprintf("%d", init.config.CommonConfig.Port)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log2.Logger.Fatalf("failed to listen: %v", err)
	}

	// 创建 gRPC 服务器选项，设置消息大小限制
	grpcOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(common.GRPC_TRANSFER_SIZE),
		grpc.MaxSendMsgSize(common.GRPC_TRANSFER_SIZE),
	}

	grpcServer := grpc.NewServer(grpcOptions...)
	dataService := &Server{
		logger:           log2.Logger,
		ossClient:        init.ossClient,
		k8sService:       init.k8sService,
		tableInfoService: init.tableInfoService,
	}
	// 注册服务
	pb.RegisterDataSourceServiceServer(grpcServer, dataService)

	// 创建 gRPC-Gateway mux
	gwmux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}

	// 注册 gRPC-Gateway 处理器
	if err := pb.RegisterDataSourceServiceHandlerFromEndpoint(
		context.Background(),
		gwmux,
		addr, // gRPC 服务器地址
		opts,
	); err != nil {
		log2.Logger.Fatalf("Failed to register gRPC-Gateway handler: %v", err)
	}

	log2.Logger.Infof("gRPC server running at %v", listen.Addr())
	go func() {
		if err := grpcServer.Serve(listen); err != nil {
			log2.Logger.Fatalf("failed to serve gRPC: %v", err)
		}
	}()

	// 启动定时任务
	Schedule(init.config)

	// 启动监控服务
	MonitorMetric()

	// 启动 HTTP 服务器（支持 REST API）
	httpMux := http.NewServeMux()
	httpMux.Handle("/v1/", gwmux) // grpc-gateway 路由

	// 注册现有的 HTTP 路由
	routes.RegisterRoutes()

	log2.Logger.Infof("HTTP server running at %s", init.config.HttpServiceConfig.Port)
	if err = http.ListenAndServe(":"+fmt.Sprintf("%d", init.config.HttpServiceConfig.Port), httpMux); err != nil {
		log2.Logger.Fatalf("failed to serve: %v", err)
	}

}
