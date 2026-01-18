package clients

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// 使用生成的proto代码
	pb "data-integrate-test/generated/datasource"
)

// DataServiceClient Data Service客户端
type DataServiceClient struct {
	conn   *grpc.ClientConn
	client pb.DataSourceServiceClient
}

// NewDataServiceClient 创建Data Service客户端
func NewDataServiceClient(host string, port int) (*DataServiceClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("连接Data Service失败: %v", err)
	}

	return &DataServiceClient{
		conn:   conn,
		client: pb.NewDataSourceServiceClient(conn),
	}, nil
}

// Close 关闭连接
func (c *DataServiceClient) Close() error {
	return c.conn.Close()
}

// StreamReadRequest 流式读取请求参数
type StreamReadRequest struct {
	AssetName       string
	ChainInfoId     string
	DbFields        []string
	RequestId       string
	FileType        pb.FileType
	FilePath        string
	FilterNames     []string
	FilterValues    []*pb.FilterValue
	SortRules       []*pb.SortRule
	FilterOperators []pb.FilterOperator
	Alias           string
}

// ReadStreamingData 流式读取数据
func (c *DataServiceClient) ReadStreamingData(ctx context.Context, req *StreamReadRequest) ([]*pb.ArrowResponse, error) {
	// 构建proto请求
	protoReq := &pb.StreamReadRequest{
		AssetName:       req.AssetName,
		ChainInfoId:     req.ChainInfoId,
		DbFields:        req.DbFields,
		RequestId:       req.RequestId,
		FileType:        req.FileType,
		FilePath:        req.FilePath,
		FilterNames:     req.FilterNames,
		FilterValues:    req.FilterValues,
		SortRules:       req.SortRules,
		FilterOperators: req.FilterOperators,
		Alias:           req.Alias,
	}

	// 如果没有提供RequestId，生成一个
	if protoReq.RequestId == "" {
		protoReq.RequestId = fmt.Sprintf("read_%d", time.Now().UnixNano())
	}

	// 调用gRPC流式接口
	stream, err := c.client.ReadStreamingData(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}

	// 接收所有流式响应
	var responses []*pb.ArrowResponse
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			// 流结束
			break
		}
		if err != nil {
			return nil, fmt.Errorf("接收流式数据失败: %v", err)
		}
		responses = append(responses, resp)
	}

	return responses, nil
}

// WriteInternalDataRequest 写入内部数据请求参数
type WriteInternalDataRequest struct {
	ArrowBatch    []byte
	DbName        string
	TableName     string
	JobInstanceId string
}

// WriteInternalData 写入内部数据（流式写入）
func (c *DataServiceClient) WriteInternalData(ctx context.Context, req *WriteInternalDataRequest) error {
	// 创建流式客户端
	stream, err := c.client.WriteInternalData(ctx)
	if err != nil {
		return fmt.Errorf("创建写入流失败: %v", err)
	}

	// 构建并发送请求
	protoReq := &pb.WriterInternalDataRequest{
		ArrowBatch:    req.ArrowBatch,
		DbName:        req.DbName,
		TableName:     req.TableName,
		JobInstanceId: req.JobInstanceId,
	}

	if err := stream.Send(protoReq); err != nil {
		return fmt.Errorf("发送数据失败: %v", err)
	}

	// 关闭发送并接收响应
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("接收响应失败: %v", err)
	}

	// 检查响应
	if !resp.Success {
		return fmt.Errorf("写入失败: %s", resp.Message)
	}

	return nil
}

// WriteInternalDataBatch 批量写入内部数据（支持多个批次）
func (c *DataServiceClient) WriteInternalDataBatch(ctx context.Context, batches []*WriteInternalDataRequest) error {
	// 创建流式客户端
	stream, err := c.client.WriteInternalData(ctx)
	if err != nil {
		return fmt.Errorf("创建写入流失败: %v", err)
	}

	// 发送所有批次
	for i, batch := range batches {
		protoReq := &pb.WriterInternalDataRequest{
			ArrowBatch:    batch.ArrowBatch,
			DbName:        batch.DbName,
			TableName:     batch.TableName,
			JobInstanceId: batch.JobInstanceId,
		}

		if err := stream.Send(protoReq); err != nil {
			return fmt.Errorf("发送第%d批次失败: %v", i+1, err)
		}
	}

	// 关闭发送并接收响应
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("接收响应失败: %v", err)
	}

	// 检查响应
	if !resp.Success {
		return fmt.Errorf("批量写入失败: %s", resp.Message)
	}

	return nil
}

// ReadInternalData 从内置数据库读取数据
func (c *DataServiceClient) ReadInternalData(ctx context.Context, req *pb.InternalReadRequest) ([]*pb.ArrowResponse, error) {
	// 调用gRPC流式接口
	stream, err := c.client.ReadInternalData(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}

	// 接收所有流式响应
	var responses []*pb.ArrowResponse
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			// 流结束
			break
		}
		if err != nil {
			return nil, fmt.Errorf("接收流式数据失败: %v", err)
		}
		responses = append(responses, resp)
	}

	return responses, nil
}

// GetTableInfo 获取数据源表信息
func (c *DataServiceClient) GetTableInfo(ctx context.Context, req *pb.TableInfoRequest) (*pb.TableInfoResponse, error) {
	resp, err := c.client.GetTableInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}
	return resp, nil
}

// Read 新版通用读接口
func (c *DataServiceClient) Read(ctx context.Context, req *pb.ReadRequest) ([]*pb.ArrowResponse, error) {
	// 调用gRPC流式接口
	stream, err := c.client.Read(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}

	// 接收所有流式响应
	var responses []*pb.ArrowResponse
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			// 流结束
			break
		}
		if err != nil {
			return nil, fmt.Errorf("接收流式数据失败: %v", err)
		}
		responses = append(responses, resp)
	}

	return responses, nil
}

// Write 新版通用写接口（流式写入）
func (c *DataServiceClient) Write(ctx context.Context, batches []*pb.WriteRequest) (*pb.WriteResponse, error) {
	// 创建流式客户端
	stream, err := c.client.Write(ctx)
	if err != nil {
		return nil, fmt.Errorf("创建写入流失败: %v", err)
	}

	// 发送所有批次
	for i, batch := range batches {
		if err := stream.Send(batch); err != nil {
			return nil, fmt.Errorf("发送第%d批次失败: %v", i+1, err)
		}
	}

	// 关闭发送并接收响应
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("接收响应失败: %v", err)
	}

	return resp, nil
}

