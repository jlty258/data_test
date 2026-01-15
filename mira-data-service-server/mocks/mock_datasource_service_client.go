/*
*

	@author: shiliang
	@date: 2024/12/23
	@note:

*
*/
package mocks

import (
	"context"
	pb "data-service/generated/datasource"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockDataSourceServiceClient struct {
	mock.Mock
}

// ReadBatchData 模拟 ReadBatchData 方法
func (m *MockDataSourceServiceClient) ReadBatchData(ctx context.Context, in *pb.BatchReadRequest, opts ...grpc.CallOption) (*pb.BatchResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.BatchResponse), args.Error(1)
}

// ReadStreamingData 模拟 ReadStreamingData 方法
func (m *MockDataSourceServiceClient) ReadStreamingData(ctx context.Context, in *pb.StreamReadRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ArrowResponse], error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(grpc.ServerStreamingClient[pb.ArrowResponse]), args.Error(1)
}

// SendArrowData 模拟 SendArrowData 方法
func (m *MockDataSourceServiceClient) SendArrowData(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[pb.WrappedWriterDataRequest, pb.Response], error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(grpc.ClientStreamingClient[pb.WrappedWriterDataRequest, pb.Response]), args.Error(1)
}

// WriteOSSData 模拟 WriteOSSData 方法
func (m *MockDataSourceServiceClient) WriteOSSData(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[pb.OSSWriteRequest, pb.Response], error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(grpc.ClientStreamingClient[pb.OSSWriteRequest, pb.Response]), args.Error(1)
}

// ReadOSSData 模拟 ReadOSSData 方法
func (m *MockDataSourceServiceClient) ReadOSSData(ctx context.Context, in *pb.OSSReadRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ArrowResponse], error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(grpc.ServerStreamingClient[pb.ArrowResponse]), args.Error(1)
}

// WriteInternalData 模拟 WriteInternalData 方法
func (m *MockDataSourceServiceClient) WriteInternalData(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[pb.WriterInternalDataRequest, pb.Response], error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(grpc.ClientStreamingClient[pb.WriterInternalDataRequest, pb.Response]), args.Error(1)
}

// ReadInternalData 模拟 ReadInternalData 方法
func (m *MockDataSourceServiceClient) ReadInternalData(ctx context.Context, in *pb.InternalReadRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ArrowResponse], error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(grpc.ServerStreamingClient[pb.ArrowResponse]), args.Error(1)
}

// WriterExternalData 模拟 WriterExternalData 方法
func (m *MockDataSourceServiceClient) WriterExternalData(ctx context.Context, in *pb.WriterExternalDataRequest, opts ...grpc.CallOption) (*pb.Response, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.Response), args.Error(1)
}

// GetTableInfo 模拟 GetTableInfo 方法
func (m *MockDataSourceServiceClient) GetTableInfo(ctx context.Context, in *pb.TableInfoRequest, opts ...grpc.CallOption) (*pb.TableInfoResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.TableInfoResponse), args.Error(1)
}
