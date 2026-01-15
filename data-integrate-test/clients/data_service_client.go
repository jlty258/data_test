package clients

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	
	// 需要先编译proto文件生成go代码
	// pb "data-integrate-test/proto/datasource"
)

// DataServiceClient Data Service客户端
type DataServiceClient struct {
	conn   *grpc.ClientConn
	// client pb.DataSourceServiceClient
}

func NewDataServiceClient(host string, port int) (*DataServiceClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	
	return &DataServiceClient{
		conn: conn,
		// client: pb.NewDataSourceServiceClient(conn),
	}, nil
}

func (c *DataServiceClient) Close() error {
	return c.conn.Close()
}

// ReadStreamingData 流式读取数据
func (c *DataServiceClient) ReadStreamingData(ctx context.Context, req interface{}) ([]interface{}, error) {
	// TODO: 实现ReadStreamingData调用
	// stream, err := c.client.ReadStreamingData(ctx, req)
	// if err != nil {
	//     return nil, err
	// }
	// 
	// var responses []*pb.ArrowResponse
	// for {
	//     resp, err := stream.Recv()
	//     if err == io.EOF {
	//         break
	//     }
	//     if err != nil {
	//         return nil, err
	//     }
	//     responses = append(responses, resp)
	// }
	// return responses, nil
	return nil, fmt.Errorf("not implemented: need proto compilation")
}

// WriteInternalData 写入内部数据
func (c *DataServiceClient) WriteInternalData(ctx context.Context, data []byte) error {
	// TODO: 实现WriteInternalData调用
	return fmt.Errorf("not implemented: need proto compilation")
}

