package clients

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	
	// 需要先编译proto文件生成go代码
	// pb "data-integrate-test/proto/mirapb"
)

// IDAServiceClient IDA服务客户端
type IDAServiceClient struct {
	conn   *grpc.ClientConn
	// client pb.MiraIdaAccessClient
}

func NewIDAServiceClient(host string, port int) (*IDAServiceClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	
	return &IDAServiceClient{
		conn: conn,
		// client: pb.NewMiraIdaAccessClient(conn),
	}, nil
}

func (c *IDAServiceClient) Close() error {
	return c.conn.Close()
}

// CreateDataSource 创建数据源
func (c *IDAServiceClient) CreateDataSource(ctx context.Context, req interface{}) (interface{}, error) {
	// TODO: 实现CreateDataSource调用
	// return c.client.CreateDataSource(ctx, req)
	return nil, fmt.Errorf("not implemented: need proto compilation")
}

// CreateAsset 创建资产
func (c *IDAServiceClient) CreateAsset(ctx context.Context, req interface{}) (interface{}, error) {
	// TODO: 实现CreateAsset调用
	// return c.client.CreateAsset(ctx, req)
	return nil, fmt.Errorf("not implemented: need proto compilation")
}

// GetPrivateAssetInfoByEnName 获取资产信息
func (c *IDAServiceClient) GetPrivateAssetInfoByEnName(ctx context.Context, assetEnName string) (interface{}, error) {
	// TODO: 实现GetPrivateAssetInfoByEnName调用
	// req := &pb.GetPrivateAssetInfoByEnNameRequest{
	//     BaseRequest: &pb.BaseRequest{
	//         RequestId: fmt.Sprintf("test_%d", time.Now().Unix()),
	//     },
	//     AssetEnName: assetEnName,
	// }
	// return c.client.GetPrivateAssetInfoByEnName(ctx, req)
	return nil, fmt.Errorf("not implemented: need proto compilation")
}

