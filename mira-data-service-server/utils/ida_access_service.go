package utils

import (
	"data-service/common"
	"data-service/log"
	"os"
	"sync"

	pb "chainweaver.org.cn/chainweaver/mira/mira-ida-access-service/pb/mirapb"
	"google.golang.org/grpc"
)

// IDAService IDA 服务单例
type IDAService struct {
	Client pb.MiraIdaAccessClient
	conn   *grpc.ClientConn
}

var (
	idaService *IDAService
	idaOnce    sync.Once
)

// GetIDAService 获取 IDA 服务单例
func GetIDAService() *IDAService {
	idaOnce.Do(func() {
		IDAServerAddress := os.Getenv("IDA_MANAGE_HOST")
		IDAServerPort := os.Getenv("IDA_MANAGE_PORT")
		idaAddress := IDAServerAddress + ":" + IDAServerPort

		// 添加gRPC选项，增加消息大小限制
		opts := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(common.GRPC_TRANSFER_SIZE),
				grpc.MaxCallSendMsgSize(common.GRPC_TRANSFER_SIZE),
			),
		}

		conn, err := grpc.Dial(idaAddress, opts...)
		if err != nil {
			log.Logger.Errorf("Failed to connect to IDA service: %v", err)
			return
		}

		idaService = &IDAService{
			Client: pb.NewMiraIdaAccessClient(conn),
			conn:   conn,
		}

		log.Logger.Infof("IDA service initialized at: %s", idaAddress)
	})

	return idaService
}

// Close 关闭 IDA 服务连接
func (s *IDAService) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// CloseIDAService 关闭 IDA 服务单例
func CloseIDAService() error {
	if idaService != nil {
		return idaService.Close()
	}
	return nil
}
