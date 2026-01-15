package main

import (
	"context"
	"log"
	"net"
	"os"

	pb "ida-access-service-mock/proto/mirapb"
	"google.golang.org/grpc"
)

// server 实现 MiraIdaAccess 服务
type server struct {
	pb.UnimplementedMiraIdaAccessServer
}

// GetPrivateDBConnInfo 获取数据库连接信息
func (s *server) GetPrivateDBConnInfo(ctx context.Context, req *pb.GetPrivateDBConnInfoRequest) (*pb.GetPrivateDBConnInfoResponse, error) {
	log.Printf("收到GetPrivateDBConnInfo请求: RequestId=%s, DbConnId=%d", req.RequestId, req.DbConnId)

	// Mock数据 - 返回一个MySQL数据库连接信息
	return &pb.GetPrivateDBConnInfoResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: &pb.GetPrivateDBConnInfoResp{
			DbConnId:  req.DbConnId,
			ConnName:  "Mock数据库连接",
			Host:      "localhost",
			Port:      3306,
			Type:      1, // MySQL
			Username:  "root",
			Password:  "password",
			DbName:    "test_db",
			CreatedAt: "2024-01-01T00:00:00Z",
		},
	}, nil
}

// GetPrivateAssetInfoByEnName 通过资产英文名称获取资产详情
func (s *server) GetPrivateAssetInfoByEnName(ctx context.Context, req *pb.GetPrivateAssetInfoByEnNameRequest) (*pb.GetPrivateAssetInfoByEnNameResponse, error) {
	log.Printf("收到GetPrivateAssetInfoByEnName请求: RequestId=%s, AssetEnName=%s",
		req.BaseRequest.RequestId, req.AssetEnName)

	// Mock数据 - 返回一个资产信息
	return &pb.GetPrivateAssetInfoByEnNameResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: &pb.AssetInfo{
			AssetId:      "1",
			AssetNumber:  "ASSET001",
			AssetName:    "测试资产",
			AssetEnName:  req.AssetEnName,
			AssetType:    1, // 库表
			Scale:        "1000",
			Cycle:        "daily",
			TimeSpan:     "2024-01-01 to 2024-12-31",
			HolderCompany: "测试公司",
			Intro:        "这是一个测试资产",
			TxId:         "tx_123456",
			UploadedAt:   "2024-01-01T00:00:00Z",
			DataInfo: &pb.DataInfo{
				DbName:      "test_db",
				TableName:   "test_table",
				DataSourceId: 1,
				ItemList: []*pb.SaveTableColumnItem{
					{
						Name:        "id",
						DataType:    "int",
						DataLength:  11,
						Description: "主键ID",
						IsPrimaryKey: 1,
						PrivacyQuery: 0,
					},
					{
						Name:        "name",
						DataType:    "varchar",
						DataLength:  255,
						Description: "名称",
						IsPrimaryKey: 0,
						PrivacyQuery: 0,
					},
				},
			},
			VisibleType:   1,
			ParticipantId: "participant_001",
			ParticipantName: "测试参与方",
			AccountAlias:  "test_alias",
		},
	}, nil
}

func main() {
	// 获取端口，默认9091
	port := os.Getenv("PORT")
	if port == "" {
		port = "9091"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMiraIdaAccessServer(s, &server{})

	log.Printf("IDA Access Service Mock服务启动在端口: %s", port)
	log.Printf("gRPC服务地址: localhost:%s", port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

