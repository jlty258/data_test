package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	pb "ida-access-service-mock/mirapb"
	"ida-access-service-mock/storage"
	"google.golang.org/grpc"
)

// server 实现 MiraIdaAccess 服务
type server struct {
	pb.UnimplementedMiraIdaAccessServer
	storage storage.Storage
}

// GetPrivateDBConnInfo 获取数据库连接信息
func (s *server) GetPrivateDBConnInfo(ctx context.Context, req *pb.GetPrivateDBConnInfoRequest) (*pb.GetPrivateDBConnInfoResponse, error) {
	log.Printf("收到GetPrivateDBConnInfo请求: RequestId=%s, DbConnId=%d", req.RequestId, req.DbConnId)

	ds, err := s.storage.GetDataSource(ctx, req.DbConnId)
	if err != nil {
		return &pb.GetPrivateDBConnInfoResponse{
			BaseResponse: &pb.BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("获取数据源失败: %v", err),
			},
		}, nil
	}

	return &pb.GetPrivateDBConnInfoResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: ds,
	}, nil
}

// GetPrivateAssetInfoByEnName 通过资产英文名称获取资产详情
func (s *server) GetPrivateAssetInfoByEnName(ctx context.Context, req *pb.GetPrivateAssetInfoByEnNameRequest) (*pb.GetPrivateAssetInfoByEnNameResponse, error) {
	log.Printf("收到GetPrivateAssetInfoByEnName请求: RequestId=%s, AssetEnName=%s",
		req.BaseRequest.RequestId, req.AssetEnName)

	asset, err := s.storage.GetAssetByEnName(ctx, req.AssetEnName)
	if err != nil {
		return &pb.GetPrivateAssetInfoByEnNameResponse{
			BaseResponse: &pb.BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("获取资产失败: %v", err),
			},
		}, nil
	}

	return &pb.GetPrivateAssetInfoByEnNameResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: asset,
	}, nil
}

// CreateDataSource 创建数据源
func (s *server) CreateDataSource(ctx context.Context, req *pb.CreateDataSourceRequest) (*pb.CreateDataSourceResponse, error) {
	log.Printf("收到CreateDataSource请求: RequestId=%s, Name=%s, Host=%s, Port=%d, DbType=%d",
		req.BaseRequest.RequestId, req.Name, req.Host, req.Port, req.DbType)

	id, err := s.storage.CreateDataSource(ctx, req)
	if err != nil {
		return &pb.CreateDataSourceResponse{
			BaseResponse: &pb.BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("创建数据源失败: %v", err),
			},
		}, nil
	}

	log.Printf("数据源创建成功: ID=%d, Name=%s", id, req.Name)

	return &pb.CreateDataSourceResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: &pb.DataSourceId{
			Id: id,
		},
	}, nil
}

// CreateAsset 创建资产
func (s *server) CreateAsset(ctx context.Context, req *pb.CreateAssetRequest) (*pb.CreateAssetResponse, error) {
	log.Printf("收到CreateAsset请求: RequestId=%s, ResourceNumber=%s, EnName=%s",
		req.BaseRequest.RequestId, req.ResourceBasic.ResourceNumber, req.ResourceBasic.EnName)

	id, err := s.storage.CreateAsset(ctx, req)
	if err != nil {
		return &pb.CreateAssetResponse{
			BaseResponse: &pb.BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("创建资产失败: %v", err),
			},
		}, nil
	}

	log.Printf("资产创建成功: ID=%d, EnName=%s", id, req.ResourceBasic.EnName)

	return &pb.CreateAssetResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: &pb.ResourceId{
			Id: id,
		},
	}, nil
}

// GetPrivateAssetList 获取资产列表
func (s *server) GetPrivateAssetList(ctx context.Context, req *pb.GetPrivateAssetListRequest) (*pb.GetPrivateAssetListResponse, error) {
	log.Printf("收到GetPrivateAssetList请求: RequestId=%s, PageNumber=%d, PageSize=%d",
		req.BaseRequest.RequestId, req.PageNumber, req.PageSize)

	pageNumber := req.PageNumber
	if pageNumber <= 0 {
		pageNumber = 1
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}

	assets, total, err := s.storage.ListAssets(ctx, pageNumber, pageSize, req.Filters)
	if err != nil {
		return &pb.GetPrivateAssetListResponse{
			BaseResponse: &pb.BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("获取资产列表失败: %v", err),
			},
		}, nil
	}

	return &pb.GetPrivateAssetListResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: &pb.AssetList{
			Pagination: &pb.Pagination{
				PageNumber: pageNumber,
				PageSize:   pageSize,
				Total:      total,
			},
			List: assets,
		},
	}, nil
}

// GetPrivateAssetInfo 通过资产ID获取资产详情
func (s *server) GetPrivateAssetInfo(ctx context.Context, req *pb.GetPrivateAssetInfoRequest) (*pb.GetPrivateAssetInfoResponse, error) {
	log.Printf("收到GetPrivateAssetInfo请求: RequestId=%s, AssetId=%d", req.RequestId, req.AssetId)

	asset, err := s.storage.GetAssetById(ctx, req.AssetId)
	if err != nil {
		return &pb.GetPrivateAssetInfoResponse{
			BaseResponse: &pb.BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("获取资产失败: %v", err),
			},
		}, nil
	}

	return &pb.GetPrivateAssetInfoResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: asset,
	}, nil
}

// UpdateAsset 更新资产
func (s *server) UpdateAsset(ctx context.Context, req *pb.UpdateAssetRequest) (*pb.UpdateAssetResponse, error) {
	log.Printf("收到UpdateAsset请求: RequestId=%s, AssetId=%d",
		req.BaseRequest.RequestId, req.AssetId)

	asset, err := s.storage.UpdateAsset(ctx, req)
	if err != nil {
		return &pb.UpdateAssetResponse{
			BaseResponse: &pb.BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("更新资产失败: %v", err),
			},
		}, nil
	}

	log.Printf("资产更新成功: ID=%d", req.AssetId)

	return &pb.UpdateAssetResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: asset,
	}, nil
}

// DeleteAsset 删除资产
func (s *server) DeleteAsset(ctx context.Context, req *pb.DeleteAssetRequest) (*pb.DeleteAssetResponse, error) {
	log.Printf("收到DeleteAsset请求: RequestId=%s, AssetId=%d",
		req.BaseRequest.RequestId, req.AssetId)

	err := s.storage.DeleteAsset(ctx, req.AssetId)
	if err != nil {
		return &pb.DeleteAssetResponse{
			BaseResponse: &pb.BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("删除资产失败: %v", err),
			},
		}, nil
	}

	log.Printf("资产删除成功: ID=%d", req.AssetId)

	return &pb.DeleteAssetResponse{
		BaseResponse: &pb.BaseResponse{
			Code: 0,
			Msg:  "success",
		},
	}, nil
}

func main() {
	// 获取端口，默认9091
	port := os.Getenv("PORT")
	if port == "" {
		port = "9091"
	}

	// 获取存储类型，默认memory
	storageType := storage.StorageType(os.Getenv("STORAGE_TYPE"))
	if storageType == "" {
		storageType = storage.StorageTypeMemory
	}

	log.Printf("使用存储类型: %s", storageType)

	// 创建存储配置
	storageConfig := storage.StorageConfig{
		Type: storageType,
	}

	// 创建存储实例
	st, err := storage.NewStorage(storageConfig)
	if err != nil {
		log.Fatalf("创建存储失败: %v", err)
	}
	defer st.Close()

	// 创建gRPC服务器
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMiraIdaAccessServer(s, &server{
		storage: st,
	})

	log.Printf("IDA Access Service Mock服务启动在端口: %s", port)
	log.Printf("gRPC服务地址: localhost:%s", port)
	log.Printf("存储类型: %s", storageType)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// getEnvOrDefault 获取环境变量，如果不存在则返回默认值
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvIntOrDefault 获取环境变量并转换为int，如果不存在则返回默认值
func getEnvIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

