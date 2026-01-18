package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "data-service/generated/datasource"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 连接 gRPC 服务器
	conn, err := grpc.NewClient("localhost:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// 创建客户端
	client := pb.NewDataSourceServiceClient(conn)

	// 创建导入请求
	request := &pb.ImportDataRequest{
		Targets: []*pb.ImportTarget{
			{
				External: &pb.ExternalDataSource{
					AssetName:   "test_10k_16f_asset",
					ChainInfoId: "", // 可以为空
					Alias:       "", // 可以为空
				},
				TargetTableName: "test_10k_16f_imported",
				DbName:          "test_import_db",
				Columns:         []string{}, // 空数组表示导入所有列
				Keys:            []*pb.TableKey{},
			},
		},
	}

	fmt.Printf("Calling ImportData with request:\n")
	fmt.Printf("  AssetName: %s\n", request.Targets[0].External.AssetName)
	fmt.Printf("  TargetTableName: %s\n", request.Targets[0].TargetTableName)
	fmt.Printf("  DbName: %s\n", request.Targets[0].DbName)
	fmt.Println()

	// 调用 ImportData
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	response, err := client.ImportData(ctx, request)
	if err != nil {
		log.Fatalf("ImportData failed: %v", err)
	}

	// 打印结果
	fmt.Printf("ImportData Response:\n")
	fmt.Printf("  Success: %v\n", response.Success)
	fmt.Printf("  Message: %s\n", response.Message)
	
	if len(response.Results) > 0 {
		result := response.Results[0]
		fmt.Printf("  Result[0]:\n")
		fmt.Printf("    Success: %v\n", result.Success)
		fmt.Printf("    SourceTableName: %s\n", result.SourceTableName)
		fmt.Printf("    TargetTableName: %s\n", result.TargetTableName)
		fmt.Printf("    SourceDatabase: %s\n", result.SourceDatabase)
		fmt.Printf("    TargetDatabase: %s\n", result.TargetDatabase)
		fmt.Printf("    AffectedRows: %d\n", result.AffectedRows)
		if result.ErrorMessage != "" {
			fmt.Printf("    ErrorMessage: %s\n", result.ErrorMessage)
		}
	}
}
