package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	pb "ida-access-service-mock/mirapb"
	commonpb "ida-access-service-mock/mirapb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// IDA Service 地址
	idaHost := "localhost"
	idaPort := 9091
	if len(os.Args) > 1 {
		idaHost = os.Args[1]
	}
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &idaPort)
	}

	addr := fmt.Sprintf("%s:%d", idaHost, idaPort)
	
	// 连接 IDA Service
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("连接 IDA Service 失败: %v", err)
	}
	defer conn.Close()

	client := pb.NewMiraIdaAccessClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("========== IDA Access Service Mock CRUD 测试 ==========")
	fmt.Printf("连接地址: %s\n\n", addr)

	// 1. 创建数据源 (Create)
	fmt.Println("1. 创建数据源 (CreateDataSource)...")
	dsReq := &pb.CreateDataSourceRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: fmt.Sprintf("test_ds_%d", time.Now().Unix()),
		},
		DbPattern:    1, // 关系型
		DbType:       1, // MySQL
		Name:         "测试数据源",
		Host:         "localhost",
		Port:         3306,
		Username:     "root",
		Password:     "password",
		InstanceName: "test_db",
		Address:      "test_address",
		ChainInfoId:  "test_chain",
		TenantId:     1,
		Uin:          1,
	}

	dsResp, err := client.CreateDataSource(ctx, dsReq)
	if err != nil {
		log.Fatalf("创建数据源失败: %v", err)
	}

	if dsResp.BaseResponse.Code != 0 {
		log.Fatalf("创建数据源失败: %s", dsResp.BaseResponse.Msg)
	}

	dsId := dsResp.Data.Id
	fmt.Printf("   ✅ 数据源创建成功: ID=%d\n\n", dsId)

	// 2. 查询数据源 (Read)
	fmt.Println("2. 查询数据源 (GetPrivateDBConnInfo)...")
	getDSReq := &pb.GetPrivateDBConnInfoRequest{
		RequestId: fmt.Sprintf("query_ds_%d", time.Now().Unix()),
		DbConnId:  dsId,
	}

	getDSResp, err := client.GetPrivateDBConnInfo(ctx, getDSReq)
	if err != nil {
		log.Fatalf("查询数据源失败: %v", err)
	}

	if getDSResp.BaseResponse.Code != 0 {
		log.Fatalf("查询数据源失败: %s", getDSResp.BaseResponse.Msg)
	}

	dsJson, _ := json.MarshalIndent(getDSResp.Data, "   ", "  ")
	fmt.Printf("   ✅ 数据源信息:\n%s\n\n", string(dsJson))

	// 3. 创建资产 (Create)
	fmt.Println("3. 创建资产 (CreateAsset)...")
	assetReq := &pb.CreateAssetRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: fmt.Sprintf("test_asset_%d", time.Now().Unix()),
		},
		ChainInfo: &pb.ChainInfo{
			ChainInfoId: "test_chain",
			Address:     "test_address",
			Cid:         "test_cid",
		},
		ResourceBasic: &pb.ResourceBasic{
			ResourceNumber: fmt.Sprintf("RES_%d", time.Now().Unix()),
			ZhName:         "测试资产",
			EnName:         fmt.Sprintf("test_asset_%d", time.Now().Unix()),
			Description:    "这是一个测试资产",
			ScaleValue:     1000,
			ScaleUnit:      1, // MB
			UseLimit:       "测试用途",
			Type:           1, // 库表
			DataType:       2, // 企业数据
			Authorized:     1, // 不需要授权
			MachineLearning: 1, // 不支持机器学习
		},
		Table: &pb.Table{
			DataSourceId: dsId,
			TableName:    "test_table",
			Columns: []*pb.TableColumn{
				{
					OriginName: "id",
					Name:       "id",
					DataType:   "int",
					DataLength: 11,
					PrimaryKey: 2, // 是主键
					NotNull:    2, // 非空
					Description: "主键ID",
					Level:      1,
				},
				{
					OriginName: "name",
					Name:       "name",
					DataType:   "varchar",
					DataLength: 255,
					PrimaryKey: 1, // 不是主键
					NotNull:    1, // 可空
					Description: "名称",
					Level:      1,
				},
			},
		},
	}

	assetResp, err := client.CreateAsset(ctx, assetReq)
	if err != nil {
		log.Fatalf("创建资产失败: %v", err)
	}

	if assetResp.BaseResponse.Code != 0 {
		log.Fatalf("创建资产失败: %s", assetResp.BaseResponse.Msg)
	}

	assetId := assetResp.Data.Id
	assetEnName := assetReq.ResourceBasic.EnName
	fmt.Printf("   ✅ 资产创建成功: ID=%d, EnName=%s\n\n", assetId, assetEnName)

	// 4. 查询资产列表 (Read)
	fmt.Println("4. 查询资产列表 (GetPrivateAssetList)...")
	assetListReq := &pb.GetPrivateAssetListRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: fmt.Sprintf("query_list_%d", time.Now().Unix()),
		},
		PageNumber: 1,
		PageSize:   10,
		Filters:    []*pb.Filter{},
	}

	assetListResp, err := client.GetPrivateAssetList(ctx, assetListReq)
	if err != nil {
		log.Fatalf("查询资产列表失败: %v", err)
	}

	if assetListResp.BaseResponse.Code != 0 {
		log.Fatalf("查询资产列表失败: %s", assetListResp.BaseResponse.Msg)
	}

	fmt.Printf("   ✅ 资产列表 (共 %d 条):\n", assetListResp.Data.Pagination.Total)
	for i, asset := range assetListResp.Data.List {
		fmt.Printf("   [%d] ID=%s, Name=%s, EnName=%s\n", 
			i+1, asset.AssetId, asset.AssetName, asset.AssetNumber)
	}
	fmt.Println()

	// 5. 通过ID查询资产详情 (Read)
	fmt.Println("5. 通过ID查询资产详情 (GetPrivateAssetInfo)...")
	assetInfoReq := &pb.GetPrivateAssetInfoRequest{
		RequestId: fmt.Sprintf("query_info_%d", time.Now().Unix()),
		AssetId:   assetId,
	}

	assetInfoResp, err := client.GetPrivateAssetInfo(ctx, assetInfoReq)
	if err != nil {
		log.Fatalf("查询资产详情失败: %v", err)
	}

	if assetInfoResp.BaseResponse.Code != 0 {
		log.Fatalf("查询资产详情失败: %s", assetInfoResp.BaseResponse.Msg)
	}

	assetInfoJson, _ := json.MarshalIndent(assetInfoResp.Data, "   ", "  ")
	fmt.Printf("   ✅ 资产详情:\n%s\n\n", string(assetInfoJson))

	// 6. 通过英文名查询资产详情 (Read)
	fmt.Println("6. 通过英文名查询资产详情 (GetPrivateAssetInfoByEnName)...")
	assetByNameReq := &pb.GetPrivateAssetInfoByEnNameRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: fmt.Sprintf("query_by_name_%d", time.Now().Unix()),
		},
		AssetEnName: assetEnName,
	}

	assetByNameResp, err := client.GetPrivateAssetInfoByEnName(ctx, assetByNameReq)
	if err != nil {
		log.Fatalf("通过英文名查询资产失败: %v", err)
	}

	if assetByNameResp.BaseResponse.Code != 0 {
		log.Fatalf("通过英文名查询资产失败: %s", assetByNameResp.BaseResponse.Msg)
	}

	fmt.Printf("   ✅ 资产信息: ID=%s, Name=%s, EnName=%s\n", 
		assetByNameResp.Data.AssetId,
		assetByNameResp.Data.AssetName,
		assetByNameResp.Data.AssetEnName)
	if assetByNameResp.Data.DataInfo != nil {
		fmt.Printf("   数据源ID: %d, 表名: %s\n", 
			assetByNameResp.Data.DataInfo.DataSourceId,
			assetByNameResp.Data.DataInfo.TableName)
	}
	fmt.Println()

	fmt.Println("========== CRUD 测试完成 ==========")
	fmt.Println("✅ 所有测试通过！")
	fmt.Println("")
	fmt.Println("测试总结:")
	fmt.Printf("  - 创建数据源: ✅ (ID=%d)\n", dsId)
	fmt.Printf("  - 查询数据源: ✅\n")
	fmt.Printf("  - 创建资产: ✅ (ID=%d)\n", assetId)
	fmt.Printf("  - 查询资产列表: ✅\n")
	fmt.Printf("  - 查询资产详情(按ID): ✅\n")
	fmt.Printf("  - 查询资产详情(按英文名): ✅\n")
}

