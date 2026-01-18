package main

import (
	"context"
	"data-integrate-test/clients"
	"data-integrate-test/config"
	"flag"
	"fmt"
	"log"
	"time"
)

func main() {
	var (
		configPath = flag.String("config", "config/test_config.yaml", "配置文件路径")
		action     = flag.String("action", "all", "操作类型: create-ds(创建数据源), create-asset(创建资产), query-ds(查询数据源), query-asset(查询资产), all(全部)")
		dsId       = flag.Int("ds-id", 0, "数据源ID")
		assetId    = flag.Int("asset-id", 0, "资产ID")
	)
	flag.Parse()

	fmt.Println("========== IDA Service 数据源和资产管理 ==========")
	fmt.Println()

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 创建IDA客户端
	idaClient, err := clients.NewIDAServiceClient(cfg.MockServices.IDA.Host, cfg.MockServices.IDA.Port)
	if err != nil {
		log.Fatalf("创建IDA客户端失败: %v", err)
	}
	defer idaClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch *action {
	case "create-ds":
		createDataSource(ctx, idaClient)
	case "create-asset":
		if *dsId == 0 {
			log.Fatal("创建资产需要指定数据源ID: -ds-id")
		}
		createAsset(ctx, idaClient, int32(*dsId))
	case "query-ds":
		if *dsId == 0 {
			log.Fatal("查询数据源需要指定ID: -ds-id")
		}
		queryDataSource(ctx, idaClient, int32(*dsId))
	case "query-asset":
		if *assetId == 0 {
			queryAssetList(ctx, idaClient)
		} else {
			queryAsset(ctx, idaClient, int32(*assetId))
		}
	case "all":
		// 执行全部操作：创建数据源 -> 创建资产 -> 查询
		dsId := createDataSource(ctx, idaClient)
		if dsId > 0 {
			assetId := createAsset(ctx, idaClient, dsId)
			if assetId > 0 {
				queryDataSource(ctx, idaClient, dsId)
				queryAsset(ctx, idaClient, assetId)
			}
		}
	default:
		log.Fatalf("不支持的操作类型: %s", *action)
	}

	fmt.Println()
	fmt.Println("========== 操作完成 ==========")
}

// createDataSource 创建数据源
func createDataSource(ctx context.Context, client *clients.IDAServiceClient) int32 {
	fmt.Println("1. 创建数据源...")
	
	req := &clients.CreateDataSourceRequest{
		Name:         "测试MySQL数据源",
		Host:         "localhost",
		Port:         3306,
		DBType:       1, // 1-MySQL
		Username:     "root",
		Password:     "password",
		DatabaseName: "test_db",
	}
	
	resp, err := client.CreateDataSource(ctx, req)
	if err != nil {
		fmt.Printf("   ✗ 创建失败: %v\n", err)
		return 0
	}
	
	if resp.Success {
		fmt.Printf("   ✓ 创建成功\n")
		fmt.Printf("     数据源ID: %d\n", resp.DataSourceId)
		fmt.Printf("     消息: %s\n", resp.Message)
		return resp.DataSourceId
	} else {
		fmt.Printf("   ✗ 创建失败: %s\n", resp.Message)
		return 0
	}
}

// createAsset 创建资产
func createAsset(ctx context.Context, client *clients.IDAServiceClient, dataSourceId int32) int32 {
	fmt.Println()
	fmt.Println("2. 创建资产...")
	
	req := &clients.CreateAssetRequest{
		AssetName:    "测试数据资产",
		AssetEnName:  "test_data_asset",
		DataSourceId: dataSourceId,
		DBName:       "test_db",
		TableName:    "test_table",
	}
	
	resp, err := client.CreateAsset(ctx, req)
	if err != nil {
		fmt.Printf("   ✗ 创建失败: %v\n", err)
		return 0
	}
	
	if resp.Success {
		fmt.Printf("   ✓ 创建成功\n")
		fmt.Printf("     资产ID: %d\n", resp.AssetId)
		fmt.Printf("     消息: %s\n", resp.Message)
		return resp.AssetId
	} else {
		fmt.Printf("   ✗ 创建失败: %s\n", resp.Message)
		return 0
	}
}

// queryDataSource 查询数据源
func queryDataSource(ctx context.Context, client *clients.IDAServiceClient, dsId int32) {
	fmt.Println()
	fmt.Printf("3. 查询数据源 (ID: %d)...\n", dsId)
	
	req := &clients.GetPrivateDBConnInfoRequest{
		RequestId: fmt.Sprintf("query_ds_%d", time.Now().Unix()),
		DbConnId:  dsId,
	}
	
	resp, err := client.GetPrivateDBConnInfo(ctx, req)
	if err != nil {
		fmt.Printf("   ✗ 查询失败: %v\n", err)
		return
	}
	
	if resp.Code == 0 && resp.Data != nil {
		fmt.Printf("   ✓ 查询成功\n")
		fmt.Printf("     数据源ID: %d\n", resp.Data.DbConnId)
		fmt.Printf("     连接名: %s\n", resp.Data.ConnName)
		fmt.Printf("     主机: %s:%d\n", resp.Data.Host, resp.Data.Port)
		fmt.Printf("     数据库类型: %d\n", resp.Data.Type)
		fmt.Printf("     用户名: %s\n", resp.Data.Username)
		fmt.Printf("     数据库名: %s\n", resp.Data.DbName)
	} else {
		fmt.Printf("   ✗ 查询失败: %s\n", resp.Msg)
	}
}

// queryAssetList 查询资产列表
func queryAssetList(ctx context.Context, client *clients.IDAServiceClient) {
	fmt.Println()
	fmt.Println("4. 查询资产列表...")
	
	req := &clients.GetPrivateAssetListRequest{
		RequestId:  fmt.Sprintf("query_list_%d", time.Now().Unix()),
		PageNumber: 1,
		PageSize:   10,
	}
	
	resp, err := client.GetPrivateAssetList(ctx, req)
	if err != nil {
		fmt.Printf("   ✗ 查询失败: %v\n", err)
		return
	}
	
	if resp.Code == 0 {
		fmt.Printf("   ✓ 查询成功\n")
		fmt.Printf("     总记录数: %d\n", resp.Data.Pagination.Total)
		fmt.Printf("     当前页: %d\n", resp.Data.Pagination.PageNumber)
		fmt.Printf("     每页条数: %d\n", resp.Data.Pagination.PageSize)
		fmt.Printf("     资产数量: %d\n", len(resp.Data.List))
		
		for i, asset := range resp.Data.List {
			fmt.Printf("     资产[%d]: ID=%s, 名称=%s, 英文名=%s\n", 
				i+1, asset.AssetId, asset.AssetName, asset.Alias)
		}
	} else {
		fmt.Printf("   ✗ 查询失败: %s\n", resp.Msg)
	}
}

// queryAsset 查询资产详情
func queryAsset(ctx context.Context, client *clients.IDAServiceClient, assetId int32) {
	fmt.Println()
	fmt.Printf("5. 查询资产详情 (ID: %d)...\n", assetId)
	
	req := &clients.GetPrivateAssetInfoRequest{
		RequestId: fmt.Sprintf("query_asset_%d", time.Now().Unix()),
		AssetId:   assetId,
	}
	
	resp, err := client.GetPrivateAssetInfo(ctx, req)
	if err != nil {
		fmt.Printf("   ✗ 查询失败: %v\n", err)
		return
	}
	
	if resp.Code == 0 && resp.Data != nil {
		fmt.Printf("   ✓ 查询成功\n")
		fmt.Printf("     资产ID: %s\n", resp.Data.AssetId)
		fmt.Printf("     资产编号: %s\n", resp.Data.AssetNumber)
		fmt.Printf("     资产名称: %s\n", resp.Data.AssetName)
		fmt.Printf("     资产英文名: %s\n", resp.Data.AssetEnName)
		fmt.Printf("     资产类型: %d\n", resp.Data.AssetType)
		fmt.Printf("     数据规模: %s\n", resp.Data.Scale)
		fmt.Printf("     持有公司: %s\n", resp.Data.HolderCompany)
		
		if resp.Data.DataInfo != nil {
			fmt.Printf("     数据库名: %s\n", resp.Data.DataInfo.DbName)
			fmt.Printf("     表名: %s\n", resp.Data.DataInfo.TableName)
			fmt.Printf("     数据源ID: %d\n", resp.Data.DataInfo.DataSourceId)
		}
	} else {
		fmt.Printf("   ✗ 查询失败: %s\n", resp.Msg)
	}
}

