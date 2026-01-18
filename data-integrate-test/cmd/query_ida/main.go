package main

import (
	"context"
	"data-integrate-test/clients"
	"data-integrate-test/config"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	var (
		configPath = flag.String("config", "../../config/test_config.yaml", "配置文件路径（相对于cmd/query_ida目录）")
		queryType  = flag.String("type", "all", "查询类型: all(全部), datasource(数据源), asset(资产)")
		dsId       = flag.Int("ds-id", 0, "数据源ID（查询单个数据源时使用）")
		assetId    = flag.Int("asset-id", 0, "资产ID（查询单个资产时使用）")
		pageNum    = flag.Int("page", 1, "页码（查询资产列表时使用）")
		pageSize   = flag.Int("size", 10, "每页条数（查询资产列表时使用）")
	)
	flag.Parse()

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

	ctx := context.Background()

	switch *queryType {
	case "all", "":
		// 查询所有：先查询资产列表，然后查询关联的数据源
		queryAll(ctx, idaClient, int32(*pageNum), int32(*pageSize))
	case "datasource":
		if *dsId == 0 {
			log.Fatal("查询数据源需要指定 -ds-id 参数")
		}
		queryDataSource(ctx, idaClient, int32(*dsId))
	case "asset":
		if *assetId == 0 {
			// 如果没有指定asset-id，则查询资产列表
			queryAssetList(ctx, idaClient, int32(*pageNum), int32(*pageSize))
		} else {
			// 查询单个资产详情
			queryAsset(ctx, idaClient, int32(*assetId))
		}
	default:
		log.Fatalf("不支持的查询类型: %s", *queryType)
	}
}

// queryAll 查询所有：资产列表和关联的数据源
func queryAll(ctx context.Context, client *clients.IDAServiceClient, pageNum, pageSize int32) {
	fmt.Println("========== 查询IDA Service中的资产和数据源 ==========")
	fmt.Println()

	// 1. 查询资产列表
	fmt.Println("1. 查询资产列表...")
	fmt.Println()
	
	assetListReq := &clients.GetPrivateAssetListRequest{
		RequestId:  fmt.Sprintf("query_%d", os.Getpid()),
		PageNumber: pageNum,
		PageSize:   pageSize,
		Filters:    []clients.Filter{},
	}

	assetListResp, err := client.GetPrivateAssetList(ctx, assetListReq)
	if err != nil {
		log.Fatalf("查询资产列表失败: %v", err)
	}

	if assetListResp.Code != 0 {
		log.Fatalf("查询资产列表失败: %s", assetListResp.Msg)
	}

	fmt.Printf("资产列表 (共 %d 条，当前页 %d/%d):\n", 
		assetListResp.Data.Pagination.Total,
		assetListResp.Data.Pagination.PageNumber,
		(assetListResp.Data.Pagination.Total+int64(assetListResp.Data.Pagination.PageSize)-1)/int64(assetListResp.Data.Pagination.PageSize))
	fmt.Println()

	if len(assetListResp.Data.List) == 0 {
		fmt.Println("  未找到资产")
	} else {
		for i, asset := range assetListResp.Data.List {
			fmt.Printf("  资产 #%d:\n", i+1)
			fmt.Printf("    ID: %s\n", asset.AssetId)
			fmt.Printf("    资产编号: %s\n", asset.AssetNumber)
			fmt.Printf("    资产名称: %s\n", asset.AssetName)
			fmt.Printf("    持有公司: %s\n", asset.HolderCompany)
			fmt.Printf("    参与方: %s (%s)\n", asset.ParticipantName, asset.ParticipantId)
			fmt.Printf("    上传时间: %s\n", asset.UploadedAt)
			fmt.Println()
		}
	}

	// 2. 查询数据源（如果有资产关联的数据源ID）
	if len(assetListResp.Data.List) > 0 {
		fmt.Println("2. 查询数据源信息...")
		fmt.Println()
		// 由于当前实现中资产列表不包含数据源ID，我们跳过数据源查询
		// 如果需要测试数据源查询，可以先创建数据源，然后使用其ID查询
		fmt.Println("  提示: 如需查询数据源，请先创建数据源，然后使用 -ds-id 参数查询")
	}

	fmt.Println("========== 查询完成 ==========")
}

// queryDataSource 查询单个数据源
func queryDataSource(ctx context.Context, client *clients.IDAServiceClient, dsId int32) {
	fmt.Printf("查询数据源 ID: %d\n", dsId)
	fmt.Println()

	req := &clients.GetPrivateDBConnInfoRequest{
		RequestId: fmt.Sprintf("query_ds_%d_%d", dsId, os.Getpid()),
		DbConnId:  dsId,
	}

	resp, err := client.GetPrivateDBConnInfo(ctx, req)
	if err != nil {
		log.Fatalf("查询数据源失败: %v", err)
	}

	if resp.Code != 0 {
		log.Fatalf("查询数据源失败: %s", resp.Msg)
	}

	if resp.Data == nil {
		fmt.Println("  未找到数据源")
		return
	}

	fmt.Printf("  数据源ID: %d\n", resp.Data.DbConnId)
	fmt.Printf("  连接名称: %s\n", resp.Data.ConnName)
	fmt.Printf("  地址: %s:%d\n", resp.Data.Host, resp.Data.Port)
	
	dbTypeMap := map[int32]string{
		1: "MySQL",
		2: "KingBase",
		3: "GBase",
		4: "VastBase",
	}
	dbTypeName := dbTypeMap[resp.Data.Type]
	if dbTypeName == "" {
		dbTypeName = fmt.Sprintf("Unknown(%d)", resp.Data.Type)
	}
	fmt.Printf("  数据库类型: %s\n", dbTypeName)
	fmt.Printf("  数据库名: %s\n", resp.Data.DbName)
	fmt.Printf("  用户名: %s\n", resp.Data.Username)
	fmt.Printf("  创建时间: %s\n", resp.Data.CreatedAt)
	fmt.Println()
}

// queryAssetList 查询资产列表
func queryAssetList(ctx context.Context, client *clients.IDAServiceClient, pageNum, pageSize int32) {
	fmt.Println("========== 查询资产列表 ==========")
	fmt.Println()

	req := &clients.GetPrivateAssetListRequest{
		RequestId:  fmt.Sprintf("query_asset_list_%d", os.Getpid()),
		PageNumber: pageNum,
		PageSize:   pageSize,
		Filters:    []clients.Filter{},
	}

	resp, err := client.GetPrivateAssetList(ctx, req)
	if err != nil {
		log.Fatalf("查询资产列表失败: %v", err)
	}

	if resp.Code != 0 {
		log.Fatalf("查询资产列表失败: %s", resp.Msg)
	}

	fmt.Printf("资产列表 (共 %d 条，当前页 %d):\n", 
		resp.Data.Pagination.Total,
		resp.Data.Pagination.PageNumber)
	fmt.Println()

	if len(resp.Data.List) == 0 {
		fmt.Println("  未找到资产")
	} else {
		for i, asset := range resp.Data.List {
			fmt.Printf("  资产 #%d:\n", i+1)
			fmt.Printf("    ID: %s\n", asset.AssetId)
			fmt.Printf("    资产编号: %s\n", asset.AssetNumber)
			fmt.Printf("    资产名称: %s\n", asset.AssetName)
			fmt.Printf("    持有公司: %s\n", asset.HolderCompany)
			fmt.Printf("    参与方: %s (%s)\n", asset.ParticipantName, asset.ParticipantId)
			fmt.Printf("    上传时间: %s\n", asset.UploadedAt)
			fmt.Println()
		}
	}

	fmt.Println("========== 查询完成 ==========")
}

// queryAsset 查询单个资产详情
func queryAsset(ctx context.Context, client *clients.IDAServiceClient, assetId int32) {
	fmt.Printf("========== 查询资产详情 (ID: %d) ==========\n", assetId)
	fmt.Println()

	req := &clients.GetPrivateAssetInfoRequest{
		RequestId: fmt.Sprintf("query_asset_%d_%d", assetId, os.Getpid()),
		AssetId:   assetId,
	}

	resp, err := client.GetPrivateAssetInfo(ctx, req)
	if err != nil {
		log.Fatalf("查询资产详情失败: %v", err)
	}

	if resp.Code != 0 {
		log.Fatalf("查询资产详情失败: %s", resp.Msg)
	}

	if resp.Data == nil {
		fmt.Println("  未找到资产")
		return
	}

	fmt.Printf("资产ID: %s\n", resp.Data.AssetId)
	fmt.Printf("资产编号: %s\n", resp.Data.AssetNumber)
	fmt.Printf("资产名称: %s\n", resp.Data.AssetName)
	fmt.Printf("资产英文名: %s\n", resp.Data.AssetEnName)
	
	assetTypeMap := map[int32]string{
		1: "库表",
		2: "文件",
	}
	assetTypeName := assetTypeMap[resp.Data.AssetType]
	if assetTypeName == "" {
		assetTypeName = fmt.Sprintf("Unknown(%d)", resp.Data.AssetType)
	}
	fmt.Printf("资产类型: %s\n", assetTypeName)
	fmt.Printf("数据规模: %s\n", resp.Data.Scale)
	fmt.Printf("更新周期: %s\n", resp.Data.Cycle)
	fmt.Printf("时间跨度: %s\n", resp.Data.TimeSpan)
	fmt.Printf("持有公司: %s\n", resp.Data.HolderCompany)
	fmt.Printf("简介: %s\n", resp.Data.Intro)
	fmt.Printf("交易ID: %s\n", resp.Data.TxId)
	fmt.Printf("上传时间: %s\n", resp.Data.UploadedAt)
	fmt.Printf("参与方ID: %s\n", resp.Data.ParticipantId)
	fmt.Printf("参与方名称: %s\n", resp.Data.ParticipantName)
	fmt.Printf("账户别名: %s\n", resp.Data.AccountAlias)
	
	if resp.Data.DataInfo != nil {
		fmt.Println()
		fmt.Println("数据库信息:")
		fmt.Printf("  数据库名: %s\n", resp.Data.DataInfo.DbName)
		fmt.Printf("  表名: %s\n", resp.Data.DataInfo.TableName)
		fmt.Printf("  数据源ID: %d\n", resp.Data.DataInfo.DataSourceId)
		if len(resp.Data.DataInfo.ItemList) > 0 {
			fmt.Println("  字段列表:")
			for _, col := range resp.Data.DataInfo.ItemList {
				fmt.Printf("    - %s (%s, 长度: %d)", col.Name, col.DataType, col.DataLength)
				if col.IsPrimaryKey == 1 {
					fmt.Print(" [主键]")
				}
				if col.Description != "" {
					fmt.Printf(" - %s", col.Description)
				}
				fmt.Println()
			}
		}
	}

	fmt.Println()
	fmt.Println("========== 查询完成 ==========")
}

