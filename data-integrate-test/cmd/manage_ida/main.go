package main

import (
	"context"
	"data-integrate-test/clients"
	"data-integrate-test/config"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	var (
		configPath = flag.String("config", "config/test_config.yaml", "配置文件路径")
		action     = flag.String("action", "help", "操作类型: create-ds(创建数据源), create-asset(创建资产), query-ds(查询数据源), query-asset(查询资产), query-all(查询所有), all(全部操作)")
		dsId       = flag.Int("ds-id", 0, "数据源ID")
		assetId    = flag.Int("asset-id", 0, "资产ID")
		pageNum    = flag.Int("page", 1, "页码（查询资产列表时使用）")
		pageSize   = flag.Int("size", 10, "每页条数（查询资产列表时使用）")
	)
	flag.Parse()

	// 显示帮助
	if *action == "help" || *action == "" {
		showHelp()
		return
	}

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
			queryAssetList(ctx, idaClient, int32(*pageNum), int32(*pageSize))
		} else {
			queryAsset(ctx, idaClient, int32(*assetId))
		}
	case "query-all":
		queryAll(ctx, idaClient, int32(*pageNum), int32(*pageSize))
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
		log.Fatalf("不支持的操作类型: %s\n使用 -action=help 查看帮助", *action)
	}

	fmt.Println()
	fmt.Println("========== 操作完成 ==========")
}

// showHelp 显示帮助信息
func showHelp() {
	fmt.Println("IDA Service 数据源和资产管理工具")
	fmt.Println()
	fmt.Println("用法:")
	fmt.Println("  manage-ida -action=<操作类型> [参数...]")
	fmt.Println()
	fmt.Println("操作类型:")
	fmt.Println("  create-ds      创建数据源")
	fmt.Println("  create-asset   创建资产（需要 -ds-id）")
	fmt.Println("  query-ds       查询数据源详情（需要 -ds-id）")
	fmt.Println("  query-asset    查询资产（不指定 -asset-id 时查询列表，支持 -page 和 -size）")
	fmt.Println("  query-all      查询所有（资产列表和关联的数据源，支持 -page 和 -size）")
	fmt.Println("  all            执行全部操作（创建数据源 -> 创建资产 -> 查询）")
	fmt.Println()
	fmt.Println("参数:")
	fmt.Println("  -config        配置文件路径（默认: config/test_config.yaml）")
	fmt.Println("  -action        操作类型（必需）")
	fmt.Println("  -ds-id         数据源ID")
	fmt.Println("  -asset-id      资产ID")
	fmt.Println("  -page          页码（默认: 1）")
	fmt.Println("  -size          每页条数（默认: 10）")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  manage-ida -action=create-ds")
	fmt.Println("  manage-ida -action=create-asset -ds-id=1000")
	fmt.Println("  manage-ida -action=query-ds -ds-id=1000")
	fmt.Println("  manage-ida -action=query-asset")
	fmt.Println("  manage-ida -action=query-asset -asset-id=2000")
	fmt.Println("  manage-ida -action=query-all -page=1 -size=20")
	fmt.Println("  manage-ida -action=all")
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
	fmt.Printf("查询数据源 (ID: %d)...\n", dsId)
	fmt.Println()

	req := &clients.GetPrivateDBConnInfoRequest{
		RequestId: fmt.Sprintf("query_ds_%d_%d", dsId, os.Getpid()),
		DbConnId:  dsId,
	}

	resp, err := client.GetPrivateDBConnInfo(ctx, req)
	if err != nil {
		fmt.Printf("   ✗ 查询失败: %v\n", err)
		return
	}

	if resp.Code != 0 {
		fmt.Printf("   ✗ 查询失败: %s\n", resp.Msg)
		return
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
	if resp.Data.CreatedAt != "" {
		fmt.Printf("  创建时间: %s\n", resp.Data.CreatedAt)
	}
	fmt.Println()
}

// queryAssetList 查询资产列表
func queryAssetList(ctx context.Context, client *clients.IDAServiceClient, pageNum, pageSize int32) {
	fmt.Println()
	fmt.Println("查询资产列表...")
	fmt.Println()

	req := &clients.GetPrivateAssetListRequest{
		RequestId:  fmt.Sprintf("query_asset_list_%d", os.Getpid()),
		PageNumber: pageNum,
		PageSize:   pageSize,
		Filters:    []clients.Filter{},
	}

	resp, err := client.GetPrivateAssetList(ctx, req)
	if err != nil {
		fmt.Printf("   ✗ 查询失败: %v\n", err)
		return
	}

	if resp.Code != 0 {
		fmt.Printf("   ✗ 查询失败: %s\n", resp.Msg)
		return
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
			if asset.ParticipantName != "" {
				fmt.Printf("    参与方: %s (%s)\n", asset.ParticipantName, asset.ParticipantId)
			}
			if asset.UploadedAt != "" {
				fmt.Printf("    上传时间: %s\n", asset.UploadedAt)
			}
			fmt.Println()
		}
	}
}

// queryAsset 查询资产详情
func queryAsset(ctx context.Context, client *clients.IDAServiceClient, assetId int32) {
	fmt.Println()
	fmt.Printf("查询资产详情 (ID: %d)...\n", assetId)
	fmt.Println()

	req := &clients.GetPrivateAssetInfoRequest{
		RequestId: fmt.Sprintf("query_asset_%d_%d", assetId, os.Getpid()),
		AssetId:   assetId,
	}

	resp, err := client.GetPrivateAssetInfo(ctx, req)
	if err != nil {
		fmt.Printf("   ✗ 查询失败: %v\n", err)
		return
	}

	if resp.Code != 0 {
		fmt.Printf("   ✗ 查询失败: %s\n", resp.Msg)
		return
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
	if resp.Data.Scale != "" {
		fmt.Printf("数据规模: %s\n", resp.Data.Scale)
	}
	if resp.Data.Cycle != "" {
		fmt.Printf("更新周期: %s\n", resp.Data.Cycle)
	}
	if resp.Data.TimeSpan != "" {
		fmt.Printf("时间跨度: %s\n", resp.Data.TimeSpan)
	}
	if resp.Data.HolderCompany != "" {
		fmt.Printf("持有公司: %s\n", resp.Data.HolderCompany)
	}
	if resp.Data.Intro != "" {
		fmt.Printf("简介: %s\n", resp.Data.Intro)
	}
	if resp.Data.TxId != "" {
		fmt.Printf("交易ID: %s\n", resp.Data.TxId)
	}
	if resp.Data.UploadedAt != "" {
		fmt.Printf("上传时间: %s\n", resp.Data.UploadedAt)
	}
	if resp.Data.ParticipantId != "" {
		fmt.Printf("参与方ID: %s\n", resp.Data.ParticipantId)
	}
	if resp.Data.ParticipantName != "" {
		fmt.Printf("参与方名称: %s\n", resp.Data.ParticipantName)
	}
	if resp.Data.AccountAlias != "" {
		fmt.Printf("账户别名: %s\n", resp.Data.AccountAlias)
	}

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
}

// queryAll 查询所有：资产列表和关联的数据源
func queryAll(ctx context.Context, client *clients.IDAServiceClient, pageNum, pageSize int32) {
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
		fmt.Printf("   ✗ 查询失败: %v\n", err)
		return
	}

	if assetListResp.Code != 0 {
		fmt.Printf("   ✗ 查询失败: %s\n", assetListResp.Msg)
		return
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
			if asset.ParticipantName != "" {
				fmt.Printf("    参与方: %s (%s)\n", asset.ParticipantName, asset.ParticipantId)
			}
			if asset.UploadedAt != "" {
				fmt.Printf("    上传时间: %s\n", asset.UploadedAt)
			}
			fmt.Println()
		}
	}

	// 2. 查询数据源（如果有资产关联的数据源ID）
	if len(assetListResp.Data.List) > 0 {
		fmt.Println("2. 查询数据源信息...")
		fmt.Println()
		// 由于当前实现中资产列表不包含数据源ID，我们跳过数据源查询
		// 如果需要测试数据源查询，可以先创建数据源，然后使用其ID查询
		fmt.Println("  提示: 如需查询数据源，请先创建数据源，然后使用 -action=query-ds -ds-id=<ID> 查询")
	}
}
