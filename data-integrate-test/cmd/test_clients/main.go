package main

import (
	"bytes"
	"context"
	"data-integrate-test/clients"
	"data-integrate-test/config"
	"data-integrate-test/utils"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	pb "data-integrate-test/generated/datasource"
)

func main() {
	var (
		configPath = flag.String("config", "config/test_config.yaml", "配置文件路径")
	)
	flag.Parse()

	fmt.Println("========== 客户端连接测试 ==========")
	fmt.Println()

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	fmt.Printf("配置信息:\n")
	fmt.Printf("  - Data Service: %s:%d\n", cfg.DataService.Host, cfg.DataService.Port)
	fmt.Printf("  - IDA Service: %s:%d\n", cfg.MockServices.IDA.Host, cfg.MockServices.IDA.Port)
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 测试 IDA Service 客户端连接
	fmt.Println("1. 测试 IDA Service 客户端连接...")
	idaClient, err := clients.NewIDAServiceClient(cfg.MockServices.IDA.Host, cfg.MockServices.IDA.Port)
	if err != nil {
		fmt.Printf("   ✗ 连接失败: %v\n", err)
	} else {
		fmt.Printf("   ✓ 连接成功\n")
		
		// 测试获取数据源信息
		fmt.Println("   测试 GetPrivateDBConnInfo...")
		dsReq := &clients.GetPrivateDBConnInfoRequest{
			RequestId: fmt.Sprintf("test_%d", time.Now().Unix()),
			DbConnId:  1,
		}
		dsResp, err := idaClient.GetPrivateDBConnInfo(ctx, dsReq)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			fmt.Printf("     响应码: %d\n", dsResp.Code)
			fmt.Printf("     消息: %s\n", dsResp.Msg)
			if dsResp.Data != nil {
				fmt.Printf("     数据源ID: %d\n", dsResp.Data.DbConnId)
				fmt.Printf("     连接名: %s\n", dsResp.Data.ConnName)
				fmt.Printf("     主机: %s:%d\n", dsResp.Data.Host, dsResp.Data.Port)
			}
		}

		// 测试获取资产列表
		fmt.Println("   测试 GetPrivateAssetList...")
		assetListReq := &clients.GetPrivateAssetListRequest{
			RequestId:  fmt.Sprintf("test_%d", time.Now().Unix()),
			PageNumber: 1,
			PageSize:   10,
		}
		assetListResp, err := idaClient.GetPrivateAssetList(ctx, assetListReq)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			fmt.Printf("     响应码: %d\n", assetListResp.Code)
			fmt.Printf("     消息: %s\n", assetListResp.Msg)
			fmt.Printf("     总记录数: %d\n", assetListResp.Data.Pagination.Total)
			fmt.Printf("     资产数量: %d\n", len(assetListResp.Data.List))
		}

		// 测试获取资产详情
		fmt.Println("   测试 GetPrivateAssetInfo...")
		assetReq := &clients.GetPrivateAssetInfoRequest{
			RequestId: fmt.Sprintf("test_%d", time.Now().Unix()),
			AssetId:   1,
		}
		assetResp, err := idaClient.GetPrivateAssetInfo(ctx, assetReq)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			fmt.Printf("     响应码: %d\n", assetResp.Code)
			fmt.Printf("     消息: %s\n", assetResp.Msg)
			if assetResp.Data != nil {
				fmt.Printf("     资产ID: %s\n", assetResp.Data.AssetId)
				fmt.Printf("     资产名称: %s\n", assetResp.Data.AssetName)
				fmt.Printf("     资产英文名: %s\n", assetResp.Data.AssetEnName)
			}
		}

		idaClient.Close()
	}
	fmt.Println()

	// 测试 Data Service 客户端连接
	fmt.Println("2. 测试 Data Service 客户端连接...")
	dataClient, err := clients.NewDataServiceClient(cfg.DataService.Host, cfg.DataService.Port)
	if err != nil {
		fmt.Printf("   ✗ 连接失败: %v\n", err)
		fmt.Printf("   提示: 请确保 mira-data-service-server 服务正在运行\n")
	} else {
		fmt.Printf("   ✓ 连接成功\n")
		defer dataClient.Close()

		// 测试 GetTableInfo
		fmt.Println("   测试 GetTableInfo...")
		tableInfoReq := &pb.TableInfoRequest{
			AssetName:   "test_asset",
			ChainInfoId: "test_chain",
			RequestId:   fmt.Sprintf("test_%d", time.Now().Unix()),
			IsExactQuery: true,
			Alias:       "",
		}
		tableInfoResp, err := dataClient.GetTableInfo(ctx, tableInfoReq)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
			fmt.Printf("   提示: 这可能是正常的，因为资产可能不存在\n")
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			fmt.Printf("     表名: %s\n", tableInfoResp.TableName)
			fmt.Printf("     记录数: %d\n", tableInfoResp.RecordCount)
			fmt.Printf("     记录大小: %d 字节\n", tableInfoResp.RecordSize)
			fmt.Printf("     列数: %d\n", len(tableInfoResp.Columns))
		}

		// 测试 ReadStreamingData（需要有效的资产信息）
		fmt.Println("   测试 ReadStreamingData...")
		fmt.Printf("   注意: 此测试需要有效的资产名称和链信息ID\n")
		readReq := &clients.StreamReadRequest{
			AssetName:   "test_asset",
			ChainInfoId: "test_chain",
			DbFields:    []string{"*"},
			RequestId:   fmt.Sprintf("read_test_%d", time.Now().Unix()),
			FileType:    pb.FileType_STREAM_ARROW,
			FilterNames: []string{},
			FilterValues: []*pb.FilterValue{},
			SortRules: []*pb.SortRule{},
			FilterOperators: []pb.FilterOperator{},
			Alias: "",
		}
		readResp, err := dataClient.ReadStreamingData(ctx, readReq)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
			fmt.Printf("   提示: 这可能是正常的，因为资产可能不存在或服务未配置\n")
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			fmt.Printf("     收到 %d 个 Arrow 响应批次\n", len(readResp))
			totalRows := int64(0)
			for i, resp := range readResp {
				if len(resp.ArrowBatch) > 0 {
					// 尝试解析 Arrow 数据统计行数
					rowCount, err := utils.CountRowsFromArrow(resp.ArrowBatch)
					if err == nil {
						totalRows += rowCount
						fmt.Printf("     批次 %d: %d 行\n", i+1, rowCount)
					} else {
						fmt.Printf("     批次 %d: %d 字节 (无法解析行数: %v)\n", i+1, len(resp.ArrowBatch), err)
					}
				}
			}
			if totalRows > 0 {
				fmt.Printf("     总行数: %d\n", totalRows)
			}
		}

		// 测试 ReadInternalData
		fmt.Println("   测试 ReadInternalData...")
		internalReadReq := &pb.InternalReadRequest{
			TableName:  "test_table",
			DbFields:   []string{"*"},
			DbName:     "test_db",
			FilterNames: []string{},
			FilterValues: []*pb.FilterValue{},
			SortRules: []*pb.SortRule{},
			FilterOperators: []pb.FilterOperator{},
			JobInstanceId: fmt.Sprintf("job_%d", time.Now().Unix()),
		}
		internalReadResp, err := dataClient.ReadInternalData(ctx, internalReadReq)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
			fmt.Printf("   提示: 这可能是正常的，因为表可能不存在\n")
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			fmt.Printf("     收到 %d 个 Arrow 响应批次\n", len(internalReadResp))
		}

		// 测试 Read（新版通用读接口）
		fmt.Println("   测试 Read (新版通用读接口)...")
		readRequest := &pb.ReadRequest{
			Columns: []string{},
			SortRules: []*pb.SortRule{},
			FilterConditions: []*pb.FilterCondition{},
			Keys: []*pb.TableKey{},
		}
		// 设置外部数据源
		readRequest.DataSource = &pb.ReadRequest_External{
			External: &pb.ExternalDataSource{
				AssetName:   "test_asset",
				ChainInfoId: "test_chain",
				Alias:       "",
			},
		}
		readNewResp, err := dataClient.Read(ctx, readRequest)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
			fmt.Printf("   提示: 这可能是正常的，因为资产可能不存在\n")
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			fmt.Printf("     收到 %d 个 Arrow 响应批次\n", len(readNewResp))
		}

		// 测试 WriteInternalData（需要创建测试 Arrow 数据）
		fmt.Println("   测试 WriteInternalData...")
		testArrowData := createTestArrowData()
		writeReq := &clients.WriteInternalDataRequest{
			ArrowBatch:    testArrowData,
			DbName:        "test_db",
			TableName:     fmt.Sprintf("test_table_%d", time.Now().Unix()),
			JobInstanceId: fmt.Sprintf("write_job_%d", time.Now().Unix()),
		}
		err = dataClient.WriteInternalData(ctx, writeReq)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
			fmt.Printf("   提示: 这可能是正常的，因为数据库可能不存在或权限不足\n")
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			fmt.Printf("     成功写入 %d 字节的 Arrow 数据\n", len(testArrowData))
		}

		// 测试 Write（新版通用写接口）
		fmt.Println("   测试 Write (新版通用写接口)...")
		writeBatches := []*pb.WriteRequest{
			{
				ArrowBatch: testArrowData,
				DbName:     "test_db",
				TableName:  fmt.Sprintf("test_table_write_%d", time.Now().Unix()),
			},
		}
		writeResp, err := dataClient.Write(ctx, writeBatches)
		if err != nil {
			fmt.Printf("   ✗ 调用失败: %v\n", err)
			fmt.Printf("   提示: 这可能是正常的，因为数据库可能不存在或权限不足\n")
		} else {
			fmt.Printf("   ✓ 调用成功\n")
			if writeResp != nil {
				fmt.Printf("     成功: %v\n", writeResp.Success)
				fmt.Printf("     消息: %s\n", writeResp.Message)
			}
		}
	}
	fmt.Println()

	fmt.Println("========== 测试完成 ==========")
}

// createTestArrowData 创建测试用的 Arrow 数据
func createTestArrowData() []byte {
	pool := memory.NewGoAllocator()

	// 创建 schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// 创建数据
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// 添加数据
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie", "David", "Eve"}, nil)
	builder.Field(2).(*array.Int32Builder).AppendValues([]int32{25, 30, 35, 28, 32}, nil)
	builder.Field(3).(*array.Float64Builder).AppendValues([]float64{95.5, 87.0, 92.5, 89.0, 91.5}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// 序列化为字节
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	defer writer.Close()

	if err := writer.Write(record); err != nil {
		log.Fatalf("写入 Arrow 数据失败: %v", err)
	}

	return buf.Bytes()
}

