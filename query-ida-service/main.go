package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	
	// 使用 data-integrate-test 的 proto 生成代码
	pb "data-integrate-test/generated/datasource"
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 注意：这里需要使用正确的 proto 包
	// 由于 proto 文件可能还没有编译，我们先使用 HTTP 方式查询
	fmt.Printf("正在连接 IDA Service: %s\n", addr)
	fmt.Printf("注意：如果 proto 未编译，请使用 HTTP 方式查询\n\n")
	
	// 使用 HTTP 方式查询
	queryViaHTTP(idaHost, idaPort)
}

func queryViaHTTP(host string, port int) {
	baseURL := fmt.Sprintf("http://%s:%d/v1", host, port)
	
	fmt.Println("========== 查询数据源和资产信息 ==========")
	fmt.Printf("IDA Service 地址: %s\n\n", baseURL)
	
	// 查询资产列表
	fmt.Println("1. 查询资产列表...")
	assetListURL := fmt.Sprintf("%s/GetPrivateAssetList", baseURL)
	fmt.Printf("   URL: %s\n", assetListURL)
	fmt.Printf("   请求: POST\n")
	fmt.Printf("   请求体示例:\n")
	requestBody := map[string]interface{}{
		"baseRequest": map[string]string{
			"requestId": fmt.Sprintf("query_%d", time.Now().Unix()),
		},
		"pageNumber": 1,
		"pageSize":   100,
		"filters":    []interface{}{},
	}
	jsonBody, _ := json.MarshalIndent(requestBody, "   ", "  ")
	fmt.Printf("   %s\n\n", string(jsonBody))
	
	// 查询数据源信息（需要知道数据源ID）
	fmt.Println("2. 查询数据源信息...")
	fmt.Printf("   接口: GetPrivateDBConnInfo\n")
	fmt.Printf("   URL: %s/GetPrivateDBConnInfo\n", baseURL)
	fmt.Printf("   请求体示例:\n")
	dsRequestBody := map[string]interface{}{
		"requestId": fmt.Sprintf("query_%d", time.Now().Unix()),
		"dbConnId":  1, // 需要替换为实际的数据源ID
	}
	dsJsonBody, _ := json.MarshalIndent(dsRequestBody, "   ", "  ")
	fmt.Printf("   %s\n\n", string(dsJsonBody))
	
	fmt.Println("========== 使用 curl 命令查询 ==========")
	fmt.Printf("\n# 查询资产列表\n")
	fmt.Printf("curl -X POST %s/GetPrivateAssetList \\\n", baseURL)
	fmt.Printf("  -H \"Content-Type: application/json\" \\\n")
	fmt.Printf("  -d '%s'\n\n", string(jsonBody))
	
	fmt.Printf("# 查询数据源信息（替换 dbConnId 为实际值）\n")
	fmt.Printf("curl -X POST %s/GetPrivateDBConnInfo \\\n", baseURL)
	fmt.Printf("  -H \"Content-Type: application/json\" \\\n")
	fmt.Printf("  -d '%s'\n\n", string(dsJsonBody))
}

