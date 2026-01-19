package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	
	pb "mira-gateway-mock/mirapb"
)

// BaseRequest 基础请求
type BaseRequest struct {
	RequestId   string `json:"requestId"`
	ChainInfoId string `json:"chainInfoId"`
	Alias       string `json:"alias"`
	Address     string `json:"address"`
}

// BaseResponse 基础响应
type BaseResponse struct {
	Code int32  `json:"code"`
	Msg  string `json:"msg"`
}

// GetPrivateDBConnInfoRequest 获取数据库连接信息请求
type GetPrivateDBConnInfoRequest struct {
	RequestId string `json:"requestId"`
	DbConnId  int32  `json:"dbConnId"`
}

// GetPrivateDBConnInfoResp 数据库连接信息
type GetPrivateDBConnInfoResp struct {
	DbConnId   int32  `json:"dbConnId"`
	ConnName   string `json:"connName"`
	Host       string `json:"host"`
	Port       int32  `json:"port"`
	Type       int32  `json:"type"` // 1.mysql, 2.kingbase, 3.llm-hub
	Username   string `json:"username"`
	Password   string `json:"password"`
	DbName     string `json:"dbName"`
	CreatedAt  string `json:"createdAt"`
	LlmHubToken string `json:"llmHubToken,omitempty"`
}

// GetPrivateDBConnInfoResponse 获取数据库连接信息响应
type GetPrivateDBConnInfoResponse struct {
	BaseResponse BaseResponse          `json:"baseResponse"`
	Data         GetPrivateDBConnInfoResp `json:"data"`
}

// GetPrivateAssetInfoByEnNameRequest 通过资产英文名称获取资产详情请求
type GetPrivateAssetInfoByEnNameRequest struct {
	BaseRequest BaseRequest `json:"baseRequest"`
	AssetEnName string      `json:"assetEnName"`
}

// SaveTableColumnItem 数据表字段信息
// 注意：JSON 标签需要匹配 data-service 的期望格式（下划线格式）
type SaveTableColumnItem struct {
	Name        string `json:"name"`
	DataType    string `json:"data_type"`
	DataLength  int32  `json:"data_length"`
	Description string `json:"description"`
	IsPrimaryKey int32 `json:"is_primary_key"`
	PrivacyQuery int32 `json:"privacy_query"`
}

// DataInfo 数据库信息
type DataInfo struct {
	DbName       string                `json:"dbName"`
	TableName    string                `json:"tableName"`
	ItemList     []SaveTableColumnItem `json:"itemList"`
	DataSourceId int32                 `json:"dataSourceId"`
}

// AssetInfo 资产信息
type AssetInfo struct {
	AssetId      string   `json:"assetId"` // 修改为字符串类型，匹配 data-service 的期望
	AssetNumber  string   `json:"assetNumber"`
	AssetName    string   `json:"assetName"`
	AssetEnName  string   `json:"assetEnName"`
	AssetType    int32    `json:"assetType"` // 1-库表, 2-文件
	DataInfo     *DataInfo `json:"dataInfo"` // 嵌套的 DataInfo 结构
	DataProductType int32 `json:"dataProductType,omitempty"`
}

// GetPrivateAssetInfoByEnNameResponse 通过资产英文名称获取资产详情响应
type GetPrivateAssetInfoByEnNameResponse struct {
	BaseResponse BaseResponse `json:"baseResponse"`
	Data         AssetInfo    `json:"data"`
}

// GetResultStorageConfigRequest 获取结果存储配置请求
type GetResultStorageConfigRequest struct {
	JobInstanceId string `json:"jobInstanceId"`
	ParticipantId string `json:"participantId"`
}

// ResultStorageConfig 结果存储配置
type ResultStorageConfig struct {
	ResultStorageType int32  `json:"resultStorageType"` // 0-未知(存储到MINIO), 1-mysql, 2-kingbase等
	Host              string `json:"host"`
	Port              int32  `json:"port"`
	User              string `json:"user"`
	Password          string `json:"password"`
	Db                string `json:"db"`
	TlsConfig         *TlsConfig `json:"tlsConfig,omitempty"`
}

// TlsConfig TLS配置
type TlsConfig struct {
	UseTls     bool   `json:"useTls"`
	Mode       string `json:"mode"`
	ServerName string `json:"serverName"`
	CaCert     string `json:"caCert"`
	ClientCert string `json:"clientCert"`
	ClientKey  string `json:"clientKey"`
}

// GetResultStorageConfigResponse 获取结果存储配置响应
type GetResultStorageConfigResponse struct {
	BaseResponse BaseResponse        `json:"baseResponse"`
	Data         ResultStorageConfig `json:"data"`
}

// PushJobResultRequest 推送作业结果请求
type PushJobResultRequest struct {
	ObjectName string `json:"object_name"`
}

// IDAServiceClient IDA服务客户端
var idaClient pb.MiraIdaAccessClient
var idaConn *grpc.ClientConn

// initIDAClient 初始化IDA服务客户端
func initIDAClient() error {
	idaHost := os.Getenv("IDA_SERVICE_HOST")
	if idaHost == "" {
		idaHost = "ida-access-service-mock"
	}
	
	idaPort := os.Getenv("IDA_SERVICE_PORT")
	if idaPort == "" {
		idaPort = "9091"
	}
	
	addr := fmt.Sprintf("%s:%s", idaHost, idaPort)
	log.Printf("连接IDA服务: %s", addr)
	
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("连接IDA服务失败: %v", err)
	}
	
	idaConn = conn
	idaClient = pb.NewMiraIdaAccessClient(conn)
	log.Printf("IDA服务客户端初始化成功")
	
	return nil
}

func main() {
	// 初始化IDA服务客户端
	if err := initIDAClient(); err != nil {
		log.Fatalf("初始化IDA服务客户端失败: %v", err)
	}
	defer idaConn.Close()
	// 设置Gin为发布模式
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()

	// 添加CORS支持
	router.Use(corsMiddleware())

	// 注册路由
	v1 := router.Group("/v1")
	{
		v1.POST("/GetPrivateDBConnInfo", handleGetPrivateDBConnInfo)
		v1.POST("/GetPrivateAssetInfoByEnName", handleGetPrivateAssetInfoByEnName)
		v1.POST("/GetResultStorageConfig", handleGetResultStorageConfig)
		v1.POST("/mira/async/notify", handlePushJobResult)
	}

	// 健康检查
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// 获取端口，默认8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Mira Gateway Mock服务启动在端口: %s", port)
	log.Printf("健康检查: http://localhost:%s/health", port)
	log.Printf("API端点: http://localhost:%s/v1/*", port)

	if err := router.Run(":" + port); err != nil {
		log.Fatalf("启动服务器失败: %v", err)
	}
}

// CORS中间件
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, X-Space-Id, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

// handleGetPrivateDBConnInfo 处理获取数据库连接信息请求
func handleGetPrivateDBConnInfo(c *gin.Context) {
	var req GetPrivateDBConnInfoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	log.Printf("收到GetPrivateDBConnInfo请求: RequestId=%s, DbConnId=%d", req.RequestId, req.DbConnId)

	// 调用IDA服务的gRPC接口
	ctx := context.Background()
	protoReq := &pb.GetPrivateDBConnInfoRequest{
		RequestId: req.RequestId,
		DbConnId:  req.DbConnId,
	}
	
	protoResp, err := idaClient.GetPrivateDBConnInfo(ctx, protoReq)
	if err != nil {
		log.Printf("调用IDA服务失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"baseResponse": BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("调用IDA服务失败: %v", err),
			},
		})
		return
	}
	
	// 转换响应
	resp := GetPrivateDBConnInfoResponse{
		BaseResponse: BaseResponse{
			Code: protoResp.BaseResponse.Code,
			Msg:  protoResp.BaseResponse.Msg,
		},
	}
	
	if protoResp.Data != nil {
		resp.Data = GetPrivateDBConnInfoResp{
			DbConnId:    protoResp.Data.DbConnId,
			ConnName:    protoResp.Data.ConnName,
			Host:        protoResp.Data.Host,
			Port:        protoResp.Data.Port,
			Type:        protoResp.Data.Type,
			Username:    protoResp.Data.Username,
			Password:    protoResp.Data.Password,
			DbName:      protoResp.Data.DbName,
			CreatedAt:   protoResp.Data.CreatedAt,
			LlmHubToken: protoResp.Data.LlmHubToken,
		}
	}

	c.JSON(http.StatusOK, resp)
}

// handleGetPrivateAssetInfoByEnName 处理通过资产英文名称获取资产详情请求
func handleGetPrivateAssetInfoByEnName(c *gin.Context) {
	var req GetPrivateAssetInfoByEnNameRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	log.Printf("收到GetPrivateAssetInfoByEnName请求: RequestId=%s, AssetEnName=%s", 
		req.BaseRequest.RequestId, req.AssetEnName)

	// 调用IDA服务的gRPC接口
	ctx := context.Background()
	protoReq := &pb.GetPrivateAssetInfoByEnNameRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId:   req.BaseRequest.RequestId,
			ChainInfoId: req.BaseRequest.ChainInfoId,
			Alias:       req.BaseRequest.Alias,
			Address:     req.BaseRequest.Address,
		},
		AssetEnName: req.AssetEnName,
	}
	
	protoResp, err := idaClient.GetPrivateAssetInfoByEnName(ctx, protoReq)
	if err != nil {
		log.Printf("调用IDA服务失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"baseResponse": BaseResponse{
				Code: 1,
				Msg:  fmt.Sprintf("调用IDA服务失败: %v", err),
			},
		})
		return
	}
	
	// 转换响应
	resp := GetPrivateAssetInfoByEnNameResponse{
		BaseResponse: BaseResponse{
			Code: protoResp.BaseResponse.Code,
			Msg:  protoResp.BaseResponse.Msg,
		},
	}
	
	if protoResp.Data != nil {
		assetInfo := AssetInfo{
			AssetId:        protoResp.Data.AssetId,
			AssetNumber:    protoResp.Data.AssetNumber,
			AssetName:      protoResp.Data.AssetName,
			AssetEnName:    protoResp.Data.AssetEnName,
			AssetType:      protoResp.Data.AssetType,
			DataProductType: int32(protoResp.Data.DataProductType),
		}
		
		// 转换DataInfo
		if protoResp.Data.DataInfo != nil {
			var itemList []SaveTableColumnItem
			for _, item := range protoResp.Data.DataInfo.ItemList {
				itemList = append(itemList, SaveTableColumnItem{
					Name:        item.Name,
					DataType:    item.DataType,
					DataLength:  item.DataLength,
					Description: item.Description,
					IsPrimaryKey: item.IsPrimaryKey,
					PrivacyQuery: item.PrivacyQuery,
				})
			}
			
			assetInfo.DataInfo = &DataInfo{
				DbName:       protoResp.Data.DataInfo.DbName,
				TableName:    protoResp.Data.DataInfo.TableName,
				ItemList:     itemList,
				DataSourceId: protoResp.Data.DataInfo.DataSourceId,
			}
		}
		
		resp.Data = assetInfo
	}

	c.JSON(http.StatusOK, resp)
}

// handleGetResultStorageConfig 处理获取结果存储配置请求
func handleGetResultStorageConfig(c *gin.Context) {
	var req GetResultStorageConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	log.Printf("收到GetResultStorageConfig请求: JobInstanceId=%s, ParticipantId=%s", 
		req.JobInstanceId, req.ParticipantId)

	// Mock数据 - 返回存储到MINIO的配置（ResultStorageType=0表示存储到MINIO）
	resp := GetResultStorageConfigResponse{
		BaseResponse: BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: ResultStorageConfig{
			ResultStorageType: 0, // 0表示存储到MINIO
			Host:              "localhost",
			Port:              3306,
			User:              "root",
			Password:          "password",
			Db:                "result_db",
		},
	}

	c.JSON(http.StatusOK, resp)
}

// handlePushJobResult 处理推送作业结果请求
func handlePushJobResult(c *gin.Context) {
	var req PushJobResultRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	log.Printf("收到PushJobResult请求: ObjectName=%s", req.ObjectName)

	// Mock响应 - 返回成功
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Job result pushed successfully",
	})
}

