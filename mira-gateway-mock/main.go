package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
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

// AssetInfo 资产信息
type AssetInfo struct {
	AssetId      int32  `json:"assetId"`
	AssetNumber  string `json:"assetNumber"`
	AssetName    string `json:"assetName"`
	AssetEnName  string `json:"assetEnName"`
	Type         int32  `json:"type"` // 1-库表, 2-文件
	Host         string `json:"host"`
	Port         int32  `json:"port"`
	DbName       string `json:"dbName"`
	TableName    string `json:"tableName"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	DbType       int32  `json:"dbType"`
	Columns      string `json:"columns"`
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

func main() {
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

	// Mock数据 - 返回一个MySQL数据库连接信息
	resp := GetPrivateDBConnInfoResponse{
		BaseResponse: BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: GetPrivateDBConnInfoResp{
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

	// Mock数据 - 返回一个资产信息
	resp := GetPrivateAssetInfoByEnNameResponse{
		BaseResponse: BaseResponse{
			Code: 0,
			Msg:  "success",
		},
		Data: AssetInfo{
			AssetId:     1,
			AssetNumber: "ASSET001",
			AssetName:   "测试资产",
			AssetEnName: req.AssetEnName,
			Type:        1, // 库表
			Host:        "localhost",
			Port:        3306,
			DbName:      "test_db",
			TableName:   "test_table",
			Username:    "root",
			Password:    "password",
			DbType:      1, // MySQL
			Columns:     `[{"name":"id","type":"int"},{"name":"name","type":"varchar"}]`,
		},
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

