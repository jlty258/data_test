package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"data-service/log"

	pb "chainweaver.org.cn/chainweaver/mira/mira-ida-access-service/pb/mirapb"
)

// MiraGatewayClient Mira网关客户端
type MiraGatewayClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// miraGatewayInstance 单例
var miraGatewayInstance *MiraGatewayClient
var miraGatewayOnce sync.Once

// GetMiraGateway 获取Mira网关客户端单例
func GetMiraGateway() *MiraGatewayClient {
	miraGatewayOnce.Do(func() {
		IDAServerAddress := os.Getenv("MIRA_GATEWAY_HOST")
		IDAServerPort := os.Getenv("MIRA_GATEWAY_HOST_PORT")
		baseURL := fmt.Sprintf("http://%s:%s/v1", IDAServerAddress, IDAServerPort)

		miraGatewayInstance = &MiraGatewayClient{
			BaseURL: baseURL,
			HTTPClient: &http.Client{
				Timeout: 1 * time.Hour,
			},
		}

		log.Logger.Infof("Mira Gateway client initialized with base URL: %s", baseURL)
	})

	return miraGatewayInstance
}

// GetPrivateAssetInfoByEnName 通过资产英文名称获取资产详情
func (c *MiraGatewayClient) GetPrivateAssetInfoByEnName(req *pb.GetPrivateAssetInfoByEnNameRequest) (*pb.GetPrivateAssetInfoByEnNameResponse, error) {
	url := c.BaseURL + "/GetPrivateAssetInfoByEnName"

	// 将请求转换为JSON
	reqData, err := json.Marshal(req)
	if err != nil {
		log.Logger.Errorf("Failed to marshal request: %v", err)
		return nil, err
	}

	// 创建HTTP请求
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(reqData))
	if err != nil {
		log.Logger.Errorf("Failed to create HTTP request: %v", err)
		return nil, err
	}

	// 设置请求头
	httpReq.Header.Set("Content-Type", "application/json")
	// 添加 X-Space-Id 头
	if req.BaseRequest != nil && req.BaseRequest.ChainInfoId != "" {
		httpReq.Header.Set("X-Space-Id", req.BaseRequest.ChainInfoId)
	}

	// 发送请求
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		log.Logger.Errorf("Failed to send HTTP request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		// 读取响应体
		bodyBytes, _ := io.ReadAll(resp.Body)
		bodyStr := string(bodyBytes)

		// 获取 requestId
		requestId := ""
		if req.BaseRequest != nil {
			requestId = req.BaseRequest.RequestId
		}

		// 打印完整响应信息
		log.Logger.Infof("Request data: %s, RequestId: %s", string(reqData), req.BaseRequest.RequestId)
		log.Logger.Errorf("HTTP request failed with status code: %d, RequestId=%s, Response: %+v, Body: %s",
			resp.StatusCode, requestId, resp, bodyStr)

		return nil, fmt.Errorf("HTTP request failed with status code: %d, RequestId=%s",
			resp.StatusCode, requestId)
	}

	// 读取响应体
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Logger.Errorf("Failed to read response body: %v", err)
		return nil, err
	}

	// 先解析为 map
	var rawMap map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &rawMap); err != nil {
		log.Logger.Errorf("Failed to unmarshal to map: %v", err)
		return nil, err
	}

	// 从 map 中删除 dataProductType 字段
	if dataMap, ok := rawMap["data"].(map[string]interface{}); ok {
		delete(dataMap, "dataProductType")
	}

	// 重新编码为 JSON
	modifiedJSON, err := json.Marshal(rawMap)
	if err != nil {
		log.Logger.Errorf("Failed to marshal modified map: %v", err)
		return nil, err
	}

	// 解析修改后的 JSON
	var response pb.GetPrivateAssetInfoByEnNameResponse
	if err := json.Unmarshal(modifiedJSON, &response); err != nil {
		log.Logger.Errorf("Failed to unmarshal modified JSON: %v", err)
		return nil, err
	}

	return &response, nil
}

// GetResultStorageConfig 获取结果存储配置
func (c *MiraGatewayClient) GetResultStorageConfig(req *pb.GetResultStorageConfigRequest) (*pb.GetResultStorageConfigResponse, error) {
	url := c.BaseURL + "/GetResultStorageConfig"

	// 将请求转换为JSON
	reqData, err := json.Marshal(req)
	if err != nil {
		log.Logger.Errorf("Failed to marshal request: %v", err)
		return nil, err
	}

	// 创建HTTP请求
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(reqData))
	if err != nil {
		log.Logger.Errorf("Failed to create HTTP request: %v", err)
		return nil, err
	}

	// 设置请求头
	httpReq.Header.Set("Content-Type", "application/json")

	// 发送请求
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		log.Logger.Errorf("Failed to send HTTP request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	// 读取响应体
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Logger.Errorf("Failed to read response body: %v", err)
		return nil, err
	}

	// 解析响应
	var response pb.GetResultStorageConfigResponse
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		log.Logger.Errorf("Failed to unmarshal response: %v", err)
		return nil, err
	}

	return &response, nil
}

// GetPrivateDBConnInfo 获取数据库连接信息
func (c *MiraGatewayClient) GetPrivateDBConnInfo(req *pb.GetPrivateDBConnInfoRequest) (*pb.GetPrivateDBConnInfoResponse, error) {
	url := c.BaseURL + "/GetPrivateDBConnInfo"

	reqData, err := json.Marshal(req)
	if err != nil {
		log.Logger.Errorf("Failed to marshal request: %v", err)
		return nil, err
	}

	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(reqData))
	if err != nil {
		log.Logger.Errorf("Failed to create HTTP request: %v", err)
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		log.Logger.Errorf("Failed to send HTTP request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Logger.Errorf("Failed to read response body: %v", err)
		return nil, err
	}

	var response pb.GetPrivateDBConnInfoResponse
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		log.Logger.Errorf("Failed to unmarshal response: %v", err)
		return nil, err
	}

	return &response, nil
}

// 给mira-gateway发送计算结果推送的请求
func (c *MiraGatewayClient) PushJobResult(chainInfoId, jobInstanceId string, partyId string, dataId string) error {
	url := c.BaseURL + "/mira/async/notify"

	objectName := chainInfoId + "/" + jobInstanceId + "/" + partyId + "/" + dataId
	reqData, err := json.Marshal(map[string]interface{}{
		"object_name": objectName,
	})
	if err != nil {
		log.Logger.Errorf("Failed to marshal request: %v", err)
		return err
	}

	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(reqData))
	if err != nil {
		log.Logger.Errorf("Failed to create HTTP request: %v", err)
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		log.Logger.Errorf("Failed to send HTTP request: %v", err)
		return err
	}
	defer resp.Body.Close()

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	return nil
}
