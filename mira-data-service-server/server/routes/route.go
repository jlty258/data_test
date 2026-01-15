/*
*

	@author: shiliang
	@date: 2024/9/23
	@note:

*
*/
package routes

import (
	pb "data-service/generated/datasource"
	"data-service/log"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// 定义一个全局通道用于传递作业完成信息
var MinioUrlChan = make(chan struct {
	Data   map[string]interface{}
	Status pb.JobStatus
	Error  error
})

// jobCompletionHandler 处理来自客户端的作业完成请求
func jobCompletionHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Infof("Received request at /api/job/completed with method %s", r.Method)

	if r.Method == http.MethodPost {
		// 解析请求体中的 JSON 数据
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Logger.Errorf("Error reading request body: %v", err)
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		// 打印原始请求体
		log.Logger.Infof("Raw request body: %s", string(body))

		// 解析 JSON 数据
		var data map[string]interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			log.Logger.Errorf("Error parsing JSON data: %v", err)
			sendErrorToChannel(fmt.Sprintf("Error parsing JSON data: %v", err))
			http.Error(w, "Error parsing JSON data", http.StatusBadRequest)
			return
		}

		// 检查必要字段
		statusStr, statusOk := data["status"].(string)
		if !statusOk {
			log.Logger.Error("Missing 'status' parameter")
			sendErrorToChannel("Missing 'status' parameter")
			http.Error(w, "Missing 'status' parameter", http.StatusBadRequest)
			return
		}
		status := ParseJobStatus(statusStr)

		// 检查是否有错误信息
		if errorMsg, hasError := data["error"].(string); hasError {
			log.Logger.Errorf("Job completed with error: %s", errorMsg)
			sendErrorToChannel(errorMsg)
			http.Error(w, errorMsg, http.StatusInternalServerError)
			return
		}

		// 创建动态数据对象
		dynamicData := make(map[string]interface{})
		for key, value := range data {
			// 跳过 status 和 mode 字段
			if key != "status" && key != "mode" {
				dynamicData[key] = value
			}
		}

		// 发送信息到通道
		go func(dynamicData map[string]interface{}, status pb.JobStatus) {
			MinioUrlChan <- struct {
				Data   map[string]interface{}
				Status pb.JobStatus
				Error  error
			}{
				Data:   dynamicData,
				Status: status,
				Error:  nil,
			}
		}(dynamicData, status)

		// 返回成功响应
		response := map[string]interface{}{
			"success": true,
			"message": "Job completed successfully",
			"data":    dynamicData,
			"status":  statusStr,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	} else {
		log.Logger.Errorf("Invalid request method: %s", r.Method)
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

// RegisterRoutes 注册所有的 HTTP 路由
func RegisterRoutes() {
	http.HandleFunc("/api/job/completed", jobCompletionHandler)
}

// sendErrorToChannel 将错误信息发送到 MinioUrlChan 通道
func sendErrorToChannel(errMsg string) {
	go func() {
		MinioUrlChan <- struct {
			Data   map[string]interface{}
			Status pb.JobStatus
			Error  error
		}{
			Status: pb.JobStatus_JOB_STATUS_FAILED,
			Error:  fmt.Errorf(errMsg),
		}
	}()
}

// 字符串转 JobStatus 枚举
func ParseJobStatus(status string) pb.JobStatus {
	switch status {
	case "pending":
		return pb.JobStatus_JOB_STATUS_PENDING
	case "running":
		return pb.JobStatus_JOB_STATUS_RUNNING
	case "succeeded", "success", "completed":
		return pb.JobStatus_JOB_STATUS_SUCCEEDED
	case "failed", "error":
		return pb.JobStatus_JOB_STATUS_FAILED
	default:
		return pb.JobStatus_JOB_STATUS_UNKNOWN
	}
}
