/*
*

	@author: shiliang
	@date: 2024/12/23
	@note: http请求测试

*
*/
package routes

import (
	"data-service/log"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	pb "data-service/generated/datasource"
)

func setup() {
	viper.Set("LoggerConfig.Level", "debug") // 模拟配置项
	viper.Set("TestConfig", "true")
	viper.AutomaticEnv()        // 启用自动环境变量加载（如果有需要）
	viper.SetConfigType("yaml") // 确保配置类型是 yaml（如果依赖 yaml 类型）
	viper.SetConfigFile("")     // 禁用文件读取，避免默认路径读取配置文件

	log.InitLogger()
}

func TestJobCompletionHandler_PostRequest_ValidJson_Success(t *testing.T) {
	setup()
	// 构造有效的 JSON 请求体
	body := `{
		"dbName": "test_db",
		"tableName": "test_table",
		"status": "succeeded",
		"mode": "query"
	}`

	req, err := http.NewRequest("POST", "/api/job/completed", strings.NewReader(body))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()

	// 启动 goroutine 监听 MinioUrlChan，断言内容
	done := make(chan struct{})
	go func() {
		result := <-MinioUrlChan
		assert.Equal(t, "test_db", result.Data["dbName"])
		assert.Equal(t, "test_table", result.Data["tableName"])
		assert.Equal(t, pb.JobStatus_JOB_STATUS_SUCCEEDED, result.Status)
		assert.Nil(t, result.Error)
		close(done)
	}()

	// 调用 handler
	jobCompletionHandler(rr, req)

	// 检查 HTTP 响应
	assert.Equal(t, http.StatusOK, rr.Code)
	var response map[string]interface{}
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, true, response["success"])
	assert.Equal(t, "Job completed successfully", response["message"])
	assert.Equal(t, "succeeded", response["status"])

	// 等待 goroutine 校验完成
	<-done
}
