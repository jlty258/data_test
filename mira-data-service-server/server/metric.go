package main

import (
	"data-service/config"
	log "data-service/log"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	ginmetrics "github.com/penglongli/gin-metrics/ginmetrics"
)

const (
	labelServiceName = "service_name"
	labelSuccess     = "success"
	labelHandler     = "handler"

	serviceNameValue = "data-service"
)

var (
	// 使用Once确保只初始化一次
	initMetricsOnce sync.Once
	M               = ginmetrics.GetMonitor()
)

// getMetrics 返回本项目核心指标定义
func getMetrics() []*ginmetrics.Metric {
	return []*ginmetrics.Metric{
		// 导入接口（ImportData）
		{
			Type:        ginmetrics.Counter,
			Name:        "import_requests_total",
			Description: "Total number of import data requests",
			Labels:      []string{labelServiceName, labelHandler, labelSuccess},
		},
		{
			Type:        ginmetrics.Gauge,
			Name:        "import_duration_seconds",
			Description: "Duration of import data processing in seconds (last request)",
			Labels:      []string{labelServiceName, labelHandler, labelSuccess},
		},
	}
}

// MonitorMetric 启动监控服务（带 HTTP /metrics 端点和 /health 健康检查）
func MonitorMetric() {
	conf := config.GetConfigMap()
	if !conf.CommonConfig.MonitorEnabled {
		log.Logger.Info("Monitor is disabled")
		return
	}

	initMetricsOnce.Do(func() {
		monitor := InitMonitorRouter()

		// 配置 gin-metrics
		M.SetMetricPath("/metrics")
		M.SetSlowTime(10)
		M.SetDuration([]float64{0.1, 0.3, 1.2, 5, 10})

		// 注册自定义指标
		for _, metric := range getMetrics() {
			if err := M.AddMetric(metric); err != nil {
				log.Logger.Errorf("Failed to add metric %s: %v", metric.Name, err)
				return
			}
		}

		// 使用 gin-metrics 中间件
		M.Use(monitor)

		// 启动监控服务
		endPoint := fmt.Sprintf(":%d", conf.CommonConfig.MonitorPort)
		go func() {
			log.Logger.Infof("Starting monitor server on %s", endPoint)
			if err := monitor.Run(endPoint); err != nil {
				log.Logger.Errorf("Failed to start monitor server: %v", err)
			}
		}()
	})
}

// InitMonitorRouter 初始化监控路由（健康检查 + metrics）
func InitMonitorRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// 健康检查
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	return r
}

// ObserveDuration 记录某操作耗时（以秒为单位）并计数
// handler: 逻辑名，例如 "ProcessWriteStream" / "ProcessReadRequest" / "ExportCsv"
func ObserveDuration(handler string, start time.Time, success bool) {
	durSec := time.Since(start).Seconds()
	successStr := fmt.Sprintf("%t", success)
	labels := []string{serviceNameValue, handler, successStr}

	// 只处理 ImportData
	if handler == "ImportData" {
		if m := M.GetMetric("import_requests_total"); m != nil {
			m.Inc(labels)
		}
		if m := M.GetMetric("import_duration_seconds"); m != nil {
			m.SetGaugeValue(labels, durSec)
		}
	}
}
