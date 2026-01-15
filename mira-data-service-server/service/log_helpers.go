package service

import (
	"data-service/log"
	"data-service/utils"
	"fmt"
	"regexp"
	"strings"
)

// SourceConnLogCtx 用于记录源连接异常的上下文
type SourceConnLogCtx struct {
	TableType   string
	JdbcURL     string
	DriverURL   string
	DriverClass string
	Username    string
}

// LogSourceConnectionError 针对源连接异常的特征错误，输出更聚焦的上下文日志
func LogSourceConnectionError(err error, ctx SourceConnLogCtx) bool {
	if err == nil {
		return false
	}
	low := strings.ToLower(err.Error())
	match := strings.Contains(low, "initialize datasource failed") ||
		strings.Contains(low, "communications link failure") ||
		strings.Contains(low, "connectexception") ||
		strings.Contains(low, "connection refused") ||
		strings.Contains(low, "timeout") ||
		strings.Contains(low, "could not connect") ||
		strings.Contains(low, "the driver has not received any packets")

	if match {
		log.Logger.Errorw("Source connection error while creating JDBC resource",
			"tableType", ctx.TableType,
			"jdbcUrlPreview", utils.PreviewSQL(ctx.JdbcURL, 256),
			"driverClass", ctx.DriverClass,
			"user", ctx.Username,
			"driverUrl", ctx.DriverURL,
			"err", err.Error(),
		)
	}
	return match
}

// checkStreamloaderOutput 解析 doris-streamloader 输出，失败时返回错误
func checkStreamloaderOutput(out string) error {
	reStatus := regexp.MustCompile(`"Status"\s*:\s*"(.*?)"`)
	reMessage := regexp.MustCompile(`"Message"\s*:\s*"(.*?)"`)
	reFailLoadRows := regexp.MustCompile(`"FailLoadRows"\s*:\s*(\d+)`)
	reLoadedRows := regexp.MustCompile(`"LoadedRows"\s*:\s*(\d+)`)
	reTotalRows := regexp.MustCompile(`"TotalRows"\s*:\s*(\d+)`)

	status, message := "", ""
	if m := reStatus.FindStringSubmatch(out); len(m) > 1 {
		status = m[1]
	}
	if m := reMessage.FindStringSubmatch(out); len(m) > 1 {
		message = m[1]
	}

	// 检查业务指标：FailLoadRows > 0 或 TotalRows > 0 但 LoadedRows = 0
	failLoadRows := 0
	loadedRows := 0
	totalRows := 0

	if m := reFailLoadRows.FindStringSubmatch(out); len(m) > 1 {
		fmt.Sscanf(m[1], "%d", &failLoadRows)
	}
	if m := reLoadedRows.FindStringSubmatch(out); len(m) > 1 {
		fmt.Sscanf(m[1], "%d", &loadedRows)
	}
	if m := reTotalRows.FindStringSubmatch(out); len(m) > 1 {
		fmt.Sscanf(m[1], "%d", &totalRows)
	}

	// 业务失败：有失败行数，或总行数>0但加载行数=0
	if failLoadRows > 0 || (totalRows > 0 && loadedRows == 0) {
		return fmt.Errorf("doris-streamloader business failure: status=%s, failLoadRows=%d, loadedRows=%d, totalRows=%d, message=%s",
			status, failLoadRows, loadedRows, totalRows, message)
	}

	// 状态检查
	if strings.EqualFold(status, "success") || strings.EqualFold(status, "ok") {
		return nil
	}

	// 特殊处理：Publish Timeout 但数据已成功加载的情况
	// 如果状态是 "Publish Timeout" 且消息包含 "transaction commit successfully" 且数据已加载，视为成功
	if strings.EqualFold(status, "Publish Timeout") || strings.Contains(status, "Publish Timeout") {
		if strings.Contains(strings.ToLower(message), "transaction commit successfully") {
			log.Logger.Warnf("Doris stream load: Publish Timeout but data committed successfully. LoadedRows=%d, TotalRows=%d, message=%s",
				loadedRows, totalRows, message)
			return nil
		}
	}

	low := strings.ToLower(out)
	if status == "" && !(strings.Contains(low, "fail") || strings.Contains(low, "error")) {
		return nil
	}
	if status == "" {
		status = "Unknown"
	}
	return fmt.Errorf("doris-streamloader not success: status=%s, message=%s, output=%s", status, message, out)
}
