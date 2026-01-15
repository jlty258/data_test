package common

import (
	"fmt"
	"strconv"
	"time"
)

// DorisExportTaskStatus Doris导出任务状态枚举
type DorisExportTaskStatus string

const (
	ExportStatusPending   DorisExportTaskStatus = "PENDING"   // 待执行
	ExportStatusRunning   DorisExportTaskStatus = "RUNNING"   // 执行中
	ExportStatusFinished  DorisExportTaskStatus = "FINISHED"  // 已完成
	ExportStatusFailed    DorisExportTaskStatus = "FAILED"    // 失败
	ExportStatusCancelled DorisExportTaskStatus = "CANCELLED" // 已取消
)

// String 实现Stringer接口
func (s DorisExportTaskStatus) String() string {
	return string(s)
}

// IsValid 检查状态是否有效
func (s DorisExportTaskStatus) IsValid() bool {
	switch s {
	case ExportStatusPending, ExportStatusRunning, ExportStatusFinished, ExportStatusFailed, ExportStatusCancelled:
		return true
	default:
		return false
	}
}

// IsCompleted 检查任务是否已完成（无论成功或失败）
func (s DorisExportTaskStatus) IsCompleted() bool {
	return s == ExportStatusFinished || s == ExportStatusFailed || s == ExportStatusCancelled
}

// IsSuccessful 检查任务是否成功完成
func (s DorisExportTaskStatus) IsSuccessful() bool {
	return s == ExportStatusFinished
}

// ExportTaskInfo Doris导出任务信息
type ExportTaskInfo struct {
	JobId        string                `json:"jobId"`        // 任务ID
	Label        string                `json:"label"`        // 任务标签
	State        DorisExportTaskStatus `json:"state"`        // 任务状态
	Progress     string                `json:"progress"`     // 进度百分比
	TaskInfo     string                `json:"taskInfo"`     // 任务详细信息
	ErrorMsg     string                `json:"errorMsg"`     // 错误信息
	CreateTime   *time.Time            `json:"createTime"`   // 任务创建时间
	FinishTime   *time.Time            `json:"finishTime"`   // 任务完成时间
	ExportPath   string                `json:"exportPath"`   // 导出路径
	ExportedRows int64                 `json:"exportedRows"` // 导出行数
	FileSize     string                `json:"fileSize"`     // 文件大小
}

// GetExportTaskStatuses 获取所有有效的导出任务状态
func GetExportTaskStatuses() []DorisExportTaskStatus {
	return []DorisExportTaskStatus{
		ExportStatusPending,
		ExportStatusRunning,
		ExportStatusFinished,
		ExportStatusFailed,
		ExportStatusCancelled,
	}
}

// ParseDorisExportTaskStatus 解析字符串为导出任务状态
func ParseDorisExportTaskStatus(status string) DorisExportTaskStatus {
	switch status {
	case "PENDING":
		return ExportStatusPending
	case "RUNNING":
		return ExportStatusRunning
	case "FINISHED":
		return ExportStatusFinished
	case "FAILED":
		return ExportStatusFailed
	case "CANCELLED":
		return ExportStatusCancelled
	default:
		return DorisExportTaskStatus(status) // 返回原始值，便于调试
	}
}

// ToExportTaskInfo 将map[string]interface{}转换为ExportTaskInfo结构体
func ToExportTaskInfo(data map[string]interface{}) *ExportTaskInfo {
	if data == nil {
		return nil
	}

	info := &ExportTaskInfo{}

	if v, ok := data["JobId"]; ok && v != nil {
		info.JobId = toString(v)
	}
	if v, ok := data["Label"]; ok && v != nil {
		info.Label = toString(v)
	}
	if v, ok := data["State"]; ok && v != nil {
		info.State = ParseDorisExportTaskStatus(toString(v))
	}
	if v, ok := data["Progress"]; ok && v != nil {
		info.Progress = toString(v)
	}
	if v, ok := data["TaskInfo"]; ok && v != nil {
		info.TaskInfo = toString(v)
	}
	if v, ok := data["ErrorMsg"]; ok && v != nil {
		info.ErrorMsg = toString(v)
	}
	if v, ok := data["CreateTime"]; ok && v != nil {
		if t, err := parseTime(toString(v)); err == nil {
			info.CreateTime = t
		}
	}
	if v, ok := data["FinishTime"]; ok && v != nil {
		if t, err := parseTime(toString(v)); err == nil {
			info.FinishTime = t
		}
	}
	if v, ok := data["ExportPath"]; ok && v != nil {
		info.ExportPath = toString(v)
	}
	if v, ok := data["ExportedRows"]; ok && v != nil {
		info.ExportedRows = toInt64(v)
	}
	if v, ok := data["FileSize"]; ok && v != nil {
		info.FileSize = toString(v)
	}

	return info
}

// 辅助函数：转换为字符串
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// 辅助函数：转换为int64
func toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case string:
		if num, err := strconv.ParseInt(val, 10, 64); err == nil {
			return num
		}
	case []byte:
		if num, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return num
		}
	}
	return 0
}

// 辅助函数：解析时间
func parseTime(timeStr string) (*time.Time, error) {
	if timeStr == "" {
		return nil, fmt.Errorf("empty time string")
	}

	// 尝试不同的时间格式
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return &t, nil
		}
	}

	return nil, fmt.Errorf("unable to parse time: %s", timeStr)
}
