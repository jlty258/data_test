package common

// TaskStatus 任务状态枚举
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"   // 待执行
	TaskStatusRunning   TaskStatus = "running"   // 执行中
	TaskStatusCompleted TaskStatus = "completed" // 已完成
	TaskStatusFailed    TaskStatus = "failed"    // 失败
)

// String 实现Stringer接口
func (s TaskStatus) String() string {
	return string(s)
}

// IsValid 检查状态是否有效
func (s TaskStatus) IsValid() bool {
	switch s {
	case TaskStatusPending, TaskStatusRunning, TaskStatusCompleted, TaskStatusFailed:
		return true
	default:
		return false
	}
}

// GetTaskStatusList 获取所有有效的任务状态
func GetTaskStatusList() []TaskStatus {
	return []TaskStatus{
		TaskStatusPending,
		TaskStatusRunning,
		TaskStatusCompleted,
		TaskStatusFailed,
	}
}
