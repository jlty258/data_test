package models

import (
	"data-service/common"
	"time"
)

// CleanupTask 清理任务模型
type CleanupTask struct {
	ID            uint              `gorm:"primarykey"`
	JobInstanceID string            `gorm:"uniqueIndex;size:255;not null;comment:作业实例ID"`
	TaskType      string            `gorm:"not null;comment:任务类型(doris_table, mira_table, etc)"`
	Status        common.TaskStatus `gorm:"not null;default:'pending';comment:任务状态(pending, running, completed, failed)"`
	TablesFound   int               `gorm:"default:0;comment:找到的表数量"`
	TablesDropped int               `gorm:"default:0;comment:成功删除的表数量"`
	ErrorMessage  string            `gorm:"type:text;comment:错误信息"`
	StartedAt     *time.Time        `gorm:"comment:开始时间"`
	CompletedAt   *time.Time        `gorm:"comment:完成时间"`
	RetryCount    int               `gorm:"default:0;comment:重试次数"`
	MaxRetries    int               `gorm:"default:3;comment:最大重试次数"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (CleanupTask) TableName() string {
	return "t_data_service_cleanup_tasks"
}
