package repositories

import (
	"data-service/common"
	"data-service/database/gorm/models"
	"time"

	"gorm.io/gorm"
)

type CleanupTaskRepository struct {
	db *gorm.DB
}

func NewCleanupTaskRepository(db *gorm.DB) *CleanupTaskRepository {
	return &CleanupTaskRepository{db: db}
}

// Create 创建清理任务
func (r *CleanupTaskRepository) Create(task *models.CleanupTask) error {
	return r.db.Create(task).Error
}

// FindByJobInstanceID 根据作业实例ID查找任务
func (r *CleanupTaskRepository) FindByJobInstanceID(jobInstanceID string) (*models.CleanupTask, error) {
	var task models.CleanupTask
	err := r.db.Where("job_instance_id = ?", jobInstanceID).First(&task).Error
	return &task, err
}

// UpdateStatus 更新任务状态
func (r *CleanupTaskRepository) UpdateStatus(taskID uint, status common.TaskStatus, errorMsg string) error {
	updates := map[string]interface{}{
		"status":     status,
		"updated_at": time.Now(),
	}

	switch status {
	case common.TaskStatusRunning:
		now := time.Now()
		updates["started_at"] = &now
	case common.TaskStatusCompleted, common.TaskStatusFailed:
		now := time.Now()
		updates["completed_at"] = &now
	}

	if errorMsg != "" {
		updates["error_message"] = errorMsg
	}

	return r.db.Model(&models.CleanupTask{}).Where("id = ?", taskID).Updates(updates).Error
}

// IncrementRetryCount 增加重试次数
func (r *CleanupTaskRepository) IncrementRetryCount(taskID uint) error {
	return r.db.Model(&models.CleanupTask{}).Where("id = ?", taskID).
		UpdateColumn("retry_count", gorm.Expr("retry_count + 1")).Error
}

// FindPendingTasks 查找待处理的任务
func (r *CleanupTaskRepository) FindPendingTasks() ([]models.CleanupTask, error) {
	var tasks []models.CleanupTask
	err := r.db.Where("status = ? AND retry_count < max_retries", common.TaskStatusPending).Find(&tasks).Error
	return tasks, err
}

// UpdateTaskProgress 更新任务进度
func (r *CleanupTaskRepository) UpdateTaskProgress(taskID uint, tablesFound, tablesDropped int) error {
	return r.db.Model(&models.CleanupTask{}).Where("id = ?", taskID).
		Updates(map[string]interface{}{
			"tables_found":   tablesFound,
			"tables_dropped": tablesDropped,
			"updated_at":     time.Now(),
		}).Error
}

// FindRetryTasksWithPagination 分页查找需要重新执行的任务
func (r *CleanupTaskRepository) FindRetryTasksWithPagination(offset, limit int) ([]models.CleanupTask, error) {
	var tasks []models.CleanupTask

	// 条件：
	// 1. retry_count > 0（已执行过或重启前的任务）
	// 2. 状态为failed或pending
	// 3. 排除最近2分钟内更新过的任务（避免重复处理）
	twoMinutesAgo := time.Now().Add(-2 * time.Minute)

	err := r.db.Where("retry_count > 0 AND (status = ? OR status = ?) AND updated_at < ?",
		common.TaskStatusFailed, common.TaskStatusPending, twoMinutesAgo).
		Order("updated_at ASC"). // 按更新时间升序，优先处理较早的任务
		Offset(offset).
		Limit(limit).
		Find(&tasks).Error
	return tasks, err
}

// CountRetryTasks 统计需要重新执行的任务数量
func (r *CleanupTaskRepository) CountRetryTasks(twoMinutesAgo time.Time) (int64, error) {
	var totalCount int64
	err := r.db.Model(&models.CleanupTask{}).
		Where("retry_count > 0 AND (status = ? OR status = ?) AND updated_at < ?",
			common.TaskStatusFailed, common.TaskStatusPending, twoMinutesAgo).
		Count(&totalCount).Error
	return totalCount, err
}

// FindPendingTasksWithPagination 分页查找状态为pending的任务
func (r *CleanupTaskRepository) FindPendingTasksWithPagination(offset, limit int) ([]models.CleanupTask, error) {
	var tasks []models.CleanupTask
	err := r.db.Where("status = ?", common.TaskStatusPending).
		Order("created_at ASC"). // 按创建时间升序，优先处理较早的任务
		Offset(offset).
		Limit(limit).
		Find(&tasks).Error
	return tasks, err
}

// CountPendingTasks 统计状态为pending的任务数量
func (r *CleanupTaskRepository) CountPendingTasks() (int64, error) {
	var totalCount int64
	err := r.db.Model(&models.CleanupTask{}).
		Where("status = ?", common.TaskStatusPending).
		Count(&totalCount).Error
	return totalCount, err
}
