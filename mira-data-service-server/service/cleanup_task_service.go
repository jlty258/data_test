package service

import (
	"context"
	"data-service/config"
	"data-service/database/gorm"
	"data-service/database/gorm/models"
	"data-service/database/gorm/repositories"
	"data-service/log"
	"data-service/oss"
	"errors"
	"fmt"

	gormlib "gorm.io/gorm"

	"data-service/common"
)

type CleanupTaskService struct {
	taskRepo *repositories.CleanupTaskRepository
}

func NewCleanupTaskService() *CleanupTaskService {
	db := gorm.GetGormDB()
	return &CleanupTaskService{
		taskRepo: repositories.NewCleanupTaskRepository(db),
	}
}

// CreateCleanupTask 创建清理任务
func (s *CleanupTaskService) CreateCleanupTask(jobInstanceID, taskType string) (*models.CleanupTask, error) {
	task := &models.CleanupTask{
		JobInstanceID: jobInstanceID,
		TaskType:      taskType,
		Status:        common.TaskStatusPending,
		MaxRetries:    3,
	}

	err := s.taskRepo.Create(task)
	if err != nil {
		return nil, fmt.Errorf("failed to create cleanup task: %v", err)
	}

	log.Logger.Infof("Created cleanup task for job %s, task ID: %d", jobInstanceID, task.ID)
	return task, nil
}

// GetOrCreateCleanupTask 获取或创建清理任务
func (s *CleanupTaskService) GetOrCreateCleanupTask(jobInstanceID, taskType string) (*models.CleanupTask, error) {
	// 先尝试查找现有任务
	task, err := s.taskRepo.FindByJobInstanceID(jobInstanceID)
	if err == nil {
		// 任务已存在，检查状态
		if task.Status == "completed" {
			log.Logger.Infof("Cleanup task for job %s already completed", jobInstanceID)
			return task, nil
		}
		if task.Status == "failed" && task.RetryCount < task.MaxRetries {
			// 可以重试
			log.Logger.Infof("Retrying failed cleanup task for job %s", jobInstanceID)
			return task, nil
		}
	}

	// 创建新任务
	return s.CreateCleanupTask(jobInstanceID, taskType)
}

// ExecuteCleanupTask 执行清理任务
func (s *CleanupTaskService) ExecuteCleanupTask(jobInstanceId string) error {
	task, err := s.GetCleanupTaskByJobInstanceId(jobInstanceId)
	if err != nil {
		return fmt.Errorf("failed to get cleanup task: %v", err)
	}

	// 更新状态为运行中
	err = s.taskRepo.UpdateStatus(task.ID, common.TaskStatusRunning, "")
	if err != nil {
		return fmt.Errorf("failed to update task status: %v", err)
	}

	// 清理导出的数据文件
	err = s.cleanupExportFiles(jobInstanceId)
	if err != nil {
		log.Logger.Warnf("failed to cleanup export files: %v", err)
	}

	// 根据任务类型执行不同的清理逻辑
	switch task.TaskType {
	case "doris_table":
		return s.executeDorisTableCleanup(task)
	case "mira_table":
		return s.executeMiraTableCleanup(task)
	default:
		errorMsg := fmt.Sprintf("unknown task type: %s", task.TaskType)
		s.taskRepo.UpdateStatus(task.ID, common.TaskStatusFailed, errorMsg)
		return errors.New(errorMsg)
	}
}

// executeDorisTableCleanup 执行Doris表清理
func (s *CleanupTaskService) executeDorisTableCleanup(task *models.CleanupTask) error {
	defer func() {
		if r := recover(); r != nil {
			errorMsg := fmt.Sprintf("panic during cleanup: %v", r)
			s.taskRepo.UpdateStatus(task.ID, "failed", errorMsg)
		}
	}()

	// 创建Doris服务
	dorisService, err := NewDorisService(task.JobInstanceID)
	if err != nil {
		errorMsg := fmt.Sprintf("failed to create doris service: %v", err)
		s.taskRepo.UpdateStatus(task.ID, "failed", errorMsg)
		return errors.New(errorMsg)
	}

	// 删库前：无视 requestId，直接清理库内所有通过 CREATE FILE 创建的文件
	if ds, ok := dorisService.(*DorisService); ok {
		log.Logger.Infof("Cleaning up all FILE entries in db %s before dropping database", task.JobInstanceID)
		ds.CleanupAllFilesInDatabase(task.JobInstanceID)
	}

	// 执行清理
	err = dorisService.DropDatabase(task.JobInstanceID)
	if err != nil {
		// 增加重试次数
		s.taskRepo.IncrementRetryCount(task.ID)

		// 检查是否超过最大重试次数
		task.RetryCount++
		if task.RetryCount >= task.MaxRetries {
			errorMsg := fmt.Sprintf("cleanup failed after %d retries: %v", task.MaxRetries, err)
			s.taskRepo.UpdateStatus(task.ID, "failed", errorMsg)
			return fmt.Errorf("cleanup failed after %d retries: %v", task.MaxRetries, err)
		}

		// 更新状态为pending，等待下次重试
		s.taskRepo.UpdateStatus(task.ID, "pending", "")
		return fmt.Errorf("cleanup failed after %d retries: %v", task.MaxRetries, err)
	}

	// 清理成功
	s.taskRepo.UpdateStatus(task.ID, "completed", "")
	log.Logger.Infof("Successfully completed cleanup task %d for job %s", task.ID, task.JobInstanceID)

	return nil
}

// executeMiraTableCleanup 执行Mira表清理（类似实现）
func (s *CleanupTaskService) executeMiraTableCleanup(task *models.CleanupTask) error {
	// 实现Mira表清理逻辑
	// 类似executeDorisTableCleanup的实现
	return nil
}

// GetRetryCleanupTasks 获取需要重新执行的清理任务列表
func (s *CleanupTaskService) GetRetryCleanupTasks(page, pageSize int) ([]models.CleanupTask, int64, error) {
	offset := (page - 1) * pageSize

	// 获取总数
	totalCount, err := s.taskRepo.CountPendingTasks()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count pending tasks: %v", err)
	}

	// 获取分页数据
	tasks, err := s.taskRepo.FindPendingTasksWithPagination(offset, pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get pending tasks: %v", err)
	}

	return tasks, totalCount, nil
}

// GetCleanupTaskByJobInstanceId 根据作业实例ID查找清理任务
func (s *CleanupTaskService) GetCleanupTaskByJobInstanceId(jobInstanceID string) (*models.CleanupTask, error) {
	task, err := s.taskRepo.FindByJobInstanceID(jobInstanceID)
	if err != nil {
		if err == gormlib.ErrRecordNotFound {
			return nil, fmt.Errorf("cleanup task not found for job instance: %s", jobInstanceID)
		}
		return nil, fmt.Errorf("failed to find cleanup task for job %s: %v", jobInstanceID, err)
	}

	log.Logger.Infof("Found cleanup task for job %s: ID=%d, Status=%s, RetryCount=%d",
		jobInstanceID, task.ID, task.Status, task.RetryCount)

	return task, nil
}

// cleanupExportFiles 清理导出的数据文件
func (s *CleanupTaskService) cleanupExportFiles(jobInstanceID string) error {
	// 创建OSS客户端
	ossFactory := oss.NewOSSFactory(config.GetConfigMap())
	ossClient, err := ossFactory.NewOSSClient()
	if err != nil {
		return fmt.Errorf("failed to create OSS client: %v", err)
	}

	// 清理批量数据桶中的导出文件
	err = ossClient.DeleteObjectsByJobInstanceId(context.Background(), common.BATCH_DATA_BUCKET_NAME, jobInstanceID)
	if err != nil {
		return fmt.Errorf("failed to delete objects by job instance id from batch data bucket: %v", err)
	}

	log.Logger.Infof("Successfully cleaned up export files for job %s", jobInstanceID)
	return nil
}
