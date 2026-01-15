package service

import (
	"context"
	"data-service/common"
	"data-service/config"
	pb "data-service/generated/datasource"
	"data-service/log"
	"fmt"
	"strings"
	"sync"
)

// ImportService 数据导入服务
type ImportService struct {
}

var importLocks sync.Map

// NewImportService 创建数据导入服务实例
func NewImportService() *ImportService {
	return &ImportService{}
}

// ImportData 执行数据导入
func (s *ImportService) ImportData(ctx context.Context, request *pb.ImportDataRequest) (*pb.ImportDataResponse, error) {
	response := &pb.ImportDataResponse{
		Success: true,
		Message: "Import completed",
		Results: make([]*pb.ImportResult, 0, len(request.Targets)),
	}

	for _, target := range request.Targets {
		result := s.processImportTarget(ctx, target)
		response.Results = append(response.Results, result)

		// 如果有任何失败，更新整体状态
		if !result.Success {
			response.Success = false
		}
	}

	// 设置最终消息
	s.setFinalMessage(response)

	return response, nil
}

// processImportTarget 处理单个导入目标
func (s *ImportService) processImportTarget(ctx context.Context, target *pb.ImportTarget) *pb.ImportResult {
	result := &pb.ImportResult{
		SourceDatabase: target.External.AssetName,
		TargetDatabase: target.DbName,
		Success:        false,
	}

	// 1. 创建数据库
	if err := s.ensureDatabase(target.DbName); err != nil {
		result.ErrorMessage = err.Error()
		return result
	}

	// 2. 导入数据
	tableName, affectedRows, err := s.importDataToDoris(target)
	if err != nil {
		result.ErrorMessage = err.Error()
		return result
	}

	// 3. 创建清理任务
	s.createCleanupTask(target.DbName)

	// 设置成功结果
	result.Success = true
	result.SourceTableName = target.External.AssetName
	result.TargetTableName = tableName
	result.AffectedRows = affectedRows

	return result
}

// ensureDatabase 确保数据库存在
func (s *ImportService) ensureDatabase(dbName string) error {
	dbService, err := NewDorisService("")
	if err != nil {
		return fmt.Errorf("failed to create db service: %v", err)
	}

	err = dbService.EnsureDorisDatabaseExists(dbName)
	if err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	return nil
}

// importDataToDoris 将数据导入到 Doris
func (s *ImportService) importDataToDoris(target *pb.ImportTarget) (string, int64, error) {
	log.Logger.Infof("importDataToDoris params - DbName: %s, TargetTableName: %s, Columns: %v, External: %+v, Keys: %+v",
		target.DbName, target.TargetTableName, target.Columns, target.External, target.Keys)

	// 并发防重：按库+表序列化导入
	unlock := lockImport(fmt.Sprintf("%s.%s", target.DbName, target.TargetTableName))
	defer unlock()

	dorisService, err := NewDorisService(target.DbName)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create doris service: %v", err)
	}

	randomSuffix, err := common.GenerateRandomString(8)
	if err != nil {
		return "", 0, fmt.Errorf("failed to generate random suffix: %v", err)
	}
	jobInstanceId := target.DbName + "_" + randomSuffix

	// 优先使用 ImportTarget.keys 中的“自增主键”定义
	if pk, ok := getAutoIncrementPrimaryKey(target); ok {
		// 从配置文件读取分批大小，如果未配置则使用默认值 5000
		batchSize := config.GetConfigMap().DorisConfig.ImportBatchSize
		if batchSize <= 0 {
			batchSize = 5000
		}
		tableName, err := dorisService.CreateExternalAndInternalTableAndImportDataBatched(
			target.External.AssetName,
			target.External.ChainInfoId,
			target.External.Alias,
			jobInstanceId,
			target.TargetTableName,
			target.DbName,
			pk,
			batchSize,
			target.Columns...,
		)
		if err != nil {
			return "", 0, err
		}
		affectedRows, err := s.getTableRowCount(dorisService, tableName)
		if err != nil {
			log.Logger.Warnf("Failed to get row count for table %s: %v", tableName, err)
			affectedRows = 0
		}
		return tableName, affectedRows, nil
	}

	tableName, err := dorisService.CreateExternalAndInternalTableAndImportData(
		target.External.AssetName,
		target.External.ChainInfoId,
		target.External.Alias,
		jobInstanceId,
		target.TargetTableName,
		target.DbName,
		target.Columns...,
	)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create external table from asset: %v", err)
	}

	// 查询导入的行数
	affectedRows, err := s.getTableRowCount(dorisService, tableName)
	if err != nil {
		log.Logger.Warnf("Failed to get row count for table %s: %v", tableName, err)
		// 即使无法获取行数，也不影响主流程
		affectedRows = 0
	}

	return tableName, affectedRows, nil
}

// getTableRowCount 获取表的行数
func (s *ImportService) getTableRowCount(dorisService IDorisService, tableName string) (int64, error) {
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	rows, done, err := dorisService.ExecuteSQL(countSQL)
	if err != nil {
		return 0, err
	}
	if done != nil {
		defer done()
	} else {
		defer rows.Close()
	}

	if rows.Next() {
		var count int64
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}

	return 0, fmt.Errorf("no result returned from count query")
}

// createCleanupTask 创建清理任务
func (s *ImportService) createCleanupTask(dbName string) {
	cleanupService := NewCleanupTaskService()
	cleanupTask, err := cleanupService.GetOrCreateCleanupTask(dbName, "doris_table")
	if err != nil {
		log.Logger.Warnf("Failed to create cleanup task for job %s: %v", dbName, err)
		// 不阻断主流程，只记录警告日志
	} else {
		log.Logger.Infof("Cleanup task created/retrieved for job %s, task ID: %d", dbName, cleanupTask.ID)
	}
}

// setFinalMessage 设置最终消息
func (s *ImportService) setFinalMessage(response *pb.ImportDataResponse) {
	if !response.Success {
		// 检查是否全部失败
		allFailed := true
		for _, result := range response.Results {
			if result.Success {
				allFailed = false
				break
			}
		}

		if allFailed {
			response.Message = "Import failed for all targets"
		} else {
			response.Message = "Import completed with some failures"
		}
	} else {
		response.Message = "Import completed successfully"
	}
}

// 根据 ImportTarget.keys 解析是否存在"单列自增主键"；若无，则连到源库自动检测
func getAutoIncrementPrimaryKey(target *pb.ImportTarget) (string, bool) {
	// 只从 ImportTarget.keys 中读取自增主键
	if target != nil && len(target.Keys) > 0 {
		for _, k := range target.Keys {
			// 检查是否为自增主键类型
			if k == nil || k.KeyType != pb.KeyType_KEY_TYPE_AUTO_INCREMENT {
				continue
			}
			// 仅支持单列主键做分批
			if len(k.ColumnNames) != 1 {
				continue
			}
			pk := strings.TrimSpace(k.ColumnNames[0])
			if pk == "" {
				continue
			}
			return pk, true
		}
	}

	// 未找到自增主键
	return "", false
}

func lockImport(key string) func() {
	muIface, _ := importLocks.LoadOrStore(key, &sync.Mutex{})
	mu := muIface.(*sync.Mutex)
	mu.Lock()
	return func() { mu.Unlock() }
}
