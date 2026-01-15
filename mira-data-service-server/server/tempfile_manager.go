package main

import (
	"data-service/common"
	"data-service/log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// TempFileManager 管理临时文件的创建和清理
type TempFileManager struct {
	activeFiles sync.Map // 记录活跃的临时文件
}

var (
	manager = &TempFileManager{}
	once    sync.Once
)

// GetManager 获取单例的 TempFileManager
func GetManager() *TempFileManager {
	once.Do(func() {
		// 确保临时目录存在
		if err := os.MkdirAll(common.DATA_DIR, 0755); err != nil {
			log.Logger.Errorf("Failed to create temp directory: %v", err)
		}
	})
	return manager
}

// CreateTempFile 创建临时文件并记录
func (m *TempFileManager) CreateTempFile() (*os.File, error) {
	tmpFile, err := os.CreateTemp(common.DATA_DIR, "arrow_stream_*.arrow")
	if err != nil {
		return nil, err
	}

	m.activeFiles.Store(tmpFile.Name(), true)
	log.Logger.Debugf("Created temp file: %s", tmpFile.Name())
	return tmpFile, nil
}

// ReleaseTempFile 释放临时文件
func (m *TempFileManager) ReleaseTempFile(fileName string) {
	m.activeFiles.Delete(fileName)
	log.Logger.Debugf("Released temp file: %s", fileName)
}

// IsFileActive 检查文件是否在使用中
func (m *TempFileManager) IsFileActive(fileName string) bool {
	_, exists := m.activeFiles.Load(fileName)
	return exists
}

// StartCleanupTask 启动清理任务
func (m *TempFileManager) StartCleanupTask() {
	go m.cleanupTempFiles()
}

// cleanupTempFiles 清理临时文件
func (m *TempFileManager) cleanupTempFiles() {
	log.Logger.Info("Starting temp files cleanup task")
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		log.Logger.Info("Running temp files cleanup cycle")
		files, err := os.ReadDir(common.DATA_DIR)
		if err != nil {
			log.Logger.Errorf("Failed to read temp directory: %v", err)
			continue
		}

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "arrow_stream_") && strings.HasSuffix(file.Name(), ".arrow") {
				filePath := filepath.Join(common.DATA_DIR, file.Name())

				// 检查文件是否在使用中
				if m.IsFileActive(filePath) {
					log.Logger.Debugf("File %s is still active, skipping", filePath)
					continue
				}

				// 检查文件年龄
				fileInfo, err := os.Stat(filePath)
				if err != nil {
					log.Logger.Errorf("Failed to stat file %s: %v", filePath, err)
					continue
				}

				// 只清理超过6小时的文件
				if time.Since(fileInfo.ModTime()) < time.Hour*6 {
					log.Logger.Debugf("Skipping recent file: %s", filePath)
					continue
				}

				if err := os.Remove(filePath); err != nil {
					log.Logger.Errorf("Failed to remove old temp file %s: %v", filePath, err)
				} else {
					log.Logger.Debugf("Cleaned up old temp file: %s", filePath)
				}
			}
		}
		log.Logger.Info("Completed temp files cleanup cycle")
	}
}
