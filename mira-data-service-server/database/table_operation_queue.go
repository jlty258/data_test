/*
*

	@author: shiliang
	@date: 2025/4/18
	@note:

*
*/
package database

import (
	pb "data-service/generated/datasource"
	log2 "data-service/log"
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/ipc"
)

// TableOperation 表示一个表操作任务
type TableOperation struct {
	TableName  string
	Schema     *arrow.Schema
	DbType     pb.DataSourceType
	DB         *sql.DB
	IpcReader  *ipc.Reader
	DbStrategy DatabaseStrategy
	ResultCh   chan error
}

var (
	// 全局操作队列
	globalOperationQueue chan *TableOperation
	// 用于通知处理协程退出的信号通道
	stopSignal      chan struct{}
	queueInitOnce   sync.Once
	tableQueueMutex sync.Mutex

	// 记录每个表当前是否有正在处理的操作
	tableProcessingMap   = make(map[string]bool)
	tableProcessingMutex sync.Mutex
)

// 初始化全局队列和处理协程
func initGlobalQueue() {
	queueInitOnce.Do(func() {
		globalOperationQueue = make(chan *TableOperation, 1000) // 队列大小可根据需求调整
		stopSignal = make(chan struct{})
		go processOperations(globalOperationQueue, stopSignal)
	})
}

// GetOperationQueue 获取操作队列
func GetOperationQueue() chan *TableOperation {
	initGlobalQueue()
	return globalOperationQueue
}

func processOperations(queue chan *TableOperation, stopCh chan struct{}) {
	log2.Logger.Infof("启动全局表操作处理协程")

	for {
		select {
		case op, ok := <-queue:
			if !ok {
				// 队列已关闭，退出协程
				log2.Logger.Infof("全局队列已关闭，处理协程退出")
				return
			}

			// 检查表是否正在处理中
			tableProcessingMutex.Lock()
			if tableProcessingMap[op.TableName] {
				// 表正在处理中，将操作放回队列稍后处理
				tableProcessingMutex.Unlock()
				go func(operation *TableOperation) {
					// 稍微延迟后重新入队
					time.Sleep(10 * time.Millisecond)
					queue <- operation
				}(op)
				continue
			}

			// 标记表为处理中
			tableProcessingMap[op.TableName] = true
			tableProcessingMutex.Unlock()

			// 在新的协程中处理操作，这样不会阻塞主循环
			go func(operation *TableOperation) {
				// 处理操作
				processOperation(operation)

				// 处理完成后，移除表的处理中标记
				tableProcessingMutex.Lock()
				delete(tableProcessingMap, operation.TableName)
				tableProcessingMutex.Unlock()
			}(op)

		case <-stopCh:
			// 收到停止信号，退出协程
			log2.Logger.Infof("处理协程收到停止信号，退出")
			return
		}
	}
}

// 处理单个操作
func processOperation(op *TableOperation) {
	defer op.IpcReader.Release()
	// 先创建表（如果不存在）
	err := createTableIfNeeded(op.TableName, op.Schema, op.DbStrategy)
	if err != nil {
		// 检查错误是否是因为表已存在
		if strings.Contains(err.Error(), "already exists") {
			log2.Logger.Infof("Table %s already exists, continuing with data insertion", op.TableName)
		} else {
			// 其他错误需要记录警告
			log2.Logger.Warnf("Create table warning: %v", err)
		}
	}

	// 然后插入数据
	err = InsertArrowDataInBatches(op.DB, op.TableName, op.Schema, op.IpcReader, op.DbType)
	op.ResultCh <- err
}

// CleanupQueue 清理队列
func CleanupQueue() {
	tableQueueMutex.Lock()
	defer tableQueueMutex.Unlock()

	if globalOperationQueue != nil {
		// 发送停止信号
		close(stopSignal)

		// 关闭队列
		close(globalOperationQueue)

		// 重置变量
		globalOperationQueue = nil
		stopSignal = nil

		// 重置初始化标志，允许再次初始化
		queueInitOnce = sync.Once{}
	}
}

// QueueSize 获取队列大小（此处返回1表示有一个全局队列）
func QueueSize() int {
	tableQueueMutex.Lock()
	defer tableQueueMutex.Unlock()

	if globalOperationQueue != nil {
		return 1
	}
	return 0
}

func createTableIfNeeded(tableName string, schema *arrow.Schema, dbStrategy DatabaseStrategy) error {
	return dbStrategy.CreateTemporaryTableIfNotExists(tableName, schema)
}
