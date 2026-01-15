/*
*

	@author: shiliang
	@date: 2024/10/25
	@note: 数据库增删改查操作

*
*/
package database

import (
	"context"
	"data-service/common"
	"data-service/config"
	pb "data-service/generated/datasource"
	log "data-service/log"
	"data-service/utils"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/google/uuid"
)

func InsertArrowDataInBatches(db *sql.DB, tableName string, schema *arrow.Schema, ipcReader *ipc.Reader, dbType pb.DataSourceType) error {
	for i := 0; i < common.MAX_RETRY_COUNT; i++ {
		err := performBatchInsert(db, tableName, schema, ipcReader, dbType)
		if err == nil {
			return nil // 插入成功
		}

		// 如果是死锁错误则重试
		if isWriteConflictError(err) {
			log.Logger.Infof("Retrying batch insert due to write conflict error: %v", err)
			time.Sleep(common.MAX_RETRY_INTERVAL)
			continue
		}

		return fmt.Errorf("batch insert failed: %v", err) // 非死锁错误立即返回
	}

	return fmt.Errorf("failed to insert data after %d retries", common.MAX_RETRY_COUNT)
}

// 处理批量插入数据
func processBatch(tx *sql.Tx, args []interface{}, currentRowCount int64, schema *arrow.Schema,
	tableName string, dbType pb.DataSourceType) ([]interface{}, int64, int64, error) {
	var totalProcessedRows int64 = 0
	sqlGenerator := &SQLGenerator{}

	for len(args) >= common.MAX_BATCH_ARGS_SIZE {
		fieldsPerRow := len(schema.Fields())
		completeRows := (common.MAX_BATCH_ARGS_SIZE / fieldsPerRow) * fieldsPerRow

		batchToInsert := args[:completeRows]
		remainingArgs := args[completeRows:]

		insertSQL, err := sqlGenerator.GenerateInsertSQL(tableName, batchToInsert, schema, dbType)
		log.Logger.Debugf("Get InsertSQL: %s", insertSQL)
		log.Logger.Debugf("Get InsertBatch: %+v", batchToInsert)
		if err != nil {
			log.Logger.Errorf("Failed to generate insert SQL: %v", err)
			return nil, 0, 0, fmt.Errorf("failed to generate insert SQL: %v", err)
		}

		if _, err := tx.Exec(insertSQL, batchToInsert...); err != nil {
			log.Logger.Errorf("Failed to execute batch insert to table %s: %v", tableName, err)
			return nil, 0, 0, fmt.Errorf("failed to execute batch insert: %v", err)
		}

		args = remainingArgs
		processedRows := int64(completeRows / fieldsPerRow)
		totalProcessedRows += processedRows
		currentRowCount -= processedRows
	}

	// 处理剩余的参数
	if len(args) > 0 {
		insertSQL, err := sqlGenerator.GenerateInsertSQL(tableName, args, schema, dbType)
		log.Logger.Debugf("Get InsertSQL: %s", insertSQL)
		log.Logger.Debugf("Get InsertBatch: %+v", args)
		if err != nil {
			log.Logger.Errorf("Failed to generate insert SQL: %v", err)
			return nil, 0, 0, fmt.Errorf("failed to generate insert SQL: %v", err)
		}

		if _, err := tx.Exec(insertSQL, args...); err != nil {
			log.Logger.Errorf("Failed to execute batch insert to table %s: %v", tableName, err)
			return nil, 0, 0, fmt.Errorf("failed to execute batch insert: %v", err)
		}

		totalProcessedRows += currentRowCount
		currentRowCount = 0
		args = []interface{}{}
	}

	return args, currentRowCount, totalProcessedRows, nil
}

// 执行批量插入的逻辑
func performBatchInsert(db *sql.DB, tableName string, schema *arrow.Schema, ipcReader *ipc.Reader, dbType pb.DataSourceType) error {
	tx, err := db.Begin()
	if err != nil {
		log.Logger.Errorf("Failed to begin transaction: %v", err)
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback() // 确保事务失败时回滚

	argsBatch := []interface{}{}
	rowCount := int64(0)
	totalRowCount := int64(0)
	log.Logger.Infof("Start Inserting........")
	rowExtractor := &ArrowRowExtractor{}

	// 主处理循环
	for ipcReader.Next() {
		record := ipcReader.Record()
		if record == nil || record.NumRows() == 0 {
			log.Logger.Infof("Skipping empty record")
			continue
		}

		args, err := rowExtractor.ExtractRowData(record)
		if err != nil {
			log.Logger.Errorf("Failed to extract row data: %v", err)
			return err
		}
		log.Logger.Debugf("Extracted row data: %v", args)
		argsBatch = append(argsBatch, args...)
		rowCount += record.NumRows()

		// 当达到批次大小时处理数据
		if rowCount >= int64(common.BATCH_DATA_SIZE) {
			var processedCount int64
			argsBatch, rowCount, processedCount, err = processBatch(tx, argsBatch, rowCount, schema, tableName, dbType)
			if err != nil {
				return err
			}
			totalRowCount += processedCount
		}
	}

	// 处理最后一批数据
	if rowCount > 0 || len(argsBatch) > 0 {
		var processedCount int64
		_, _, processedCount, err = processBatch(tx, argsBatch, rowCount, schema, tableName, dbType)
		if err != nil {
			return err
		}
		totalRowCount += processedCount
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		log.Logger.Errorf("Failed to commit transaction to table %s: %v", tableName, err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log.Logger.Infof("Data inserted successfully into %s, total row count: %d", tableName, totalRowCount)
	return nil
}

// 判断是否为写冲突错误
func isWriteConflictError(err error) bool {
	errMsg := err.Error()
	return strings.Contains(errMsg, "Duplicate entry") || // 唯一键冲突
		strings.Contains(errMsg, "Lock wait timeout") // 锁超时
}

// StartAutoProfile 启动定时 CPU、内存和 trace profile 采集
func StartAutoProfile() {
	go func() {
		traceDir := "/home/workspace/logs/trace"
		// 确保 trace 目录存在
		if err := os.MkdirAll(traceDir, os.ModePerm); err != nil {
			log.Logger.Errorf("could not create trace dir: %v", err)
			return
		}

		// 清理旧日志文件的函数
		cleanupOldProfiles := func() {
			files, err := os.ReadDir(traceDir)
			if err != nil {
				log.Logger.Errorf("Failed to read trace directory: %v", err)
				return
			}

			now := time.Now()
			for _, file := range files {
				if file.IsDir() {
					continue
				}

				info, err := file.Info()
				if err != nil {
					continue
				}

				// 删除7天前的文件
				if now.Sub(info.ModTime()) > 7*24*time.Hour {
					if err := os.Remove(filepath.Join(traceDir, file.Name())); err != nil {
						log.Logger.Errorf("Failed to remove old profile file %s: %v", file.Name(), err)
					} else {
						log.Logger.Infof("Removed old profile file: %s", file.Name())
					}
				}
			}
		}

		// 性能采集计数器
		profileCounter := 0
		const (
			CPU_PROFILE_INTERVAL   = 3  // 每3小时采集一次CPU
			MEM_PROFILE_INTERVAL   = 6  // 每6小时采集一次内存
			TRACE_PROFILE_INTERVAL = 12 // 每12小时采集一次trace
		)

		for {
			// 每次采集前清理旧文件
			cleanupOldProfiles()

			now := time.Now().Format("20060102_150405")

			// CPU Profile - 每3小时采集一次，每次10秒
			if profileCounter%CPU_PROFILE_INTERVAL == 0 {
				cpuFilename := traceDir + "/cpu_profile_" + now + ".out"
				cpuFile, err := os.Create(cpuFilename)
				if err != nil {
					log.Logger.Errorf("could not create CPU profile: %v", err)
				} else {
					log.Logger.Infof("Start CPU profiling: %s", cpuFilename)
					pprof.StartCPUProfile(cpuFile)
					time.Sleep(10 * time.Second) // 减少采样时间
					pprof.StopCPUProfile()
					cpuFile.Close()
					log.Logger.Infof("CPU profiling finished: %s", cpuFilename)
				}
			}

			// Memory Profile - 每6小时采集一次
			if profileCounter%MEM_PROFILE_INTERVAL == 0 {
				memFilename := traceDir + "/mem_profile_" + now + ".out"
				memFile, err := os.Create(memFilename)
				if err != nil {
					log.Logger.Errorf("could not create memory profile: %v", err)
				} else {
					log.Logger.Infof("Start memory profiling: %s", memFilename)
					pprof.WriteHeapProfile(memFile)
					memFile.Close()
					log.Logger.Infof("Memory profiling finished: %s", memFilename)
				}
			}

			// Trace Profile - 每12小时采集一次，每次5秒
			if profileCounter%TRACE_PROFILE_INTERVAL == 0 {
				traceFilename := traceDir + "/trace_" + now + ".out"
				traceFile, err := os.Create(traceFilename)
				if err != nil {
					log.Logger.Errorf("could not create trace profile: %v", err)
				} else {
					log.Logger.Infof("Start trace profiling: %s", traceFilename)
					if err := trace.Start(traceFile); err != nil {
						log.Logger.Errorf("could not start trace: %v", err)
						traceFile.Close()
					} else {
						time.Sleep(5 * time.Second) // 减少采样时间
						trace.Stop()
						traceFile.Close()
						log.Logger.Infof("Trace profiling finished: %s", traceFilename)
					}
				}
			}

			profileCounter++
			time.Sleep(time.Hour)
		}
	}()
}

func Init() error {
	StartAutoProfile()
	conf := config.GetConfigMap()
	dbType := utils.ConvertDBType(conf.Dbms.Type)
	connInfo := &pb.ConnectionInfo{Host: conf.Dbms.Host,
		Port:     conf.Dbms.Port,
		User:     conf.Dbms.User,
		DbName:   pb.DbConstant_name[int32(pb.DbConstant_MIRA_ENGINE_TEMP)],
		Password: conf.Dbms.Password,
	}
	dbStrategy, err := DatabaseFactory(dbType, connInfo)
	if err != nil {
		return fmt.Errorf("failed to create database strategy: %v", err)
	}

	baseConnInfo := *connInfo
	baseConnInfo.DbName = ""
	if err := dbStrategy.ConnectToDBWithPass(&baseConnInfo); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	// 检查数据库是否存在，如不存在则创建
	if err := dbStrategy.EnsureDatabaseExists(connInfo.DbName); err != nil {
		log.Logger.Errorf("Failed to ensure database exists: %v", err)
		return fmt.Errorf("failed to ensure database exists: %v", err)
	}

	return nil
}

func ProcessGroupCountResults(rows *sql.Rows, tableName string) (*pb.GroupCountResponse, error) {
	recordCount := int64(0)

	// 获取结果集的列信息
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns info: %v", err)
	}

	// 创建一个与列数匹配的切片
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		// 为每个值创建一个指针
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// 如果只有一个列，那就是计数值
		if len(columns) == 1 {
			// 提取计数值
			var count int64
			var err error
			switch v := values[0].(type) {
			case int64:
				count = v
			case int32:
				count = int64(v)
			case int:
				count = int64(v)
			case int16:
				count = int64(v)
			case int8:
				count = int64(v)
			case uint64:
				count = int64(v)
			case uint32:
				count = int64(v)
			case uint:
				count = int64(v)
			case uint16:
				count = int64(v)
			case uint8:
				count = int64(v)
			case []byte:
				count, err = strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("failed to convert count value to int64: %v", err)
				}
			default:
				return nil, fmt.Errorf("unexpected count value type: %T", v)
			}
			recordCount = count
		} else {
			// 如果有多列，最后一列是计数值
			countIndex := len(values) - 1

			// 提取计数值
			var count int64
			var err error
			switch v := values[countIndex].(type) {
			case int64:
				count = v
			case int32:
				count = int64(v)
			case int:
				count = int64(v)
			case int16:
				count = int64(v)
			case int8:
				count = int64(v)
			case uint64:
				count = int64(v)
			case uint32:
				count = int64(v)
			case uint:
				count = int64(v)
			case uint16:
				count = int64(v)
			case uint8:
				count = int64(v)
			case []byte:
				count, err = strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("failed to convert count value to int64: %v", err)
				}
			default:
				return nil, fmt.Errorf("unexpected count value type: %T", v)
			}
			recordCount = count
		}
	}

	// 检查是否有错误发生
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating rows: %v", err)
	}

	// 返回结果
	return &pb.GroupCountResponse{
		TableName:   tableName,
		RecordCount: recordCount,
	}, nil
}

// CalculateEstimatedBatchSize 计算预估的批次大小，并返回应该获取的记录数
func CalculateEstimatedBatchSize(rows interface {
	ColumnTypes() ([]*sql.ColumnType, error)
}, batchSize int) (int, error) {
	// 获取列类型前加日志
	log.Logger.Debugf("CalculateEstimatedBatchSize: rows pointer: %p, batchSize: %d", rows, batchSize)

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Logger.Errorf("CalculateEstimatedBatchSize: failed to get column types, rows: %p, err: %v", rows, err)
		// 如果是 context 错误，打印更多提示
		if errors.Is(err, context.Canceled) {
			log.Logger.Errorf("CalculateEstimatedBatchSize: context was canceled before getting column types")
		}
		return 0, fmt.Errorf("failed to get column types: %v", err)
	}

	// 计算每行预估大小
	var rowSize int64
	for _, ct := range columnTypes {
		log.Logger.Debugf("Column: %s, Type: %s", ct.Name(), ct.DatabaseTypeName())
		switch ct.DatabaseTypeName() {
		case "VARCHAR", "CHAR", "TEXT", "LONGTEXT":
			rowSize += 100 // 预估平均长度
		case "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL":
			rowSize += 8
		case "TIMESTAMP", "DATETIME":
			rowSize += 8
		case "BOOLEAN":
			rowSize += 1
		default:
			rowSize += 8 // 默认值
		}
	}

	// 计算预估的批次大小，考虑 Arrow 格式的额外开销
	estimatedSize := rowSize * int64(batchSize)
	estimatedSize = estimatedSize + estimatedSize/5 // 增加 20% 的 Arrow 格式开销

	// 如果预估大小超过 gRPC 消息大小限制，则按比例缩小批次大小
	if estimatedSize > common.GRPC_MAX_MESSAGE_SIZE {
		adjustedBatchSize := int(float64(batchSize) * float64(common.GRPC_MAX_MESSAGE_SIZE) / float64(estimatedSize))
		return adjustedBatchSize, nil
	}

	return batchSize, nil
}

// generateIndexName 生成符合 MySQL 长度限制的索引名称
func generateIndexName() string {
	// 生成 UUID 并取前 10 位
	uuid := uuid.New().String()
	// 去掉 UUID 中的横线，取前 10 位
	shortUUID := strings.ReplaceAll(uuid, "-", "")[:10]
	return "idx_" + shortUUID
}

func AddIndexToFilterName(db *sql.DB, dbType pb.DataSourceType, dbName, tableName, columnName string) error {
	var checkIndexSQL string
	var args []interface{}

	switch dbType {
	case pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
		pb.DataSourceType_DATA_SOURCE_TYPE_TDSQL,
		pb.DataSourceType_DATA_SOURCE_TYPE_TIDB:
		checkIndexSQL = `
			SELECT COUNT(1)
			FROM information_schema.STATISTICS
			WHERE table_schema = ?
			  AND table_name = ?
			  AND column_name = ?;
		`
		args = []interface{}{dbName, tableName, columnName}
	case pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE:
		checkIndexSQL = `
			SELECT COUNT(1)
			FROM pg_class t, pg_class i, pg_index ix, pg_attribute a
			WHERE t.oid = ix.indrelid
			  AND i.oid = ix.indexrelid
			  AND a.attrelid = t.oid
			  AND a.attnum = ANY(ix.indkey)
			  AND t.relname = $1
			  AND a.attname = $2;
		`
		args = []interface{}{tableName, columnName}
	default:
		return fmt.Errorf("unsupported db type: %v", dbType)
	}

	var count int
	err := db.QueryRow(checkIndexSQL, args...).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check index existence: %v", err)
	}
	if count > 0 {
		log.Logger.Infof("Column %s of table %s already has an index, skip creating.", columnName, tableName)
		return nil // 已有索引，直接返回
	}

	// 没有索引则创建
	indexName := generateIndexName()
	sqlStatement := fmt.Sprintf("CREATE INDEX %s ON %s (%s);", indexName, tableName, columnName)
	log.Logger.Infof("Attempting to create index: %s", sqlStatement)
	_, err = db.Exec(sqlStatement)
	_, err = db.Exec(sqlStatement)
	if err != nil {
		log.Logger.Errorf("Failed to create index %s on table %s: %v", indexName, tableName, err)
		return fmt.Errorf("failed to create index %s: %v", indexName, err)
	}
	log.Logger.Infof("Successfully created index %s on table %s", indexName, tableName)
	return nil
}
