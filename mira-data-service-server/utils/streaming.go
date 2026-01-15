/*
*

	@author: shiliang
	@date: 2024/12/18
	@note:

*
*/
package utils

import (
	log "data-service/log"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"os"
)

// GetRemainingRecords 处理并返回剩余的数据
func GetRemainingRecords(recordBatch []arrow.Record, rowsToSend int64) ([]arrow.Record, int64) {
	var remainingRecordBatch []arrow.Record
	remainingRows := rowsToSend

	for _, r := range recordBatch {
		recordRows := r.NumRows() // recordRows 是 int64 类型
		if remainingRows >= recordRows {
			remainingRecordBatch = append(remainingRecordBatch, r)
			remainingRows -= recordRows
		} else {
			// 只保留部分记录，构建新的 Record
			sliceStart := int64(0)
			sliceEnd := remainingRows
			slicedRecord := array.NewRecord(r.Schema(), r.Columns(), sliceEnd-sliceStart)
			remainingRecordBatch = append(remainingRecordBatch, slicedRecord)
			remainingRows = 0
			break
		}
	}

	return remainingRecordBatch, remainingRows
}

// createCSVFile 用于创建并返回一个新的 CSV 文件
func CreateCSVFile(fullPath string) (*os.File, error) {
	file, err := os.Create(fullPath)
	if err != nil {
		log.Logger.Errorf("Failed to create file: %v\n", err)
		return nil, err
	}
	log.Logger.Infof("Created CSV file: %s", fullPath)
	return file, nil
}
