package utils

import (
	"bytes"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

// ParseArrowResponse 解析Arrow响应数据
func ParseArrowResponse(data []byte) (arrow.Record, error) {
	reader := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(memory.NewGoAllocator()))
	defer reader.Release()
	
	if !reader.Next() {
		return nil, fmt.Errorf("no record found")
	}
	
	record := reader.Record()
	record.Retain() // 增加引用计数，防止被释放
	
	return record, nil
}

// CountRowsFromArrow 从Arrow数据中统计行数
func CountRowsFromArrow(data []byte) (int64, error) {
	record, err := ParseArrowResponse(data)
	if err != nil {
		return 0, err
	}
	defer record.Release()
	
	return record.NumRows(), nil
}

