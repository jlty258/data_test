package utils

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/iancoleman/orderedmap"
)

// JSON -> CSV, 写入内存不落盘, 保持JSON字段原始顺序
func JsonToCSV(jsonData []byte) (string, error) {
	// 空JSON返回空字符串
	if len(jsonData) == 0 {
		return "", nil
	}

	// 解析JSON数据
	var data []*orderedmap.OrderedMap
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return "", fmt.Errorf("failed to parse JSON: %v", err)
	}

	// 使用 strings.Builder 来创建一个内存中的 CSV
	var sb strings.Builder
	writer := csv.NewWriter(&sb)

	// 获取CSV表头：使用第一条记录的键顺序
	var header []string
	if len(data) > 0 {
		header = data[0].Keys()
	}

	// 写入表头
	if err := writer.Write(header); err != nil {
		return "", fmt.Errorf("failed to write header to CSV: %v", err)
	}

	// 写入数据
	for _, record := range data {
		var row []string
		for _, field := range header {
			value, exists := record.Get(field)
			if exists {
				row = append(row, fmt.Sprintf("%v", value))
			} else {
				// 对于缺失的字段，填充空字符串
				row = append(row, "")
			}
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write record to CSV: %v", err)
		}
	}

	// 确保所有数据写入
	writer.Flush()

	// 返回生成的CSV字符串
	return sb.String(), nil
}

func HardwareJsonToCSV(jsonData []byte) (string, error) {
	// 空JSON返回空字符串
	if len(jsonData) == 0 {
		return "", nil
	}

	// 解析JSON数据
	var rawData []map[string]interface{}
	if err := json.Unmarshal(jsonData, &rawData); err != nil {
		return "", fmt.Errorf("failed to parse JSON: %v", err)
	}

	// 处理嵌套对象，将嵌套对象转换为JSON字符串
	var data []*orderedmap.OrderedMap
	for _, item := range rawData {
		orderedItem := orderedmap.New()
		for key, value := range item {
			// 检查值是否为嵌套对象
			switch v := value.(type) {
			case map[string]interface{}:
				// 将嵌套对象转换为JSON字符串
				nestedJSON, err := json.Marshal(v)
				if err != nil {
					return "", fmt.Errorf("failed to marshal nested object: %v", err)
				}
				orderedItem.Set(key, string(nestedJSON))
			case []interface{}:
				// 处理数组类型的值
				nestedJSON, err := json.Marshal(v)
				if err != nil {
					return "", fmt.Errorf("failed to marshal nested array: %v", err)
				}
				orderedItem.Set(key, string(nestedJSON))
			default:
				orderedItem.Set(key, value)
			}
		}
		data = append(data, orderedItem)
	}

	// 使用 strings.Builder 来创建一个内存中的 CSV
	var sb strings.Builder
	writer := csv.NewWriter(&sb)

	// 获取CSV表头：使用第一条记录的键顺序
	var header []string
	if len(data) > 0 {
		header = data[0].Keys()
	}

	// 写入表头
	if err := writer.Write(header); err != nil {
		return "", fmt.Errorf("failed to write header to CSV: %v", err)
	}

	// 写入数据
	for _, record := range data {
		var row []string
		for _, field := range header {
			value, exists := record.Get(field)
			if exists {
				row = append(row, fmt.Sprintf("%v", value))
			} else {
				// 对于缺失的字段，填充空字符串
				row = append(row, "")
			}
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write record to CSV: %v", err)
		}
	}

	// 确保所有数据写入
	writer.Flush()

	// 返回生成的CSV字符串
	return sb.String(), nil
}

// PreviewSQL 压缩空白并截断长 SQL，用于日志安全输出
func PreviewSQL(s string, limit int) string {
	// 压缩多空格/换行
	s = strings.Join(strings.Fields(s), " ")
	if limit <= 0 || len(s) <= limit {
		return s
	}
	return s[:limit] + fmt.Sprintf("... (truncated, len=%d)", len(s))
}
