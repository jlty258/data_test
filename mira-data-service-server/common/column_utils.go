package common

import (
	"fmt"
	"strings"
)

// ColumnInfo 列信息结构
type ColumnInfo struct {
	Name     string // 列名
	DataType string // 数据类型
	Nullable bool   // 是否可为空
	Default  string // 默认值
}

// FilterColumnsByNames 根据指定的列名过滤列信息
// 如果 columns 为空，返回所有列；否则只返回指定的列（去重后）
func FilterColumnsByNames(allColumns []ColumnInfo, columnNames ...string) ([]ColumnInfo, error) {
	if len(columnNames) == 0 {
		return allColumns, nil
	}

	// 去重请求列
	reqSet := make(map[string]struct{}, len(columnNames))
	var uniqueReq []string
	for _, name := range columnNames {
		if _, ok := reqSet[name]; !ok {
			reqSet[name] = struct{}{}
			uniqueReq = append(uniqueReq, name)
		}
	}

	// 过滤匹配列
	var filtered []ColumnInfo
	foundSet := make(map[string]struct{})
	for _, col := range allColumns {
		if _, ok := reqSet[col.Name]; ok {
			if _, dup := foundSet[col.Name]; !dup {
				filtered = append(filtered, col)
				foundSet[col.Name] = struct{}{}
			}
		}
	}

	// 校验：按去重后的请求集合比较
	if len(foundSet) != len(reqSet) {
		var missing []string
		for _, name := range uniqueReq {
			if _, ok := foundSet[name]; !ok {
				missing = append(missing, name)
			}
		}
		var found []string
		for name := range foundSet {
			found = append(found, name)
		}
		return nil, fmt.Errorf("some specified columns not found: missing columns: %v, found columns: %v", missing, found)
	}

	return filtered, nil
}

// GetColumnNames 从列信息中提取列名
func GetColumnNames(columns []ColumnInfo) []string {
	var names []string
	for _, col := range columns {
		names = append(names, col.Name)
	}
	return names
}

// contains 检查切片中是否包含指定元素
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// filterColumnsWithRowidHandling 过滤列，如果 columnList 中包含 rowid，确保结果中包含类型为 BIGINT 的 rowid 列
// 如果源表中已有 rowid 列，会保留它并将类型改为 BIGINT；如果源表中没有 rowid 列，会添加一个新的 BIGINT 类型的 rowid 列
func FilterColumnsWithRowidHandling(allColumns []ColumnInfo, columnList []string) ([]ColumnInfo, error) {
	// 检查 columnList 中是否包含 rowid
	hasRowidInRequest := false
	for _, col := range columnList {
		if strings.EqualFold(col, "rowid") {
			hasRowidInRequest = true
			break
		}
	}

	// 先尝试正常过滤（如果源表有 rowid，会被正常过滤出来）
	filteredColumns, err := FilterColumnsByNames(allColumns, columnList...)
	if err != nil {
		// 如果过滤失败，检查是否因为缺少 rowid 列
		// 如果是，则从 columnList 中移除 rowid 后重新过滤
		if hasRowidInRequest {
			// 创建不包含 rowid 的列表
			filteredColumnList := make([]string, 0, len(columnList)-1)
			for _, c := range columnList {
				if !strings.EqualFold(c, "rowid") {
					filteredColumnList = append(filteredColumnList, c)
				}
			}
			// 用不包含 rowid 的列表重新过滤
			filteredColumns, err = FilterColumnsByNames(allColumns, filteredColumnList...)
			if err != nil {
				return nil, fmt.Errorf("failed to filter columns: %v", err)
			}
			// 手动添加 rowid 列（类型为 BIGINT）
			rowidColumn := ColumnInfo{
				Name:     "rowid",
				DataType: "BIGINT",
				Nullable: true,
				Default:  "",
			}
			filteredColumns = append(filteredColumns, rowidColumn)
		} else {
			return nil, fmt.Errorf("failed to filter columns: %v", err)
		}
	}

	return filteredColumns, nil
}
