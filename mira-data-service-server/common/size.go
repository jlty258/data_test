/*
*

	@author: shiliang
	@date: 2025/12/24
	@note: 数据大小估算工具函数

*
*/
package common

import (
	"fmt"
	"time"
)

// EstimateRowSize calculates the estimated size of a single row based on its values
func EstimateRowSize(values []interface{}) int64 {
	var rowSize int64
	for _, val := range values {
		if val != nil {
			switch v := val.(type) {
			case []byte:
				rowSize += int64(len(v))
			case string:
				rowSize += int64(len(v))
			case int64:
				rowSize += 8
			case int32:
				rowSize += 4
			case int16:
				rowSize += 2
			case int8:
				rowSize += 1
			case int:
				rowSize += 8 // int is 64-bit on 64-bit platforms, 32-bit on 32-bit platforms
			case uint64:
				rowSize += 8
			case uint32:
				rowSize += 4
			case uint16:
				rowSize += 2
			case uint8:
				rowSize += 1
			case uint:
				rowSize += 8 // uint is 64-bit on 64-bit platforms, 32-bit on 32-bit platforms
			case float64:
				rowSize += 8
			case float32:
				rowSize += 4
			case bool:
				rowSize += 1
			case time.Time:
				// Time is typically stored as 8 bytes (Unix timestamp) plus timezone info
				// Estimate as string representation size for accuracy
				rowSize += int64(len(v.Format(time.RFC3339)))
			default:
				// For other types, estimate using string length
				rowSize += int64(len(fmt.Sprintf("%v", v)))
			}
		}
	}
	return rowSize
}
