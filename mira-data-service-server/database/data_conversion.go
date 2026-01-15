/*
*

	@author: shiliang
	@date: 2025/2/20
	@note: 数据转化功能，包含不同字段类型的处理、类型转化

*
*/
package database

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
)

type DataConversion interface {
	ExtractRowData(record arrow.Record) ([]interface{}, error)
}

type ArrowRowExtractor struct {
}

func (a ArrowRowExtractor) ExtractRowData(record arrow.Record) ([]interface{}, error) {
	argsBatch := []interface{}{}
	numRows := record.NumRows() // 获取行数

	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ { // 使用 int64
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			column := record.Column(colIdx)
			var value interface{}

			switch column.DataType().ID() {
			case arrow.INT8:
				int8Array := column.(*array.Int8)
				value = int8Array.Value(int(rowIdx)) // 转换为 int8
			case arrow.INT16:
				int16Array := column.(*array.Int16)
				value = int16Array.Value(int(rowIdx)) // 转换为 int16
			case arrow.INT32:
				int32Array := column.(*array.Int32)
				value = int32Array.Value(int(rowIdx)) // 转换为 int
			case arrow.INT64:
				int64Array := column.(*array.Int64)
				value = int64Array.Value(int(rowIdx)) // 转换为 int64
			case arrow.UINT8:
				uint8Array := column.(*array.Uint8)
				value = uint8Array.Value(int(rowIdx)) // 转换为 uint8
			case arrow.UINT16:
				uint16Array := column.(*array.Uint16)
				value = uint16Array.Value(int(rowIdx)) // 转换为 uint16
			case arrow.UINT32:
				uint32Array := column.(*array.Uint32)
				value = uint32Array.Value(int(rowIdx)) // 转换为 uint32
			case arrow.UINT64:
				uint64Array := column.(*array.Uint64)
				value = uint64Array.Value(int(rowIdx)) // 转换为 uint64
			case arrow.STRING:
				stringArray := column.(*array.String)
				value = stringArray.Value(int(rowIdx)) // 转换为 int
			case arrow.LARGE_STRING:
				largeStringArray := column.(*array.LargeString)
				value = largeStringArray.Value(int(rowIdx))
			case arrow.FLOAT32:
				float32Array := column.(*array.Float32)
				value = float32Array.Value(int(rowIdx)) // 转换为 float32
			case arrow.FLOAT64:
				float64Array := column.(*array.Float64)
				value = float64Array.Value(int(rowIdx)) // 转换为 float64
			case arrow.DECIMAL128:
				decimal128Array := column.(*array.Decimal128)
				decimalValue := decimal128Array.Value(int(rowIdx))
				value = decimalValue.ToFloat64(decimal128Array.DataType().(*arrow.Decimal128Type).Scale) // 转换为 float64
			case arrow.DATE32:
				date32Array := column.(*array.Date32)
				day := date32Array.Value(int(rowIdx))
				value = time.Date(1970, time.January, 1+int(day), 0, 0, 0, 0, time.UTC) // 转换为 time.Time
			case arrow.TIMESTAMP:
				timestampArray := column.(*array.Timestamp)
				timeUnit := timestampArray.DataType().(*arrow.TimestampType).Unit
				ts := timestampArray.Value(int(rowIdx)) // 获取时间戳值

				// ts 是 arrow.Timestamp 类型，我们需要将其转换为 int64
				// var t time.Time
				switch timeUnit {
				case arrow.Second:
					// arrow.Timestamp 是以秒为单位的时间戳
					t_str := time.Unix(int64(ts), 0).UTC().Format("2006-01-02 15:04:05")
					value = t_str
				case arrow.Millisecond:
					// 毫秒级时间戳需要乘以毫秒
					t_str := time.Unix(0, int64(ts)*int64(time.Millisecond)).UTC().Format("2006-01-02 15:04:05.000")
					value = t_str
				case arrow.Microsecond:
					// 微秒级时间戳需要乘以微秒
					t_str := time.Unix(0, int64(ts)*int64(time.Microsecond)).UTC().Format("2006-01-02 15:04:05.000000")
					value = t_str
				case arrow.Nanosecond:
					// 纳秒级时间戳直接使用
					t_str := time.Unix(0, int64(ts)).UTC().Format("2006-01-02 15:04:05.000000000")
					value = t_str
				default:
					return nil, fmt.Errorf("unsupported time unit: %v", timeUnit)
				}
			case arrow.BINARY:
				binaryArray := column.(*array.Binary)
				value = binaryArray.Value(int(rowIdx)) // 转换为 []byte
			default:
				return nil, fmt.Errorf("unsupported column type: %v", column.DataType().ID())
			}

			argsBatch = append(argsBatch, value)
		}
	}
	return argsBatch, nil
}
