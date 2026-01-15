/*
*

	@author: shiliang
	@date: 2024/12/27
	@note: 存放所有 Arrow 数据构建器相关的工具函数

*
*/
package utils

import (
	"data-service/log"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
)

// 处理 string 类型的值
func AppendStringValue(b interface{}, val interface{}) {
	switch v := val.(type) {
	case nil:
		// nil 值转为空格
		switch b := b.(type) {
		case *array.StringBuilder:
			b.Append(" ")
		case *array.LargeStringBuilder:
			b.Append(" ")
		}
	case []byte:
		// []byte 转换为字符串
		switch b := b.(type) {
		case *array.StringBuilder:
			b.Append(string(v))
		case *array.LargeStringBuilder:
			b.Append(string(v))
		}
	case int, int8, int16, int32, int64:
		// 整数类型转为字符串
		switch b := b.(type) {
		case *array.StringBuilder:
			b.Append(fmt.Sprintf("%v", v))
		case *array.LargeStringBuilder:
			b.Append(fmt.Sprintf("%v", v))
		}
	case string:
		// 字符串本身
		switch b := b.(type) {
		case *array.StringBuilder:
			b.Append(v)
		case *array.LargeStringBuilder:
			b.Append(v)
		}
	case bool:
		// 布尔值转为 "true" 或 "false"
		switch b := b.(type) {
		case *array.StringBuilder:
			b.Append(fmt.Sprintf("%v", v))
		case *array.LargeStringBuilder:
			b.Append(fmt.Sprintf("%v", v))
		}
	case time.Time:
		// 根据时间是否包含时分秒来决定格式
		var timeStr string
		if v.Hour() == 0 && v.Minute() == 0 && v.Second() == 0 {
			// 只有日期部分，可能是 DATE 类型
			timeStr = v.Format("2006-01-02")
		} else {
			// 包含时间部分，可能是 TIMESTAMP 或 DATETIME 类型
			timeStr = v.Format("2006-01-02 15:04:05")
		}
		switch sb := b.(type) {
		case *array.StringBuilder:
			sb.Append(timeStr)
		case *array.LargeStringBuilder:
			sb.Append(timeStr)
		}
	default:
		// 不支持的类型转为 "unsupported"
		switch b := b.(type) {
		case *array.StringBuilder:
			b.Append("unsupported")
		case *array.LargeStringBuilder:
			b.Append("unsupported")
		}
	}
}

// 处理无符号整数类型的值
func AppendUint8Value(b *array.Uint8Builder, val interface{}) {
	switch v := val.(type) {
	case nil:
		b.Append(uint8(0)) // 默认值 0
	case uint8:
		b.Append(v)
	case int64:
		b.Append(uint8(v)) // 转换为 uint8
	default:
		b.Append(uint8(0)) // 默认值 0
	}
}

func AppendUint16Value(b *array.Uint16Builder, val interface{}) {
	switch v := val.(type) {
	case nil:
		b.Append(uint16(0)) // 默认值 0
	case uint16:
		b.Append(v)
	case int64:
		b.Append(uint16(v)) // 转换为 uint16
	default:
		b.Append(uint16(0)) // 默认值 0
	}
}

func AppendUint32Value(b *array.Uint32Builder, val interface{}) {
	switch v := val.(type) {
	case nil:
		b.Append(uint32(0)) // 默认值 0
	case uint32:
		b.Append(v)
	case int64:
		b.Append(uint32(v)) // 转换为 uint32
	default:
		b.Append(uint32(0)) // 默认值 0
	}
}

func AppendUint64Value(b *array.Uint64Builder, val interface{}) {
	switch v := val.(type) {
	case nil:
		b.Append(uint64(0)) // 默认值 0
	case uint64:
		b.Append(v)
	case int64:
		b.Append(uint64(v)) // 转换为 uint64
	default:
		b.Append(uint64(0)) // 默认值 0
	}
}

func AppendInt8Value(b *array.Int8Builder, val interface{}) {
	if val == nil {
		b.Append(int8(0)) // 给定默认值 0
	} else if intVal, ok := val.(int8); ok {
		b.Append(intVal)
	} else if intVal, ok := val.(int64); ok {
		b.Append(int8(intVal)) // 将 int64 转换为 int8
	} else {
		b.Append(int8(0)) // 默认值 0
	}
}

func AppendInt16Value(b *array.Int16Builder, val interface{}) {
	if val == nil {
		b.Append(int16(0)) // 给定默认值 0
	} else if intVal, ok := val.(int16); ok {
		b.Append(intVal)
	} else if intVal, ok := val.(int64); ok {
		b.Append(int16(intVal)) // 将 int64 转换为 int16
	} else {
		b.Append(int16(0)) // 默认值 0
	}
}

func AppendInt32Value(b *array.Int32Builder, val interface{}) {
	if val == nil {
		b.Append(int32(0)) // 默认值为 0
	} else if intValue, ok := val.(int32); ok {
		b.Append(intValue)
	} else if intValue, ok := val.(int64); ok {
		b.Append(int32(intValue)) // 尝试将 int64 转换为 int32
	} else {
		b.Append(int32(0)) // 默认值 0
	}
}

func AppendInt64Value(b *array.Int64Builder, val interface{}) {
	if val == nil {
		b.Append(int64(0)) // 默认值为 0
	} else if intValue, ok := val.(int64); ok {
		b.Append(intValue)
	} else {
		b.Append(int64(0)) // 默认值 0
	}
}

func AppendFloat32Value(b *array.Float32Builder, val interface{}) {
	if val == nil {
		b.Append(float32(0.0)) // 默认值 0.0
	} else if floatVal, ok := val.(float32); ok {
		b.Append(floatVal)
	} else if floatVal, ok := val.(float64); ok {
		b.Append(float32(floatVal)) // 将 float64 转换为 float32
	} else {
		b.Append(float32(0.0)) // 默认值 0.0
	}
}

func AppendFloat64Value(b *array.Float64Builder, val interface{}) {
	if val == nil {
		b.Append(float64(0.0)) // 默认值 0.0
	} else if floatValue, ok := val.(float64); ok {
		b.Append(floatValue)
	} else if floatValue, ok := val.(float32); ok {
		b.Append(float64(floatValue)) // 将 float32 转换为 float64
	} else {
		b.Append(float64(0.0)) // 默认值 0.0
	}
}

// 处理 Decimal128 类型的值
func AppendDecimalValue(b *array.Decimal128Builder, val interface{}) {
	switch v := val.(type) {
	case nil:
		_ = b.AppendValueFromString("0") // 默认值 "0"
	case []byte:
		decimalStr := normalizeDecimalString(string(v), b.Type().(*arrow.Decimal128Type).Scale)
		_ = b.AppendValueFromString(decimalStr)
	case string:
		decimalStr := normalizeDecimalString(v, b.Type().(*arrow.Decimal128Type).Scale)
		_ = b.AppendValueFromString(decimalStr)
	default:
		_ = b.AppendValueFromString("0") // 默认值 "0"
	}
}

// normalizeDecimalString 将输入的十进制字符串调整为符合 Scale 的格式
func normalizeDecimalString(input string, scale int32) string {
	// 如果没有小数点，直接追加 scale 个零并加上小数点
	parts := strings.Split(input, ".")
	if len(parts) == 1 {
		// 处理没有小数点的整数输入
		return input + "." + strings.Repeat("0", int(scale))
	}

	// 有小数点，拆分整数部分和小数部分
	intPart := parts[0]
	decPart := parts[1]

	// 根据 Scale 调整小数部分
	if len(decPart) < int(scale) {
		// 小数部分不足，补零
		decPart += strings.Repeat("0", int(scale)-len(decPart))
	} else {
		// 小数部分超出，截断
		decPart = decPart[:scale]
	}

	// 返回整数部分和小数部分
	return intPart + "." + decPart
}

// 处理时间类型的值
func AppendTimeValue(b *array.Time32Builder, val interface{}) {
	switch v := val.(type) {
	case nil:
		b.AppendNull()
	case string:
		parsedTime, err := time.Parse("15:04:05", v)
		if err != nil {
			b.Append(0) // 默认值
		} else {
			seconds := parsedTime.Hour()*3600 + parsedTime.Minute()*60 + parsedTime.Second()
			b.Append(arrow.Time32(seconds))
		}
	default:
		b.Append(0) // 默认值
	}
}

// 处理时间戳类型的值
func AppendTimestampValue(b *array.TimestampBuilder, val interface{}) {
	if val == nil {
		// 默认值为 0，即 1970-01-01T00:00:00Z
		b.Append(arrow.Timestamp(0))
		return
	}

	switch v := val.(type) {
	case time.Time:
		// 如果是 time.Time 类型，根据时间戳单位转换
		appendTimestampFromTime(b, v)
	case []byte:
		// 如果是 []byte 类型，尝试解析为字符串的时间戳
		appendTimestampFromBytes(b, v)
	default:
		// 不支持的类型，使用默认值
		b.Append(arrow.Timestamp(0))
	}
}

func appendTimestampFromTime(b *array.TimestampBuilder, timeVal time.Time) {
	unit := b.Type().(*arrow.TimestampType).Unit
	switch unit {
	case arrow.Second:
		// 转换为秒级时间戳
		b.Append(arrow.Timestamp(timeVal.Unix()))
	case arrow.Millisecond:
		// 转换为毫秒级时间戳
		b.Append(arrow.Timestamp(timeVal.UnixNano() / 1e6))
	case arrow.Microsecond:
		// 转换为微秒级时间戳
		b.Append(arrow.Timestamp(timeVal.UnixNano() / 1e3))
	case arrow.Nanosecond:
		// 转换为纳秒级时间戳
		b.Append(arrow.Timestamp(timeVal.UnixNano()))
	default:
		// 默认时间戳为 0
		b.Append(arrow.Timestamp(0))
	}
}

func appendTimestampFromBytes(b *array.TimestampBuilder, byteVal []byte) {
	strVal := string(byteVal)
	// 解析字符串为时间戳，格式为 "2006-01-02 15:04:05"
	parsedTime, err := time.Parse("2006-01-02 15:04:05", strVal)
	if err == nil {
		// 如果解析成功，将时间戳的纳秒数添加到 builder
		b.Append(arrow.Timestamp(parsedTime.UnixNano()))
	} else {
		// 如果解析失败，记录警告日志，并添加默认值 0
		log.Logger.Warnf("Failed to parse TIMESTAMP: %s", strVal)
		b.Append(arrow.Timestamp(0))
	}
}

func AppendDate32Value(b *array.Date32Builder, val interface{}) {
	if val == nil {
		// 默认值为 1970-01-01
		b.Append(arrow.Date32(0))
		return
	}

	switch v := val.(type) {
	case string:
		// 解析字符串类型的日期
		appendDate32FromString(b, v)
	case time.Time:
		// 从 time.Time 类型转换为 Date32
		appendDate32FromTime(b, v)
	case []byte:
		// 从字节切片（[]byte）解析为字符串，再处理为日期
		appendDate32FromString(b, string(v))
	default:
		// 不支持的类型，记录警告日志并使用默认值
		log.Logger.Warnf("Invalid type for Date32Builder, appended default value 0")
		b.Append(arrow.Date32(0))
	}
}

func appendDate32FromString(b *array.Date32Builder, strVal string) {
	// 强制使用 UTC 时区解析日期字符串
	location := time.UTC
	parsedDate, err := time.ParseInLocation("2006-01-02", strVal, location)
	if err != nil {
		log.Logger.Errorf("Failed to parse date string: %s, error: %v", strVal, err)
		b.AppendNull()
		return
	}

	// 计算从 1970-01-01 到指定日期的天数
	date32 := int32(parsedDate.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24)
	b.Append(arrow.Date32(date32))
}

func appendDate32FromTime(b *array.Date32Builder, timeVal time.Time) {
	// 计算从 1970-01-01 到指定日期的天数
	date32 := int32(timeVal.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24)
	b.Append(arrow.Date32(date32))
}

func AppendTime32Value(b *array.Time32Builder, val interface{}) {
	if val == nil {
		// 如果值是 nil，则追加默认值 00:00:00
		b.Append(arrow.Time32(0))
		return
	}

	switch v := val.(type) {
	case string:
		// 处理字符串类型的时间
		appendTime32FromString(b, v)
	default:
		// 不支持的类型，追加默认值 00:00:00
		b.Append(arrow.Time32(0))
	}
}

func appendTime32FromString(b *array.Time32Builder, strVal string) {
	// 按 "15:04:05" 格式解析字符串
	parsedTime, err := time.Parse("15:04:05", strVal)
	if err != nil {
		// 如果解析失败，追加默认值 00:00:00
		log.Logger.Errorf("Failed to parse time string: %s, error: %v", strVal, err)
		b.Append(arrow.Time32(0))
		return
	}

	// 计算从 00:00:00 开始的秒数
	secondsSinceMidnight := parsedTime.Hour()*3600 + parsedTime.Minute()*60 + parsedTime.Second()
	b.Append(arrow.Time32(secondsSinceMidnight))
}

func AppendValueToBuilder(b array.Builder, val interface{}) error {
	switch b := b.(type) {
	case *array.StringBuilder:
		AppendStringValue(b, val)
	case *array.LargeStringBuilder:
		AppendStringValue(b, val)
	case *array.Uint8Builder:
		AppendUint8Value(b, val)
	case *array.Uint16Builder:
		AppendUint16Value(b, val)
	case *array.Uint32Builder:
		AppendUint32Value(b, val)
	case *array.Uint64Builder:
		AppendUint64Value(b, val)
	case *array.Int8Builder:
		AppendInt8Value(b, val)
	case *array.Int16Builder:
		AppendInt16Value(b, val)
	case *array.Int32Builder:
		AppendInt32Value(b, val)
	case *array.Int64Builder:
		AppendInt64Value(b, val)
	case *array.Time32Builder:
		AppendStringValue(b, val)
	case *array.TimestampBuilder:
		AppendStringValue(b, val)
	case *array.Float32Builder:
		AppendFloat32Value(b, val)
	case *array.Float64Builder:
		AppendFloat64Value(b, val)
	case *array.Decimal128Builder:
		AppendDecimalValue(b, val)
	case *array.Date32Builder:
		AppendStringValue(b, val)
	default:
		return fmt.Errorf("unsupported builder type: %T", b)
	}
	return nil
}

func ExtractRowData(record arrow.Record) ([][]interface{}, error) {
	var rows [][]interface{}
	numRows := record.NumRows() // 获取行数

	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ { // 使用 int64
		rowData := []interface{}{}
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			column := record.Column(colIdx)
			var value interface{}

			switch column.DataType().ID() {
			case arrow.INT32:
				int32Array := column.(*array.Int32)
				value = int32Array.Value(int(rowIdx)) // 转换为 int
			case arrow.INT64:
				int64Array := column.(*array.Int64)
				value = int64Array.Value(int(rowIdx)) // 转换为 int64
			case arrow.STRING:
				stringArray := column.(*array.String)
				value = stringArray.Value(int(rowIdx)) // 转换为 string
			case arrow.FLOAT64:
				float64Array := column.(*array.Float64)
				value = float64Array.Value(int(rowIdx)) // 转换为 float64
			case arrow.DATE32:
				date32Array := column.(*array.Date32)
				daysSinceEpoch := date32Array.Value(int(rowIdx))
				timestamp := time.Unix(int64(daysSinceEpoch)*86400, 0).UTC()
				formattedDate := timestamp.Format("2006-01-02") // 格式化为 YYYY-MM-DD
				value = formattedDate
			case arrow.TIMESTAMP:
				timestampArray := column.(*array.Timestamp)
				timestampValue := timestampArray.Value(int(rowIdx))
				unit := timestampArray.DataType().(*arrow.TimestampType).Unit

				// 根据时间戳的单位进行转换
				switch unit {
				case arrow.Second:
					timestamp := time.Unix(int64(timestampValue), 0).UTC()
					formattedTimestamp := timestamp.Format("2006-01-02 15:04:05")
					value = formattedTimestamp
				case arrow.Millisecond:
					timestamp := time.Unix(0, int64(timestampValue*1e6)).UTC()
					formattedTimestamp := timestamp.Format("2006-01-02 15:04:05.000")
					value = formattedTimestamp
				case arrow.Microsecond:
					timestamp := time.Unix(0, int64(timestampValue*1e3)).UTC()
					formattedTimestamp := timestamp.Format("2006-01-02 15:04:05.000000")
					value = formattedTimestamp
				case arrow.Nanosecond:
					timestamp := time.Unix(0, int64(timestampValue)).UTC()
					formattedTimestamp := timestamp.Format("2006-01-02 15:04:05.000000000")
					value = formattedTimestamp
				default:
					return nil, fmt.Errorf("unsupported timestamp unit: %v", unit)
				}
			// 可以根据需要添加更多类型的支持
			default:
				return nil, fmt.Errorf("unsupported column type: %v", column.DataType().ID())
			}

			rowData = append(rowData, value)
		}
		rows = append(rows, rowData)
	}
	return rows, nil
}

// PrintRecord 打印 Arrow Record 的数据
func PrintRecord(record arrow.Record) error {
	// 检查 record 是否为 nil
	if record == nil {
		log.Logger.Warn("Received nil record, skipping print")
		return nil
	}

	// 检查 schema 是否为 nil
	if record.Schema() == nil {
		log.Logger.Warn("Record schema is nil, skipping print")
		return nil
	}

	// 打印上分割线和开始信息
	log.Logger.Info("---------------------------- Start Print Record ----------------------------")
	log.Logger.Infof("Total Rows: %d, Total Columns: %d", record.NumRows(), record.NumCols())

	// 打印 Record 的 schema 信息
	log.Logger.Infof("Record schema: %s", record.Schema())

	// 提取每一行的数据
	rows, err := ExtractRowData(record)
	if err != nil {
		return err
	}

	// 检查 rows 是否为空
	if len(rows) == 0 {
		log.Logger.Info("No rows to print")
		// 打印下分割线和结束信息
		log.Logger.Info("---------------------------- End Print Record (Empty) ----------------------------")
		return nil
	}

	// 打印每一行的数据
	for rowIndex, row := range rows {
		log.Logger.Infof("Row %d: ", rowIndex+1)
		for colIndex, value := range row {
			// 检查 value 是否为 nil
			if value == nil {
				log.Logger.Infof("%s=nil ", record.ColumnName(colIndex))
			} else {
				log.Logger.Infof("%s=%v ", record.ColumnName(colIndex), value)
			}
		}
	}

	// 打印下分割线和结束信息
	log.Logger.Info("---------------------------- End Print Record ----------------------------")

	return nil
}
