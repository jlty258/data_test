/*
*

	@author: shiliang
	@date: 2024/12/27
	@note:

*
*/
package utils

import (
	"data-service/log"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func setup() {
	viper.Set("LoggerConfig.Level", "debug") // 模拟配置项
	viper.Set("TestConfig", "true")
	viper.AutomaticEnv()        // 启用自动环境变量加载（如果有需要）
	viper.SetConfigType("yaml") // 确保配置类型是 yaml（如果依赖 yaml 类型）
	viper.SetConfigFile("")     // 禁用文件读取，避免默认路径读取配置文件

	log.InitLogger()
}

func TestMain(m *testing.M) {
	setup() // 调用 setup 方法
	m.Run() // 运行所有测试
}

func TestAppendDecimalValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"NilInput", nil, "0.0000000000"},
		{"ByteSliceInput", []byte("123.45"), "123.4500000000"},
		{"StringInput", "678.90", "678.9000000000"},
		{"OtherTypeInput", 123, "0.0000000000"},
		{"ByteSliceExceedPrecision", []byte("12345678901234567890123456789012345678901111111111"), "0.0000000000"},
		{"StringExceedPrecision", "1234567890123456789012345678901234567890", "0.0000000000"},
		{"LongDecimalInput", []byte("123.12345678901234567890"), "123.1234567890"},
		{"StringWithoutDecimal", "456", "456.0000000000"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			// 设置 Decimal128 的类型
			dt := arrow.Decimal128Type{Precision: 38, Scale: 10}
			b := array.NewDecimal128Builder(mem, &dt)
			defer b.Release()

			// 调用 AppendDecimalValue
			AppendDecimalValue(b, test.input)

			// 获取结果数组
			arr := b.NewArray().(*array.Decimal128)
			defer arr.Release()

			// 验证数组中的 Decimal128 值是否符合预期
			require.Equal(t, 1, arr.Len(), "Array length should be 1")
			assert.Equal(t, test.expected, arr.Value(0).ToString(dt.Scale), "Decimal128 value mismatch")
		})
	}
}

func TestAppendStringValue(t *testing.T) {
	allocator := memory.NewGoAllocator()

	tests := []struct {
		name     string
		val      interface{}
		expected string
	}{
		{"NilInput", nil, " "},                       // nil 转为空格
		{"ByteSliceInput", []byte("hello"), "hello"}, // []byte 转字符串
		{"IntInput", 123, "123"},                     // 整数转字符串
		{"StringInput", "world", "world"},            // 字符串本身
		{"BoolInput", true, "true"},                  // 布尔值转字符串
		{"UnsupportedType", map[string]interface{}{"key": "value"}, "unsupported"}, // 不支持类型（假设改为存储 "unsupported"）
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sb := array.NewStringBuilder(allocator)
			defer sb.Release()

			// 调用 AppendStringValue
			AppendStringValue(sb, test.val)

			// 检查 StringBuilder 内容
			strArray := sb.NewArray().(*array.String)
			defer strArray.Release()

			require.Equal(t, 1, strArray.Len(), "Array length should be 1")
			assert.Equal(t, test.expected, strArray.Value(0), "StringBuilder value mismatch")
		})
	}
}

func TestAppendTimestampValue(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expected    arrow.Timestamp
		unit        arrow.TimeUnit // 时间戳单位
		expectError bool           // 是否期望解析失败
	}{
		{"NilInput", nil, 0, arrow.Second, false},                                                       // nil 输入
		{"ValidTimeInput", time.Unix(1672531200, 0), 1672531200, arrow.Second, false},                   // 正确的 time.Time 输入
		{"ValidByteInput", []byte("2023-01-01 00:00:00"), 1672531200000000000, arrow.Nanosecond, false}, // 正确的 []byte 输入
		{"InvalidByteInput", []byte("invalid time"), 0, arrow.Second, true},                             // 无法解析的时间字符串
		{"UnsupportedType", 12345, 0, arrow.Second, false},                                              // 不支持的类型
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			// 设置 TimestampBuilder
			dt := &arrow.TimestampType{Unit: test.unit}
			b := array.NewTimestampBuilder(mem, dt)
			defer b.Release()

			// 调用 AppendTimestampValue
			AppendTimestampValue(b, test.input)

			// 获取结果数组
			arr := b.NewArray().(*array.Timestamp)
			defer arr.Release()

			// 验证数组的长度
			require.Equal(t, 1, arr.Len(), "Array length should be 1")

			// 验证结果是否匹配预期
			assert.Equal(t, test.expected, arr.Value(0), "Timestamp value mismatch")
		})
	}
}

func TestAppendDate32Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected *int32 // 使用指针，nil 表示 NULL
	}{
		{"NilInput", nil, ptrInt32(0)},                                                     // nil 输入
		{"ValidStringInput", "2024-10-10", ptrInt32(20006)},                                // 合法的字符串输入
		{"InvalidStringInput", "invalid-date", nil},                                        // 无法解析的字符串
		{"ValidTimeInput", time.Date(2024, 10, 10, 0, 0, 0, 0, time.UTC), ptrInt32(20006)}, // 合法的 time.Time 输入
		{"ValidByteInput", []byte("2024-10-10"), ptrInt32(20006)},                          // 合法的 []byte 输入
		{"InvalidByteInput", []byte("invalid-date"), nil},                                  // 无法解析的 []byte 输入
		{"UnsupportedType", 12345, ptrInt32(0)},                                            // 不支持的类型
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			// 创建 Date32Builder
			b := array.NewDate32Builder(mem)
			defer b.Release()

			// 调用 AppendDate32Value
			AppendDate32Value(b, test.input)

			// 获取结果数组
			arr := b.NewArray().(*array.Date32)
			defer arr.Release()

			// 验证数组的长度
			require.Equal(t, 1, arr.Len(), "Array length should be 1")

			// 验证结果是否匹配预期
			if test.expected == nil {
				// 如果期望值为 nil，则检查是否为 NULL
				assert.True(t, arr.IsNull(0), "Expected NULL value")
			} else {
				// 如果期望值不为 nil，则检查值是否正确
				assert.False(t, arr.IsNull(0), "Expected non-NULL value")
				assert.Equal(t, *test.expected, int32(arr.Value(0)), "Date32 value mismatch")
			}
		})
	}
}

// 辅助函数：创建 int32 的指针
func ptrInt32(val int32) *int32 {
	return &val
}

func TestAppendTime32Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected arrow.Time32 // 期望的秒数表示
	}{
		{"NilInput", nil, arrow.Time32(0)},                      // nil 输入，期望值为 00:00:00
		{"ValidStringInput", "16:38:06", arrow.Time32(59886)},   // 合法的字符串输入，计算秒数
		{"InvalidStringInput", "invalid-time", arrow.Time32(0)}, // 非法字符串输入，期望值为 00:00:00
		{"UnsupportedType", 12345, arrow.Time32(0)},             // 不支持的类型，期望值为 00:00:00
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			// 定义 Time32 数据类型，时间单位为秒
			dtype := &arrow.Time32Type{Unit: arrow.Second}

			// 创建 Time32Builder
			b := array.NewTime32Builder(mem, dtype)
			defer b.Release()

			// 调用 AppendTime32Value
			AppendTime32Value(b, test.input)

			// 获取结果数组
			arr := b.NewArray().(*array.Time32)
			defer arr.Release()

			// 验证数组的长度
			require.Equal(t, 1, arr.Len(), "Array length should be 1")

			// 验证结果是否匹配预期
			assert.Equal(t, test.expected, arr.Value(0), "Time32 value mismatch")
		})
	}
}

func TestAppendUint8Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected uint8
	}{
		{"NilInput", nil, uint8(0)},                 // nil 输入，期望值为 0
		{"ValidUint8Input", uint8(123), uint8(123)}, // uint8 输入，直接追加
		{"Int64ToUint8", int64(42), uint8(42)},      // int64 转换为 uint8
		{"InvalidType", "string", uint8(0)},         // 无效类型，默认值为 0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewUint8Builder(mem)
			defer b.Release()

			// 调用 AppendUint8Value
			AppendUint8Value(b, test.input)

			// 获取结果数组
			arr := b.NewArray().(*array.Uint8)
			defer arr.Release()

			// 验证数组长度
			require.Equal(t, 1, arr.Len(), "Array length should be 1")

			// 验证数组值
			assert.Equal(t, test.expected, arr.Value(0), "Uint8 value mismatch")
		})
	}
}

func TestAppendUint16Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected uint16
	}{
		{"NilInput", nil, uint16(0)},                       // nil 输入，期望值为 0
		{"ValidUint16Input", uint16(12345), uint16(12345)}, // uint16 输入，直接追加
		{"Int64ToUint16", int64(65535), uint16(65535)},     // int64 转换为 uint16
		{"InvalidType", "string", uint16(0)},               // 无效类型，默认值为 0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewUint16Builder(mem)
			defer b.Release()

			// 调用 AppendUint16Value
			AppendUint16Value(b, test.input)

			// 获取结果数组
			arr := b.NewArray().(*array.Uint16)
			defer arr.Release()

			// 验证数组长度
			require.Equal(t, 1, arr.Len(), "Array length should be 1")

			// 验证数组值
			assert.Equal(t, test.expected, arr.Value(0), "Uint16 value mismatch")
		})
	}
}

func TestAppendUint32Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected uint32
	}{
		{"NilInput", nil, uint32(0)},                               // nil 输入，期望值为 0
		{"ValidUint32Input", uint32(123456789), uint32(123456789)}, // uint32 输入，直接追加
		{"Int64ToUint32", int64(4294967295), uint32(4294967295)},   // int64 转换为 uint32
		{"InvalidType", "string", uint32(0)},                       // 无效类型，默认值为 0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewUint32Builder(mem)
			defer b.Release()

			// 调用 AppendUint32Value
			AppendUint32Value(b, test.input)

			// 获取结果数组
			arr := b.NewArray().(*array.Uint32)
			defer arr.Release()

			// 验证数组长度
			require.Equal(t, 1, arr.Len(), "Array length should be 1")

			// 验证数组值
			assert.Equal(t, test.expected, arr.Value(0), "Uint32 value mismatch")
		})
	}
}

func TestAppendUint64Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected uint64
	}{
		{"NilInput", nil, uint64(0)}, // nil 输入，期望值为 0
		{"ValidUint64Input", uint64(1234567890123456789), uint64(1234567890123456789)}, // uint64 输入，直接追加
		{"Int64ToUint64", int64(9223372036854775807), uint64(9223372036854775807)},     // int64 转换为 uint64
		{"InvalidType", "string", uint64(0)},                                           // 无效类型，默认值为 0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewUint64Builder(mem)
			defer b.Release()

			// 调用 AppendUint64Value
			AppendUint64Value(b, test.input)

			// 获取结果数组
			arr := b.NewArray().(*array.Uint64)
			defer arr.Release()

			// 验证数组长度
			require.Equal(t, 1, arr.Len(), "Array length should be 1")

			// 验证数组值
			assert.Equal(t, test.expected, arr.Value(0), "Uint64 value mismatch")
		})
	}
}

func TestAppendInt8Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int8
	}{
		{"NilInput", nil, int8(0)},             // nil 输入，默认值 0
		{"ValidInt8Input", int8(42), int8(42)}, // 合法的 int8 输入
		{"Int64ToInt8", int64(100), int8(100)}, // int64 转换为 int8
		{"InvalidType", "string", int8(0)},     // 无效类型，默认值 0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewInt8Builder(mem)
			defer b.Release()

			AppendInt8Value(b, test.input)

			arr := b.NewArray().(*array.Int8)
			defer arr.Release()

			require.Equal(t, 1, arr.Len(), "Array length should be 1")
			assert.Equal(t, test.expected, arr.Value(0), "Int8 value mismatch")
		})
	}
}

func TestAppendInt16Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int16
	}{
		{"NilInput", nil, int16(0)},                     // nil 输入，默认值 0
		{"ValidInt16Input", int16(12345), int16(12345)}, // 合法的 int16 输入
		{"Int64ToInt16", int64(30000), int16(30000)},    // int64 转换为 int16
		{"InvalidType", "string", int16(0)},             // 无效类型，默认值 0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewInt16Builder(mem)
			defer b.Release()

			AppendInt16Value(b, test.input)

			arr := b.NewArray().(*array.Int16)
			defer arr.Release()

			require.Equal(t, 1, arr.Len(), "Array length should be 1")
			assert.Equal(t, test.expected, arr.Value(0), "Int16 value mismatch")
		})
	}
}

func TestAppendInt32Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int32
	}{
		{"NilInput", nil, int32(0)},                             // nil 输入，默认值 0
		{"ValidInt32Input", int32(123456789), int32(123456789)}, // 合法的 int32 输入
		{"Int64ToInt32", int64(123456), int32(123456)},          // int64 转换为 int32
		{"InvalidType", "string", int32(0)},                     // 无效类型，默认值 0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewInt32Builder(mem)
			defer b.Release()

			AppendInt32Value(b, test.input)

			arr := b.NewArray().(*array.Int32)
			defer arr.Release()

			require.Equal(t, 1, arr.Len(), "Array length should be 1")
			assert.Equal(t, test.expected, arr.Value(0), "Int32 value mismatch")
		})
	}
}

func TestAppendInt64Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{"NilInput", nil, int64(0)}, // nil 输入，默认值 0
		{"ValidInt64Input", int64(9223372036854775807), int64(9223372036854775807)}, // 合法的 int64 输入
		{"InvalidType", "string", int64(0)},                                         // 无效类型，默认值 0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewInt64Builder(mem)
			defer b.Release()

			AppendInt64Value(b, test.input)

			arr := b.NewArray().(*array.Int64)
			defer arr.Release()

			require.Equal(t, 1, arr.Len(), "Array length should be 1")
			assert.Equal(t, test.expected, arr.Value(0), "Int64 value mismatch")
		})
	}
}

func TestAppendFloat32Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float32
	}{
		{"NilInput", nil, float32(0.0)},                          // nil 输入，默认值 0.0
		{"ValidFloat32Input", float32(3.14), float32(3.14)},      // 合法的 float32 输入
		{"Float64ToFloat32", float64(2.71828), float32(2.71828)}, // float64 转换为 float32
		{"InvalidType", "string", float32(0.0)},                  // 无效类型，默认值 0.0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewFloat32Builder(mem)
			defer b.Release()

			AppendFloat32Value(b, test.input)

			arr := b.NewArray().(*array.Float32)
			defer arr.Release()

			require.Equal(t, 1, arr.Len(), "Array length should be 1")
			assert.Equal(t, test.expected, arr.Value(0), "Float32 value mismatch")
		})
	}
}

func TestAppendFloat64Value(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
	}{
		{"NilInput", nil, float64(0.0)},                                       // nil 输入，默认值 0.0
		{"ValidFloat64Input", float64(3.14159265359), float64(3.14159265359)}, // 合法的 float64 输入
		{"InvalidType", "string", float64(0.0)},                               // 无效类型，默认值 0.0
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			b := array.NewFloat64Builder(mem)
			defer b.Release()

			AppendFloat64Value(b, test.input)

			arr := b.NewArray().(*array.Float64)
			defer arr.Release()

			require.Equal(t, 1, arr.Len(), "Array length should be 1")
			assert.Equal(t, test.expected, arr.Value(0), "Float64 value mismatch")
		})
	}
}
