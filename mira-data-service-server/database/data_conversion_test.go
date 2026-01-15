/*
*

	@author: shiliang
	@date: 2025/2/20
	@note:

*
*/
package database

import (
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArrowRowExtractor_ExtractRowData(t *testing.T) {
	// 构造 schema，包含 int32 和 string 两个字段
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "data", Type: arrow.BinaryTypes.Binary},
	}
	schema := arrow.NewSchema(fields, nil)

	// 创建内存分配器
	allocator := memory.DefaultAllocator

	// 构造 int32 列数据
	intBuilder := array.NewInt32Builder(allocator)
	defer intBuilder.Release()
	intBuilder.AppendValues([]int32{1, 2, 3}, nil)
	intArray := intBuilder.NewArray()
	defer intArray.Release()

	// 构造 string 列数据
	strBuilder := array.NewStringBuilder(allocator)
	defer strBuilder.Release()
	strBuilder.AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	strArray := strBuilder.NewArray()
	defer strArray.Release()

	// 构造 binary 列数据（字节数组）
	binaryBuilder := array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
	defer binaryBuilder.Release()
	binaryBuilder.AppendValues([][]byte{
		[]byte("data1"), []byte("data2"), []byte("data3"),
	}, nil)
	binaryArray := binaryBuilder.NewArray()
	defer binaryArray.Release()

	// 构造 record，注意第三个参数为行数
	record := array.NewRecord(schema, []arrow.Array{intArray, strArray, binaryArray}, 3)
	defer record.Release()

	// 使用 ArrowRowExtractor 进行数据转换
	extractor := ArrowRowExtractor{}
	result, err := extractor.ExtractRowData(record)
	assert.NoError(t, err)

	// 按行排列转换后的数据应为：[1, "Alice", 2, "Bob", 3, "Charlie"]
	expected := []interface{}{
		int32(1), "Alice", []byte("data1"),
		int32(2), "Bob", []byte("data2"),
		int32(3), "Charlie", []byte("data3"),
	}
	assert.Equal(t, expected, result)
}
