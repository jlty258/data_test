package common

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v15/arrow"
)

// ConvertDorisTypeToArrowType 将Doris数据类型转换为Arrow数据类型
func ConvertDorisTypeToArrowType(dataType string) (arrow.DataType, error) {
	// 转换为大写以匹配mysqlTypeToArrowType的逻辑
	dbTypeName := strings.ToUpper(strings.TrimSpace(dataType))

	switch dbTypeName {
	case "VARCHAR", "CHAR":
		return arrow.BinaryTypes.String, nil
	case "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT":
		return arrow.BinaryTypes.LargeString, nil
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, nil
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nil
	case "MEDIUMINT", "INT":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "UNSIGNED TINYINT":
		return arrow.PrimitiveTypes.Uint8, nil
	case "UNSIGNED SMALLINT":
		return arrow.PrimitiveTypes.Uint16, nil
	case "UNSIGNED MEDIUMINT", "UNSIGNED INT":
		return arrow.PrimitiveTypes.Uint32, nil
	case "UNSIGNED BIGINT":
		return arrow.PrimitiveTypes.Uint64, nil
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case "TIMESTAMP", "DATETIME":
		return arrow.BinaryTypes.String, nil
	case "DECIMAL", "NUMERIC":
		// 对于字符串类型的dataType，我们无法获取precision和scale
		// 使用默认的精度和标度
		return &arrow.Decimal128Type{Precision: 38, Scale: 10}, nil
	case "DATE":
		return arrow.BinaryTypes.String, nil
	default:
		return arrow.BinaryTypes.String, nil
	}
}

// convertArrowTypeToDorisType 将Arrow数据类型转换为Doris数据类型
func ConvertArrowTypeToDorisType(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INT"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "TINYINT UNSIGNED"
	case arrow.UINT16:
		return "SMALLINT UNSIGNED"
	case arrow.UINT32:
		return "INT UNSIGNED"
	case arrow.UINT64:
		return "BIGINT UNSIGNED"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING:
		return "VARCHAR(65533)"
	case arrow.LARGE_STRING:
		return "STRING"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.DATE32:
		return "DATE"
	case arrow.DATE64:
		return "DATETIME"
	case arrow.TIMESTAMP:
		return "DATETIME"
	case arrow.DECIMAL128:
		decimalType := arrowType.(*arrow.Decimal128Type)
		return fmt.Sprintf("DECIMAL(%d,%d)", decimalType.Precision, decimalType.Scale)
	case arrow.DECIMAL256:
		decimalType := arrowType.(*arrow.Decimal256Type)
		return fmt.Sprintf("DECIMAL(%d,%d)", decimalType.Precision, decimalType.Scale)
	default:
		return "VARCHAR(65533)" // 默认使用VARCHAR
	}
}

// ConvertSqlTypeToDorisType 将SQL类型转换为Doris类型
func ConvertSqlTypeToDorisType(colType *sql.ColumnType) string {
	typeName := strings.ToUpper(colType.DatabaseTypeName())

	switch typeName {
	case "TINYINT":
		return "TINYINT"
	case "SMALLINT":
		return "SMALLINT"
	case "INT", "INTEGER":
		return "INT"
	case "BIGINT":
		return "BIGINT"
	case "FLOAT":
		return "FLOAT"
	case "DOUBLE":
		return "DOUBLE"
	case "DECIMAL":
		if precision, scale, ok := colType.DecimalSize(); ok {
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
		return "DECIMAL(10,0)"
	case "VARCHAR", "TEXT":
		if length, ok := colType.Length(); ok && length > 0 && length <= 65533 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "VARCHAR(65533)"
	case "DATE":
		return "DATE"
	case "DATETIME", "TIMESTAMP":
		return "DATETIME"
	case "BOOLEAN", "BOOL":
		return "BOOLEAN"
	default:
		return "VARCHAR(65533)"
	}
}
