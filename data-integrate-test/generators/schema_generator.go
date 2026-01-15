package generators

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// FieldType 字段类型定义
type FieldType struct {
	Name     string
	SQLType  string // 数据库SQL类型
	MaxSize  int    // 最大字节数（0表示无限制）
	Nullable bool
}

// SchemaDefinition 表结构定义
type SchemaDefinition struct {
	TableName string
	Fields    []FieldType
	RowCount  int64 // 行数：1M, 50M, 100M
}

// DatabaseTypeMapper 数据库类型映射器
type DatabaseTypeMapper struct {
	dbType string // mysql, kingbase, gbase, vastbase
}

func NewDatabaseTypeMapper(dbType string) *DatabaseTypeMapper {
	return &DatabaseTypeMapper{dbType: dbType}
}

// GenerateSchema 生成表结构（最多16个字段）
func (m *DatabaseTypeMapper) GenerateSchema(tableName string, fieldCount int, rowCount int64) *SchemaDefinition {
	if fieldCount > 16 {
		fieldCount = 16
	}
	if fieldCount < 1 {
		fieldCount = 1
	}

	fields := m.generateFields(fieldCount)
	return &SchemaDefinition{
		TableName: tableName,
		Fields:    fields,
		RowCount:  rowCount,
	}
}

// generateFields 生成字段定义
func (m *DatabaseTypeMapper) generateFields(count int) []FieldType {
	rand.Seed(time.Now().UnixNano())
	fields := []FieldType{
		{Name: "id", SQLType: m.getIntType(), MaxSize: 0, Nullable: false}, // 主键
	}

	// 字段类型池（根据数据库类型）
	typePool := m.getFieldTypePool()

	for i := 1; i < count; i++ {
		fieldType := typePool[rand.Intn(len(typePool))]
		fields = append(fields, FieldType{
			Name:     fmt.Sprintf("col_%d", i),
			SQLType:  fieldType,
			MaxSize:  m.getMaxSizeForType(fieldType),
			Nullable: rand.Float32() < 0.3, // 30%概率可空
		})
	}

	return fields
}

// getFieldTypePool 根据数据库类型获取字段类型池
func (m *DatabaseTypeMapper) getFieldTypePool() []string {
	switch m.dbType {
	case "mysql":
		return []string{
			"TINYINT", "SMALLINT", "INT", "BIGINT",
			"FLOAT", "DOUBLE", "DECIMAL(10,2)",
			"VARCHAR(255)", "VARCHAR(512)", "VARCHAR(1024)",
			"TEXT", "MEDIUMTEXT",
			"DATE", "DATETIME", "TIMESTAMP",
			"BLOB",
		}
	case "kingbase":
		return []string{
			"SMALLINT", "INTEGER", "BIGINT",
			"REAL", "DOUBLE PRECISION", "NUMERIC(10,2)",
			"VARCHAR(255)", "VARCHAR(512)", "VARCHAR(1024)",
			"TEXT", "CLOB",
			"DATE", "TIMESTAMP",
			"BYTEA",
		}
	case "gbase":
		return []string{
			"TINYINT", "SMALLINT", "INT", "BIGINT",
			"FLOAT", "DOUBLE", "DECIMAL(10,2)",
			"VARCHAR(255)", "VARCHAR(512)", "VARCHAR(1024)",
			"TEXT", "CLOB",
			"DATE", "DATETIME", "TIMESTAMP",
			"BLOB",
		}
	case "vastbase":
		return []string{
			"SMALLINT", "INTEGER", "BIGINT",
			"REAL", "DOUBLE PRECISION", "NUMERIC(10,2)",
			"VARCHAR(255)", "VARCHAR(512)", "VARCHAR(1024)",
			"TEXT",
			"DATE", "TIMESTAMP",
			"BYTEA",
		}
	default:
		return []string{"VARCHAR(255)", "INT", "TEXT"}
	}
}

// getIntType 获取整数类型（根据数据库）
func (m *DatabaseTypeMapper) getIntType() string {
	switch m.dbType {
	case "mysql", "gbase":
		return "BIGINT"
	case "kingbase", "vastbase":
		return "BIGINT"
	default:
		return "BIGINT"
	}
}

// getMaxSizeForType 获取类型的最大字节数
func (m *DatabaseTypeMapper) getMaxSizeForType(sqlType string) int {
	if strings.Contains(sqlType, "VARCHAR") {
		// 提取VARCHAR中的数字
		var size int
		fmt.Sscanf(sqlType, "VARCHAR(%d)", &size)
		return size
	}
	if strings.Contains(sqlType, "CHAR") {
		return 255 // 默认
	}
	if strings.Contains(sqlType, "TEXT") || strings.Contains(sqlType, "CLOB") {
		return 1024 // 最大1024字节
	}
	if strings.Contains(sqlType, "BLOB") || strings.Contains(sqlType, "BYTEA") {
		return 1024
	}
	return 0 // 数值类型无限制
}

