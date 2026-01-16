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
// fieldTypes: 模板中指定的字段类型列表（可选），如 ["int", "varchar", "text"]
// maxFieldSize: 模板中指定的最大字段大小（可选），0表示使用默认值
func (m *DatabaseTypeMapper) GenerateSchema(tableName string, fieldCount int, rowCount int64, fieldTypes []string, maxFieldSize int) *SchemaDefinition {
	if fieldCount > 16 {
		fieldCount = 16
	}
	if fieldCount < 1 {
		fieldCount = 1
	}

	fields := m.generateFields(fieldCount, fieldTypes, maxFieldSize)
	return &SchemaDefinition{
		TableName: tableName,
		Fields:    fields,
		RowCount:  rowCount,
	}
}

// generateFields 生成字段定义
// fieldTypes: 模板中指定的字段类型列表（可选）
// maxFieldSize: 模板中指定的最大字段大小（可选），0表示使用默认值
func (m *DatabaseTypeMapper) generateFields(count int, fieldTypes []string, maxFieldSize int) []FieldType {
	rand.Seed(time.Now().UnixNano())
	fields := []FieldType{
		{Name: "id", SQLType: m.getIntType(), MaxSize: 0, Nullable: false}, // 主键
	}

	// 确定使用的类型池
	var typePool []string
	if len(fieldTypes) > 0 {
		// 如果模板指定了字段类型，则从模板类型映射到SQL类型
		typePool = m.mapTemplateTypesToSQLTypes(fieldTypes)
	} else {
		// 否则从完整的类型池中随机选择
		typePool = m.getFieldTypePool()
	}

	for i := 1; i < count; i++ {
		var fieldType string
		if len(fieldTypes) > 0 {
			// 如果模板指定了字段类型，按照顺序循环使用
			typeIndex := (i - 1) % len(typePool)
			fieldType = typePool[typeIndex]
		} else {
			// 如果没有指定字段类型，从类型池中随机选择
			fieldType = typePool[rand.Intn(len(typePool))]
		}

		// 计算MaxSize
		maxSize := m.getMaxSizeForType(fieldType)
		// 如果模板指定了maxFieldSize，则限制字段大小
		if maxFieldSize > 0 && maxSize > maxFieldSize {
			maxSize = maxFieldSize
			// 如果字段类型是VARCHAR，需要调整SQL类型
			if strings.Contains(fieldType, "VARCHAR") {
				fieldType = fmt.Sprintf("VARCHAR(%d)", maxFieldSize)
			}
		}

		fields = append(fields, FieldType{
			Name:     fmt.Sprintf("col_%d", i),
			SQLType:  fieldType,
			MaxSize:  maxSize,
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
			"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
			"FLOAT", "DOUBLE", "DECIMAL(10,2)", "DECIMAL(18,4)", "DECIMAL(38,6)",
			"CHAR(100)", "CHAR(255)", "VARCHAR(255)", "VARCHAR(512)", "VARCHAR(1024)",
			"TEXT", "MEDIUMTEXT",
			"DATE", "DATETIME", "TIMESTAMP",
			"BLOB", "TINYBLOB", "MEDIUMBLOB",
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

// mapTemplateTypesToSQLTypes 将模板中的简单类型名映射到具体的SQL类型
// 例如: "int" -> "INT", "varchar" -> "VARCHAR(255)"
func (m *DatabaseTypeMapper) mapTemplateTypesToSQLTypes(templateTypes []string) []string {
	typeMap := m.getTemplateTypeMap()
	sqlTypes := make([]string, 0, len(templateTypes))

	for _, templateType := range templateTypes {
		templateType = strings.ToLower(strings.TrimSpace(templateType))
		if sqlType, exists := typeMap[templateType]; exists {
			sqlTypes = append(sqlTypes, sqlType)
		} else {
			// 如果模板类型不在映射表中，使用默认类型
			sqlTypes = append(sqlTypes, m.getDefaultTypeForTemplateType(templateType))
		}
	}

	return sqlTypes
}

// getTemplateTypeMap 获取模板类型到SQL类型的映射表
func (m *DatabaseTypeMapper) getTemplateTypeMap() map[string]string {
	switch m.dbType {
	case "mysql":
		return map[string]string{
			"int":        "INT",
			"tinyint":    "TINYINT",
			"smallint":   "SMALLINT",
			"mediumint":  "MEDIUMINT",
			"bigint":     "BIGINT",
			"float":      "FLOAT",
			"double":     "DOUBLE",
			"decimal":    "DECIMAL(10,2)",
			"decimal18":  "DECIMAL(18,4)",
			"decimal38":  "DECIMAL(38,6)",
			"varchar":    "VARCHAR(255)",
			"char":       "CHAR(255)",
			"text":       "TEXT",
			"mediumtext": "MEDIUMTEXT",
			"date":       "DATE",
			"datetime":   "DATETIME",
			"timestamp":  "TIMESTAMP",
			"blob":       "BLOB",
			"tinyblob":   "TINYBLOB",
			"mediumblob": "MEDIUMBLOB",
		}
	case "kingbase":
		return map[string]string{
			"int":       "INTEGER",
			"smallint":  "SMALLINT",
			"bigint":    "BIGINT",
			"real":      "REAL",
			"double":    "DOUBLE PRECISION",
			"numeric":   "NUMERIC(10,2)",
			"decimal":   "NUMERIC(10,2)",
			"varchar":   "VARCHAR(255)",
			"char":      "CHAR(255)",
			"text":      "TEXT",
			"clob":      "CLOB",
			"date":      "DATE",
			"timestamp": "TIMESTAMP",
			"bytea":     "BYTEA",
		}
	case "gbase":
		return map[string]string{
			"int":       "INT",
			"tinyint":   "TINYINT",
			"smallint":  "SMALLINT",
			"bigint":    "BIGINT",
			"float":     "FLOAT",
			"double":    "DOUBLE",
			"decimal":   "DECIMAL(10,2)",
			"varchar":   "VARCHAR(255)",
			"char":      "CHAR(255)",
			"text":      "TEXT",
			"clob":      "CLOB",
			"date":      "DATE",
			"datetime":  "DATETIME",
			"timestamp": "TIMESTAMP",
			"blob":      "BLOB",
		}
	case "vastbase":
		return map[string]string{
			"int":       "INTEGER",
			"smallint":  "SMALLINT",
			"bigint":    "BIGINT",
			"real":      "REAL",
			"double":    "DOUBLE PRECISION",
			"numeric":   "NUMERIC(10,2)",
			"decimal":   "NUMERIC(10,2)",
			"varchar":   "VARCHAR(255)",
			"char":      "CHAR(255)",
			"text":      "TEXT",
			"date":      "DATE",
			"timestamp": "TIMESTAMP",
			"bytea":     "BYTEA",
		}
	default:
		return map[string]string{
			"int":     "INT",
			"varchar": "VARCHAR(255)",
			"text":    "TEXT",
		}
	}
}

// getDefaultTypeForTemplateType 为未知的模板类型返回默认SQL类型
func (m *DatabaseTypeMapper) getDefaultTypeForTemplateType(templateType string) string {
	// 根据模板类型名称猜测SQL类型
	if strings.Contains(templateType, "int") {
		return m.getIntType()
	}
	if strings.Contains(templateType, "char") || strings.Contains(templateType, "varchar") {
		return "VARCHAR(255)"
	}
	if strings.Contains(templateType, "text") {
		return "TEXT"
	}
	if strings.Contains(templateType, "decimal") || strings.Contains(templateType, "numeric") {
		return "DECIMAL(10,2)"
	}
	if strings.Contains(templateType, "date") || strings.Contains(templateType, "time") {
		return "DATETIME"
	}
	// 默认返回VARCHAR
	return "VARCHAR(255)"
}
