package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"data-integrate-test/strategies"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// SnapshotExporter 快照导出器
type SnapshotExporter struct {
	outputDir string
}

// NewSnapshotExporter 创建快照导出器
func NewSnapshotExporter(outputDir string) *SnapshotExporter {
	return &SnapshotExporter{
		outputDir: outputDir,
	}
}

// ExportTableSnapshot 导出表的快照（表结构和数据）
func (se *SnapshotExporter) ExportTableSnapshot(
	ctx context.Context,
	strategy strategies.DatabaseStrategy,
	templateName string,
	tableName string,
) error {
	// 确保输出目录存在
	if err := os.MkdirAll(se.outputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}

	// 1. 导出表结构
	schemaFile := filepath.Join(se.outputDir, fmt.Sprintf("%s_%s_schema.sql", templateName, tableName))
	if err := se.exportTableSchema(ctx, strategy, tableName, schemaFile); err != nil {
		return fmt.Errorf("导出表结构失败: %v", err)
	}

	// 2. 导出表数据
	dataFile := filepath.Join(se.outputDir, fmt.Sprintf("%s_%s_data.csv", templateName, tableName))
	if err := se.exportTableData(ctx, strategy, tableName, dataFile); err != nil {
		return fmt.Errorf("导出表数据失败: %v", err)
	}

	fmt.Printf("✅ 成功导出表快照: %s\n", tableName)
	fmt.Printf("   表结构: %s\n", schemaFile)
	fmt.Printf("   表数据: %s\n", dataFile)

	return nil
}

// exportTableSchema 导出表结构（CREATE TABLE 语句）
func (se *SnapshotExporter) exportTableSchema(
	ctx context.Context,
	strategy strategies.DatabaseStrategy,
	tableName string,
	outputFile string,
) error {
	db := strategy.GetDB()
	dbType := strategy.GetDBType()

	var createTableSQL string
	var err error

	switch dbType {
	case "mysql", "gbase":
		createTableSQL, err = se.getMySQLCreateTable(ctx, db, tableName)
	case "kingbase", "vastbase":
		createTableSQL, err = se.getPostgreSQLCreateTable(ctx, db, tableName)
	default:
		return fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	if err != nil {
		return err
	}

	// 写入文件
	return os.WriteFile(outputFile, []byte(createTableSQL), 0644)
}

// getMySQLCreateTable 获取 MySQL 的 CREATE TABLE 语句
func (se *SnapshotExporter) getMySQLCreateTable(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)
	var table, createTable string
	err := db.QueryRowContext(ctx, query).Scan(&table, &createTable)
	if err != nil {
		return "", err
	}
	return createTable + ";\n", nil
}

// getPostgreSQLCreateTable 获取 PostgreSQL/KingBase/VastBase 的 CREATE TABLE 语句
func (se *SnapshotExporter) getPostgreSQLCreateTable(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	// 查询表结构信息
	query := `
		SELECT 
			column_name,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := db.QueryContext(ctx, query, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var columns []string
	var primaryKeys []string

	// 获取主键
	pkQuery := `
		SELECT column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		ON tc.constraint_name = kcu.constraint_name
		WHERE tc.table_schema = current_schema()
		AND tc.table_name = $1
		AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position
	`
	pkRows, err := db.QueryContext(ctx, pkQuery, tableName)
	if err == nil {
		defer pkRows.Close()
		for pkRows.Next() {
			var pkCol string
			if err := pkRows.Scan(&pkCol); err == nil {
				primaryKeys = append(primaryKeys, pkCol)
			}
		}
	}

	// 获取列信息
	for rows.Next() {
		var colName, dataType, isNullable, columnDefault sql.NullString
		var charMaxLength, numericPrecision, numericScale sql.NullInt64

		if err := rows.Scan(&colName, &dataType, &charMaxLength, &numericPrecision, &numericScale, &isNullable, &columnDefault); err != nil {
			return "", err
		}

		colDef := fmt.Sprintf(`"%s" %s`, colName.String, dataType.String)

		// 添加长度信息
		if charMaxLength.Valid && charMaxLength.Int64 > 0 {
			colDef += fmt.Sprintf("(%d)", charMaxLength.Int64)
		} else if numericPrecision.Valid && numericScale.Valid {
			colDef += fmt.Sprintf("(%d,%d)", numericPrecision.Int64, numericScale.Int64)
		} else if numericPrecision.Valid {
			colDef += fmt.Sprintf("(%d)", numericPrecision.Int64)
		}

		// 添加默认值
		if columnDefault.Valid && columnDefault.String != "" {
			colDef += " DEFAULT " + columnDefault.String
		}

		// 添加 NOT NULL
		if isNullable.String == "NO" {
			colDef += " NOT NULL"
		}

		columns = append(columns, colDef)
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	// 构建 CREATE TABLE 语句
	createTable := fmt.Sprintf("CREATE TABLE \"%s\" (\n", tableName)
	createTable += "  " + strings.Join(columns, ",\n  ")

	if len(primaryKeys) > 0 {
		// 为主键列名添加引号
		quotedPKs := make([]string, len(primaryKeys))
		for i, pk := range primaryKeys {
			quotedPKs[i] = fmt.Sprintf(`"%s"`, pk)
		}
		createTable += fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(quotedPKs, ", "))
	}

	createTable += "\n);\n"

	return createTable, nil
}

// exportTableData 导出表数据为 CSV（优化版，支持大数据量）
func (se *SnapshotExporter) exportTableData(
	ctx context.Context,
	strategy strategies.DatabaseStrategy,
	tableName string,
	outputFile string,
) error {
	db := strategy.GetDB()
	dbType := strategy.GetDBType()

	// 获取列名
	columns, err := se.getTableColumns(ctx, db, dbType, tableName)
	if err != nil {
		return fmt.Errorf("获取列信息失败: %v", err)
	}

	if len(columns) == 0 {
		return fmt.Errorf("表 %s 没有列", tableName)
	}

	// 构建查询语句（优化：使用流式查询）
	var query string
	switch dbType {
	case "mysql", "gbase":
		query = fmt.Sprintf("SELECT * FROM `%s`", tableName)
	case "kingbase", "vastbase":
		query = fmt.Sprintf(`SELECT * FROM "%s"`, tableName)
	default:
		return fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	// 执行查询（使用流式查询，避免一次性加载所有数据）
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("查询数据失败: %v", err)
	}
	defer rows.Close()

	// 创建 CSV 文件，使用带缓冲的文件写入
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()

	// 使用更大的缓冲区（8MB，适合超大数据量）
	bufferedWriter := bufio.NewWriterSize(file, 8*1024*1024) // 8MB 缓冲区
	defer bufferedWriter.Flush()

	// 使用字节数组存储分隔符，避免字符串转换开销
	fieldSeparator := []byte{0x01}      // \u0001
	lineSeparator := []byte{0xE2, 0x80, 0xA8} // \u2028 (UTF-8 编码)

	// 写入列头（直接使用字节操作）
	for i, col := range columns {
		if i > 0 {
			bufferedWriter.Write(fieldSeparator)
		}
		bufferedWriter.WriteString(col)
	}
	bufferedWriter.Write(lineSeparator)

	// 获取列类型
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("获取列类型失败: %v", err)
	}

	// 预分配切片，减少内存分配
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// 使用更大的批次（5000 行），减少写入次数
	batchSize := 5000
	rowCount := int64(0)
	startTime := time.Now()
	lastProgressTime := startTime
	lastProgressCount := int64(0)
	progressInterval := 5 * time.Second

	// 使用 bytes.Buffer 替代 strings.Builder（性能更好）
	var rowBuffer bytes.Buffer
	rowBuffer.Grow(1024) // 预分配 1KB，适合 16 个字段

	// 预分配格式化函数映射，避免类型断言开销
	formatters := make([]func(interface{}) []byte, len(columns))
	for i, colType := range columnTypes {
		formatters[i] = se.getFastFormatter(colType)
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("扫描行失败: %v", err)
		}

		// 直接写入缓冲区，避免字符串分配
		rowBuffer.Reset()
		for i, val := range values {
			if i > 0 {
				rowBuffer.Write(fieldSeparator)
			}
			// 使用优化的格式化函数，直接返回字节
			rowBuffer.Write(formatters[i](val))
		}
		rowBuffer.Write(lineSeparator)

		// 直接写入缓冲区，避免字符串拷贝
		if _, err := bufferedWriter.Write(rowBuffer.Bytes()); err != nil {
			return fmt.Errorf("写入行失败: %v", err)
		}

		rowCount++

		// 定期刷新缓冲区并输出进度
		if rowCount%int64(batchSize) == 0 {
			if err := bufferedWriter.Flush(); err != nil {
				return fmt.Errorf("刷新缓冲区失败: %v", err)
			}
		}

		// 定期输出进度（每 5 秒或每 10 万行）
		now := time.Now()
		if now.Sub(lastProgressTime) >= progressInterval || rowCount%100000 == 0 {
			elapsed := now.Sub(lastProgressTime).Seconds()
			if elapsed > 0 {
				rowsPerSec := float64(rowCount-lastProgressCount) / elapsed
				totalElapsed := now.Sub(startTime).Seconds()
				avgRowsPerSec := float64(rowCount) / totalElapsed
				fmt.Printf("  已导出 %s 行 | 当前速度: %s 行/秒 | 平均速度: %s 行/秒 | 已用时间: %s\n",
					formatNumber(rowCount),
					formatFloat(rowsPerSec),
					formatFloat(avgRowsPerSec),
					formatDuration(now.Sub(startTime)))
				lastProgressTime = now
				lastProgressCount = rowCount
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("读取行失败: %v", err)
	}

	fmt.Printf("  共导出 %s 行数据\n", formatNumber(rowCount))
	return nil
}

// getTableColumns 获取表的列名
func (se *SnapshotExporter) getTableColumns(ctx context.Context, db *sql.DB, dbType, tableName string) ([]string, error) {
	var query string
	switch dbType {
	case "mysql", "gbase":
		query = `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = DATABASE()
			AND table_name = ?
			ORDER BY ordinal_position
		`
	case "kingbase", "vastbase":
		query = `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			AND table_name = $1
			ORDER BY ordinal_position
		`
	default:
		return nil, fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	var rows *sql.Rows
	var err error
	if dbType == "mysql" || dbType == "gbase" {
		rows, err = db.QueryContext(ctx, query, tableName)
	} else {
		rows, err = db.QueryContext(ctx, query, tableName)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		columns = append(columns, colName)
	}

	return columns, rows.Err()
}

// formatValue 格式化值
// 注意：由于使用了 \u0001 作为字段分隔符和 \u2028 作为行分隔符，
// 这些字符在数据中出现的概率极低，所以不需要转义处理
func (se *SnapshotExporter) formatValue(val interface{}, columnType *sql.ColumnType) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case []byte:
		// 二进制数据转换为十六进制字符串（以 0x 开头）
		return fmt.Sprintf("0x%x", v)
	case string:
		// 字符串直接返回（\u0001 和 \u2028 在普通文本中几乎不会出现）
		return v
	case int64:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		// 其他类型转换为字符串
		return fmt.Sprintf("%v", v)
	}
}

// GetAllTables 获取数据库中的所有表
func (se *SnapshotExporter) GetAllTables(ctx context.Context, strategy strategies.DatabaseStrategy) ([]string, error) {
	db := strategy.GetDB()
	dbType := strategy.GetDBType()
	dbConfig := strategy.GetConnectionInfo()

	var query string
	switch dbType {
	case "mysql", "gbase":
		query = `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = ?
			AND table_type = 'BASE TABLE'
			ORDER BY table_name
		`
	case "kingbase", "vastbase":
		query = `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			AND table_type = 'BASE TABLE'
			ORDER BY table_name
		`
	default:
		return nil, fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	var rows *sql.Rows
	var err error
	if dbType == "mysql" || dbType == "gbase" {
		rows, err = db.QueryContext(ctx, query, dbConfig.Database)
	} else {
		rows, err = db.QueryContext(ctx, query)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

// getFastFormatter 获取高性能格式化函数，直接返回字节数组
func (se *SnapshotExporter) getFastFormatter(columnType *sql.ColumnType) func(interface{}) []byte {
	typeName := strings.ToUpper(columnType.DatabaseTypeName())
	
	// 根据数据库类型返回优化的格式化函数
	switch {
	case strings.Contains(typeName, "INT") || strings.Contains(typeName, "BIGINT"):
		return func(val interface{}) []byte {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case int64:
				return strconv.AppendInt(nil, v, 10)
			case int32:
				return strconv.AppendInt(nil, int64(v), 10)
			case int:
				return strconv.AppendInt(nil, int64(v), 10)
			default:
				return []byte(fmt.Sprintf("%v", v))
			}
		}
	case strings.Contains(typeName, "FLOAT") || strings.Contains(typeName, "DOUBLE") || strings.Contains(typeName, "DECIMAL") || strings.Contains(typeName, "NUMERIC"):
		return func(val interface{}) []byte {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case float64:
				return strconv.AppendFloat(nil, v, 'g', -1, 64)
			case float32:
				return strconv.AppendFloat(nil, float64(v), 'g', -1, 32)
			default:
				return []byte(fmt.Sprintf("%v", v))
			}
		}
	case strings.Contains(typeName, "BOOL"):
		return func(val interface{}) []byte {
			if val == nil {
				return nil
			}
			if v, ok := val.(bool); ok && v {
				return []byte("true")
			}
			return []byte("false")
		}
	case strings.Contains(typeName, "BLOB") || strings.Contains(typeName, "BYTEA") || strings.Contains(typeName, "BINARY"):
		return func(val interface{}) []byte {
			if val == nil {
				return nil
			}
			if v, ok := val.([]byte); ok {
				// 十六进制编码
				result := make([]byte, 2+len(v)*2)
				result[0] = '0'
				result[1] = 'x'
				hex.Encode(result[2:], v)
				return result
			}
			return []byte(fmt.Sprintf("%v", val))
		}
	default:
		// 字符串类型，使用优化的字符串转字节
		return func(val interface{}) []byte {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case string:
				// 使用 unsafe 转换，避免拷贝（仅当确定字符串不会被修改时）
				return unsafeStringToBytes(v)
			case []byte:
				return v
			default:
				return []byte(fmt.Sprintf("%v", v))
			}
		}
	}
}

// unsafeStringToBytes 高性能字符串转字节（零拷贝）
// 警告：返回的字节数组不能修改，且字符串必须保持有效
func unsafeStringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&struct {
		string
		int
	}{s, len(s)}))
}

// formatNumber 格式化大数字，添加千位分隔符（例如：1,234,567）
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	// 将数字转换为字符串
	str := fmt.Sprintf("%d", n)
	
	// 从右往左每3位添加一个逗号
	result := ""
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	
	return result
}

// formatFloat 格式化浮点数，保留2位小数
func formatFloat(f float64) string {
	if f >= 1000000 {
		return fmt.Sprintf("%.2fM", f/1000000)
	} else if f >= 1000 {
		return fmt.Sprintf("%.2fK", f/1000)
	}
	return fmt.Sprintf("%.2f", f)
}

// formatDuration 格式化时长
func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60
	
	if hours > 0 {
		return fmt.Sprintf("%d小时%d分%d秒", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%d分%d秒", minutes, seconds)
	}
	return fmt.Sprintf("%d秒", seconds)
}
