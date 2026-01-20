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
)

// SnapshotImporter 快照导入器
type SnapshotImporter struct {
	batchSize int // 批量插入大小
}

// NewSnapshotImporter 创建快照导入器
func NewSnapshotImporter(batchSize int) *SnapshotImporter {
	if batchSize <= 0 {
		batchSize = 5000 // 默认 5000 行一批
	}
	return &SnapshotImporter{
		batchSize: batchSize,
	}
}

// ImportTableSnapshot 导入表的快照（表结构和数据）
func (si *SnapshotImporter) ImportTableSnapshot(
	ctx context.Context,
	strategy strategies.DatabaseStrategy,
	schemaFile string,
	dataFile string,
	tableName string,
) error {
	// 1. 导入表结构
	if err := si.importTableSchema(ctx, strategy, schemaFile, tableName); err != nil {
		return fmt.Errorf("导入表结构失败: %v", err)
	}

	// 2. 导入表数据
	if err := si.importTableData(ctx, strategy, dataFile, tableName); err != nil {
		return fmt.Errorf("导入表数据失败: %v", err)
	}

	fmt.Printf("✅ 成功导入表快照: %s\n", tableName)
	return nil
}

// importTableSchema 导入表结构
func (si *SnapshotImporter) importTableSchema(
	ctx context.Context,
	strategy strategies.DatabaseStrategy,
	schemaFile string,
	tableName string,
) error {
	// 读取 SQL 文件
	sqlBytes, err := os.ReadFile(schemaFile)
	if err != nil {
		return fmt.Errorf("读取表结构文件失败: %v", err)
	}

	sqlStr := string(sqlBytes)
	dbType := strategy.GetDBType()

	// 根据数据库类型调整 SQL
	adjustedSQL := si.adjustCreateTableSQL(sqlStr, dbType, tableName)

	// 执行 CREATE TABLE
	db := strategy.GetDB()
	if _, err := db.ExecContext(ctx, adjustedSQL); err != nil {
		return fmt.Errorf("执行 CREATE TABLE 失败: %v", err)
	}

	fmt.Printf("  表结构导入成功: %s\n", tableName)
	return nil
}

// adjustCreateTableSQL 调整 CREATE TABLE SQL 以适应目标数据库
func (si *SnapshotImporter) adjustCreateTableSQL(sqlStr string, dbType string, targetTableName string) string {
	// 替换表名
	// 查找原始表名（在 CREATE TABLE 后面）
	lines := strings.Split(sqlStr, "\n")
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(line)), "CREATE TABLE") {
			// 提取原始表名并替换
			originalTableName := si.extractTableNameFromCreateTable(line)
			if originalTableName != "" {
				lines[i] = strings.Replace(line, originalTableName, targetTableName, 1)
			} else {
				// 如果无法提取，直接替换
				lines[i] = strings.Replace(line, "CREATE TABLE", fmt.Sprintf("CREATE TABLE %s", si.quoteIdentifier(targetTableName, dbType)), 1)
			}
			break
		}
	}

	// 根据数据库类型调整语法
	adjustedSQL := strings.Join(lines, "\n")
	
	switch dbType {
	case "mysql", "gbase":
		// MySQL/GBase 语法基本不需要调整
		// 但需要确保表名使用反引号
		adjustedSQL = strings.ReplaceAll(adjustedSQL, `"`, "`")
	case "kingbase", "vastbase":
		// KingBase/VastBase 使用双引号
		adjustedSQL = strings.ReplaceAll(adjustedSQL, "`", `"`)
	}

	return adjustedSQL
}

// extractTableNameFromCreateTable 从 CREATE TABLE 语句中提取表名
func (si *SnapshotImporter) extractTableNameFromCreateTable(line string) string {
	// 简单提取：CREATE TABLE `table_name` 或 CREATE TABLE "table_name"
	line = strings.TrimSpace(line)
	if idx := strings.Index(line, "CREATE TABLE"); idx >= 0 {
		remainder := line[idx+12:] // "CREATE TABLE" 长度
		remainder = strings.TrimSpace(remainder)
		
		// 提取引号内的表名
		if strings.HasPrefix(remainder, "`") {
			if endIdx := strings.Index(remainder[1:], "`"); endIdx >= 0 {
				return remainder[:endIdx+2]
			}
		} else if strings.HasPrefix(remainder, `"`) {
			if endIdx := strings.Index(remainder[1:], `"`); endIdx >= 0 {
				return remainder[:endIdx+2]
			}
		} else {
			// 没有引号，提取到第一个空格或左括号
			if spaceIdx := strings.IndexAny(remainder, " ("); spaceIdx >= 0 {
				return remainder[:spaceIdx]
			}
		}
	}
	return ""
}

// quoteIdentifier 引用标识符（根据数据库类型）
func (si *SnapshotImporter) quoteIdentifier(name string, dbType string) string {
	switch dbType {
	case "mysql", "gbase":
		return fmt.Sprintf("`%s`", name)
	case "kingbase", "vastbase":
		return fmt.Sprintf(`"%s"`, name)
	default:
		return name
	}
}

// importTableData 导入表数据（使用数据库原生工具，最高性能）
func (si *SnapshotImporter) importTableData(
	ctx context.Context,
	strategy strategies.DatabaseStrategy,
	dataFile string,
	tableName string,
) error {
	db := strategy.GetDB()
	dbType := strategy.GetDBType()

	// 读取列头
	file, err := os.Open(dataFile)
	if err != nil {
		return fmt.Errorf("打开数据文件失败: %v", err)
	}
	defer file.Close()

	// 读取第一行（列头）
	reader := bufio.NewReader(file)
	headerLine, _, err := reader.ReadLine()
	if err != nil {
		return fmt.Errorf("读取列头失败: %v", err)
	}

	// 解析列名
	columns := strings.Split(string(headerLine), "\u0001")
	if len(columns) == 0 {
		return fmt.Errorf("未找到列名")
	}

	// 根据数据库类型选择最优导入方式
	switch dbType {
	case "mysql", "gbase":
		return si.importWithLoadDataInfile(ctx, db, dataFile, tableName, columns)
	case "kingbase", "vastbase":
		return si.importWithCopy(ctx, db, dataFile, tableName, columns)
	default:
		return fmt.Errorf("不支持的数据库类型: %s", dbType)
	}
}

// importWithLoadDataInfile MySQL/GBase 使用 LOAD DATA INFILE（最高性能）
func (si *SnapshotImporter) importWithLoadDataInfile(
	ctx context.Context,
	db *sql.DB,
	dataFile string,
	tableName string,
	columns []string,
) error {
	// 构建 LOAD DATA INFILE 语句
	// 注意：文件路径需要是绝对路径，且数据库需要有 FILE 权限
	absPath, err := filepath.Abs(dataFile)
	if err != nil {
		return fmt.Errorf("获取绝对路径失败: %v", err)
	}

	// 构建列名列表
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = fmt.Sprintf("`%s`", col)
	}

	// LOAD DATA INFILE 语法
	// 字段分隔符: \u0001 (0x01)
	// 行分隔符: \u2028 (需要特殊处理，MySQL 不支持，需要预处理文件)
	// 由于 MySQL 的 LOAD DATA INFILE 不支持 \u2028，我们需要：
	// 1. 临时转换文件（将 \u2028 替换为 \n）
	// 2. 或使用 LOCAL INFILE（客户端加载）

	// 方案1：使用 LOCAL INFILE（推荐，不需要文件在服务器上）
	loadSQL := fmt.Sprintf(`LOAD DATA LOCAL INFILE '%s'
		INTO TABLE %s
		FIELDS TERMINATED BY 0x01
		LINES TERMINATED BY 0x0A
		IGNORE 1 LINES
		(%s)`,
		strings.ReplaceAll(absPath, "\\", "/"), // 统一使用 / 分隔符
		fmt.Sprintf("`%s`", tableName),
		strings.Join(quotedColumns, ","))

	// 注意：需要先预处理文件，将 \u2028 替换为 \n
	// 或者使用临时文件
	tempFile, err := si.preprocessCSVFile(dataFile)
	if err != nil {
		return fmt.Errorf("预处理文件失败: %v", err)
	}
	defer os.Remove(tempFile) // 清理临时文件

	absTempPath, _ := filepath.Abs(tempFile)
	loadSQL = fmt.Sprintf(`LOAD DATA LOCAL INFILE '%s'
		INTO TABLE %s
		FIELDS TERMINATED BY 0x01
		LINES TERMINATED BY 0x0A
		IGNORE 1 LINES
		(%s)`,
		strings.ReplaceAll(absTempPath, "\\", "/"),
		fmt.Sprintf("`%s`", tableName),
		strings.Join(quotedColumns, ","))

	startTime := time.Now()
	fmt.Printf("  开始导入数据（使用 LOAD DATA LOCAL INFILE）...\n")

	// 执行 LOAD DATA
	_, err = db.ExecContext(ctx, loadSQL)
	if err != nil {
		return fmt.Errorf("LOAD DATA INFILE 失败: %v", err)
	}

	// 获取导入的行数
	var rowCount int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	if err := db.QueryRowContext(ctx, countSQL).Scan(&rowCount); err == nil {
		elapsed := time.Since(startTime)
		fmt.Printf("  共导入 %s 行数据 | 耗时: %s | 速度: %s 行/秒\n",
			formatNumber(rowCount),
			formatDuration(elapsed),
			formatFloat(float64(rowCount)/elapsed.Seconds()))
	}

	return nil
}

// importWithCopy PostgreSQL/KingBase/VastBase 使用 COPY（最高性能）
func (si *SnapshotImporter) importWithCopy(
	ctx context.Context,
	db *sql.DB,
	dataFile string,
	tableName string,
	columns []string,
) error {
	// 构建 COPY 语句
	// PostgreSQL COPY 支持自定义分隔符
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = fmt.Sprintf(`"%s"`, col)
	}

	// COPY 语法
	// 字段分隔符: E'\x01' (0x01)
	// 行分隔符: E'\u2028' (PostgreSQL 支持 Unicode 转义)
	copySQL := fmt.Sprintf(`COPY %s (%s)
		FROM STDIN
		WITH (
			FORMAT csv,
			DELIMITER E'\x01',
			NULL '',
			HEADER true
		)`,
		fmt.Sprintf(`"%s"`, tableName),
		strings.Join(quotedColumns, ","))

	// 注意：PostgreSQL 的 COPY FROM STDIN 需要通过特殊协议
	// 标准 database/sql 不支持，需要使用 lib/pq 的 CopyIn
	// 或者预处理文件后使用 COPY FROM file

	// 方案：预处理文件，使用 COPY FROM file
	tempFile, err := si.preprocessCSVFileForPostgreSQL(dataFile)
	if err != nil {
		return fmt.Errorf("预处理文件失败: %v", err)
	}
	defer os.Remove(tempFile)

	// 获取文件绝对路径（PostgreSQL 需要服务器端路径）
	absPath, err := filepath.Abs(tempFile)
	if err != nil {
		return fmt.Errorf("获取绝对路径失败: %v", err)
	}

	// 使用 COPY FROM file（需要 superuser 权限）
	// 或者使用 COPY FROM STDIN（需要特殊驱动支持）
	// 这里使用简化方案：预处理后使用标准 COPY
	copySQL = fmt.Sprintf(`COPY %s (%s)
		FROM '%s'
		WITH (
			FORMAT csv,
			DELIMITER E'\x01',
			NULL '',
			HEADER true
		)`,
		fmt.Sprintf(`"%s"`, tableName),
		strings.Join(quotedColumns, ","),
		strings.ReplaceAll(absPath, "\\", "/"))

	startTime := time.Now()
	fmt.Printf("  开始导入数据（使用 COPY）...\n")

	_, err = db.ExecContext(ctx, copySQL)
	if err != nil {
		// 如果 COPY FROM file 失败（权限问题），回退到批量 INSERT
		fmt.Printf("  ⚠️  COPY FROM file 失败（可能需要 superuser 权限），回退到批量 INSERT: %v\n", err)
		return si.importWithBatchInsert(ctx, db, dataFile, tableName, columns)
	}

	// 获取导入的行数
	var rowCount int64
	countSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)
	if err := db.QueryRowContext(ctx, countSQL).Scan(&rowCount); err == nil {
		elapsed := time.Since(startTime)
		fmt.Printf("  共导入 %s 行数据 | 耗时: %s | 速度: %s 行/秒\n",
			formatNumber(rowCount),
			formatDuration(elapsed),
			formatFloat(float64(rowCount)/elapsed.Seconds()))
	}

	return nil
}

// preprocessCSVFile 预处理 CSV 文件（将 \u2028 替换为 \n，用于 MySQL）
func (si *SnapshotImporter) preprocessCSVFile(dataFile string) (string, error) {
	// 创建临时文件
	tempFile := dataFile + ".tmp"
	
	// 读取原文件
	content, err := os.ReadFile(dataFile)
	if err != nil {
		return "", err
	}

	// 将 \u2028 替换为 \n
	content = bytes.ReplaceAll(content, []byte{0xE2, 0x80, 0xA8}, []byte{0x0A})

	// 写入临时文件
	if err := os.WriteFile(tempFile, content, 0644); err != nil {
		return "", err
	}

	return tempFile, nil
}

// preprocessCSVFileForPostgreSQL 预处理 CSV 文件（用于 PostgreSQL）
func (si *SnapshotImporter) preprocessCSVFileForPostgreSQL(dataFile string) (string, error) {
	// PostgreSQL COPY 可以直接使用 \u2028，但为了兼容性，也转换为 \n
	return si.preprocessCSVFile(dataFile)
}

// importWithBatchInsert 批量 INSERT 导入（回退方案）
func (si *SnapshotImporter) importWithBatchInsert(
	ctx context.Context,
	db *sql.DB,
	dataFile string,
	tableName string,
	columns []string,
) error {
	// 读取文件内容
	fileContent, err := os.ReadFile(dataFile)
	if err != nil {
		return fmt.Errorf("读取数据文件失败: %v", err)
	}

	// 解析数据
	contentStr := string(fileContent)
	lineSeparator := "\u2028"
	lines := strings.Split(contentStr, lineSeparator)

	if len(lines) == 0 {
		return fmt.Errorf("数据文件为空")
	}

	// 跳过第一行（列头）
	if len(lines) > 0 {
		lines = lines[1:]
	}

	// 获取列类型
	columnTypes, err := si.getColumnTypes(ctx, db, "mysql", tableName, columns)
	if err != nil {
		return fmt.Errorf("获取列类型失败: %v", err)
	}

	// 开始事务
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	// 批量插入
	batch := make([][]interface{}, 0, si.batchSize)
	rowCount := int64(0)
	startTime := time.Now()
	fieldSeparator := []byte{0x01}

	insertSQL := si.buildInsertSQL(tableName, columns, "mysql")

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		rowData := si.parseCSVLine([]byte(line), fieldSeparator, columns, columnTypes)
		if rowData != nil {
			batch = append(batch, rowData)

			if len(batch) >= si.batchSize {
				if err := si.executeBatchInsert(ctx, tx, insertSQL, batch, columns); err != nil {
					return fmt.Errorf("批量插入失败: %v", err)
				}
				rowCount += int64(len(batch))
				batch = batch[:0]

				if rowCount%100000 == 0 {
					if err := tx.Commit(); err != nil {
						return fmt.Errorf("提交事务失败: %v", err)
					}
					tx, err = db.BeginTx(ctx, nil)
					if err != nil {
						return fmt.Errorf("开始新事务失败: %v", err)
					}
					fmt.Printf("  已导入 %s 行...\n", formatNumber(rowCount))
				}
			}
		}
	}

	if len(batch) > 0 {
		if err := si.executeBatchInsert(ctx, tx, insertSQL, batch, columns); err != nil {
			return fmt.Errorf("插入剩余数据失败: %v", err)
		}
		rowCount += int64(len(batch))
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("  共导入 %s 行数据 | 耗时: %s | 速度: %s 行/秒\n",
		formatNumber(rowCount),
		formatDuration(elapsed),
		formatFloat(float64(rowCount)/elapsed.Seconds()))

	return nil
}

// parseCSVLine 解析 CSV 行数据
func (si *SnapshotImporter) parseCSVLine(line []byte, fieldSeparator []byte, columns []string, columnTypes []*sql.ColumnType) []interface{} {
	fields := bytes.Split(line, fieldSeparator)
	if len(fields) != len(columns) {
		// 列数不匹配，跳过
		return nil
	}

	rowData := make([]interface{}, len(columns))
	for i, fieldBytes := range fields {
		rowData[i] = si.parseFieldValue(fieldBytes, columnTypes[i])
	}

	return rowData
}

// parseFieldValue 解析字段值（根据列类型）
func (si *SnapshotImporter) parseFieldValue(fieldBytes []byte, columnType *sql.ColumnType) interface{} {
	if len(fieldBytes) == 0 {
		return nil
	}

	typeName := strings.ToUpper(columnType.DatabaseTypeName())
	fieldStr := string(fieldBytes)

	// 根据类型解析
	switch {
	case strings.Contains(typeName, "INT") || strings.Contains(typeName, "BIGINT"):
		val, err := strconv.ParseInt(fieldStr, 10, 64)
		if err != nil {
			return fieldStr // 解析失败，返回字符串
		}
		return val
	case strings.Contains(typeName, "FLOAT") || strings.Contains(typeName, "DOUBLE"):
		val, err := strconv.ParseFloat(fieldStr, 64)
		if err != nil {
			return fieldStr
		}
		return val
	case strings.Contains(typeName, "DECIMAL") || strings.Contains(typeName, "NUMERIC"):
		val, err := strconv.ParseFloat(fieldStr, 64)
		if err != nil {
			return fieldStr
		}
		return val
	case strings.Contains(typeName, "BOOL"):
		return fieldStr == "true"
	case strings.Contains(typeName, "BLOB") || strings.Contains(typeName, "BYTEA") || strings.Contains(typeName, "BINARY"):
		// 十六进制格式：0x...
		if strings.HasPrefix(fieldStr, "0x") {
			hexBytes, err := hex.DecodeString(fieldStr[2:])
			if err == nil {
				return hexBytes
			}
		}
		return fieldBytes
	default:
		// 字符串类型
		return fieldStr
	}
}

// getColumnTypes 获取列类型
func (si *SnapshotImporter) getColumnTypes(ctx context.Context, db *sql.DB, dbType string, tableName string, columns []string) ([]*sql.ColumnType, error) {
	// 查询表结构
	var query string
	switch dbType {
	case "mysql", "gbase":
		query = fmt.Sprintf("SELECT * FROM `%s` LIMIT 0", tableName)
	case "kingbase", "vastbase":
		query = fmt.Sprintf(`SELECT * FROM "%s" LIMIT 0`, tableName)
	default:
		return nil, fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.ColumnTypes()
}

// buildInsertSQL 构建批量插入 SQL
func (si *SnapshotImporter) buildInsertSQL(tableName string, columns []string, dbType string) string {
	quotedTableName := si.quoteIdentifier(tableName, dbType)
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = si.quoteIdentifier(col, dbType)
	}

	// 构建占位符
	var placeholders string
	switch dbType {
	case "mysql", "gbase":
		// MySQL: (?, ?, ?, ...)
		placeholders = "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	case "kingbase", "vastbase":
		// PostgreSQL: ($1, $2, $3, ...)
		ph := make([]string, len(columns))
		for i := range ph {
			ph[i] = fmt.Sprintf("$%d", i+1)
		}
		placeholders = "(" + strings.Join(ph, ",") + ")"
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		quotedTableName,
		strings.Join(quotedColumns, ","),
		placeholders)
}

// executeBatchInsert 执行批量插入
func (si *SnapshotImporter) executeBatchInsert(
	ctx context.Context,
	tx *sql.Tx,
	insertSQL string,
	batch [][]interface{},
	columns []string,
) error {
	dbType := si.detectDBTypeFromSQL(insertSQL)

	// 对于 MySQL/GBase，可以使用 VALUES 多行插入
	if dbType == "mysql" || dbType == "gbase" {
		return si.executeMySQLBatchInsert(ctx, tx, insertSQL, batch, columns)
	}

	// 对于 PostgreSQL 系列，逐行插入（或使用 COPY）
	return si.executePostgreSQLBatchInsert(ctx, tx, insertSQL, batch, columns)
}

// executeMySQLBatchInsert MySQL/GBase 批量插入（多行 VALUES）
func (si *SnapshotImporter) executeMySQLBatchInsert(
	ctx context.Context,
	tx *sql.Tx,
	baseSQL string,
	batch [][]interface{},
	columns []string,
) error {
	if len(batch) == 0 {
		return nil
	}

	// 构建多行 VALUES
	valuesParts := make([]string, len(batch))
	args := make([]interface{}, 0, len(batch)*len(columns))

	// 单行占位符模板
	singlePlaceholder := "(" + strings.Repeat("?,", len(columns)-1) + "?)"

	for i, row := range batch {
		valuesParts[i] = singlePlaceholder
		args = append(args, row...)
	}

	// 构建完整的 INSERT SQL
	// baseSQL 格式: INSERT INTO `table` (`col1`,`col2`) VALUES (?,?)
	// 需要改为: INSERT INTO `table` (`col1`,`col2`) VALUES (?,?),(?,?),...
	valuesClause := strings.Join(valuesParts, ",")
	
	// 找到 VALUES 关键字的位置
	valuesIdx := strings.Index(baseSQL, "VALUES")
	if valuesIdx < 0 {
		return fmt.Errorf("无法找到 VALUES 关键字")
	}
	
	// 替换 VALUES 后面的部分
	sql := baseSQL[:valuesIdx+6] + " " + valuesClause

	_, err := tx.ExecContext(ctx, sql, args...)
	return err
}

// executePostgreSQLBatchInsert PostgreSQL/KingBase/VastBase 批量插入
func (si *SnapshotImporter) executePostgreSQLBatchInsert(
	ctx context.Context,
	tx *sql.Tx,
	insertSQL string,
	batch [][]interface{},
	columns []string,
) error {
	if len(batch) == 0 {
		return nil
	}

	// PostgreSQL 系列支持多行 VALUES，但占位符需要递增
	// 构建多行 VALUES，占位符从 $1 开始递增
	valuesParts := make([]string, len(batch))
	args := make([]interface{}, 0, len(batch)*len(columns))
	paramIndex := 1

	for i, row := range batch {
		ph := make([]string, len(columns))
		for j := range ph {
			ph[j] = fmt.Sprintf("$%d", paramIndex)
			paramIndex++
		}
		valuesParts[i] = "(" + strings.Join(ph, ",") + ")"
		args = append(args, row...)
	}

	// 找到 VALUES 关键字的位置
	valuesIdx := strings.Index(insertSQL, "VALUES")
	if valuesIdx < 0 {
		return fmt.Errorf("无法找到 VALUES 关键字")
	}

	// 替换 VALUES 后面的部分
	sql := insertSQL[:valuesIdx+6] + " " + strings.Join(valuesParts, ",")

	_, err := tx.ExecContext(ctx, sql, args...)
	return err
}

// detectDBTypeFromSQL 从 SQL 语句检测数据库类型
func (si *SnapshotImporter) detectDBTypeFromSQL(sql string) string {
	if strings.Contains(sql, "`") {
		return "mysql" // MySQL 使用反引号
	}
	if strings.Contains(sql, `"`) {
		return "kingbase" // PostgreSQL 系列使用双引号
	}
	return "mysql" // 默认
}
