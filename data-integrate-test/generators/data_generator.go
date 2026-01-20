package generators

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	mathrand "math/rand"
	"strings"
	"sync"
	"time"
)

// DataGenerator 数据生成器
type DataGenerator struct {
	db        *sql.DB
	schema    *SchemaDefinition
	mapper    *DatabaseTypeMapper
	batchSize int64 // 批次大小
}

func NewDataGenerator(db *sql.DB, schema *SchemaDefinition, mapper *DatabaseTypeMapper) *DataGenerator {
	// MySQL 的 prepared statement 最多支持 65535 个占位符
	// 假设每个字段一个占位符，批次大小 = 65535 / 字段数
	// 为了安全，设置为 1000 条一批（假设最多 16 个字段）
	batchSize := int64(1000)
	if len(schema.Fields) > 0 {
		maxBatch := 65535 / int64(len(schema.Fields))
		if maxBatch < batchSize {
			batchSize = maxBatch
		}
	}
	
	return &DataGenerator{
		db:        db,
		schema:    schema,
		mapper:    mapper,
		batchSize: batchSize,
	}
}

// GenerateAndInsert 生成并插入数据（支持大数据量）
func (g *DataGenerator) GenerateAndInsert(ctx context.Context) error {
	// 创建表
	if err := g.createTable(ctx); err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 计算批次数量
	totalBatches := (g.schema.RowCount + g.batchSize - 1) / g.batchSize
	
	// 使用goroutine池并发插入
	workers := 10 // 并发worker数
	jobs := make(chan int64, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var lastErr error

	// 启动worker
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batchIdx := range jobs {
				if err := g.insertBatch(ctx, batchIdx); err != nil {
					mu.Lock()
					lastErr = err
					mu.Unlock()
					return
				}
			}
		}()
	}

	// 发送任务
	go func() {
		defer close(jobs)
		for i := int64(0); i < totalBatches; i++ {
			select {
			case jobs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	return lastErr
}

// insertBatch 插入一个批次的数据
func (g *DataGenerator) insertBatch(ctx context.Context, batchIdx int64) error {
	startID := batchIdx * g.batchSize
	endID := startID + g.batchSize
	if endID > g.schema.RowCount {
		endID = g.schema.RowCount
	}

	// 生成批次数据
	values := make([]string, 0, endID-startID)
	args := make([]interface{}, 0, int(endID-startID)*len(g.schema.Fields))

	for id := startID; id < endID; id++ {
		rowValues := g.generateRow(id)
		placeholders := g.getPlaceholders()
		values = append(values, placeholders)
		args = append(args, rowValues...)
	}

	// 构建INSERT语句
	columns := make([]string, len(g.schema.Fields))
	for i, field := range g.schema.Fields {
		columns[i] = g.quoteIdentifier(field.Name)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		g.quoteIdentifier(g.schema.TableName),
		strings.Join(columns, ","),
		strings.Join(values, ","),
	)

	_, err := g.db.ExecContext(ctx, query, args...)
	return err
}

// generateRow 生成一行数据
func (g *DataGenerator) generateRow(id int64) []interface{} {
	row := make([]interface{}, len(g.schema.Fields))
	row[0] = id // ID字段

	mathrand.Seed(time.Now().UnixNano() + id)

	for i := 1; i < len(g.schema.Fields); i++ {
		field := g.schema.Fields[i]
		row[i] = g.generateValue(field, id)
	}

	return row
}

// generateValue 根据字段类型生成值
func (g *DataGenerator) generateValue(field FieldType, rowID int64) interface{} {
	// 处理可空字段
	if field.Nullable && mathrand.Float32() < 0.1 { // 10%概率为NULL
		return nil
	}

	sqlType := strings.ToUpper(field.SQLType)

	// 整数类型（按顺序检查，从最具体到最通用）
	if strings.Contains(sqlType, "TINYINT") {
		// TINYINT: -128 to 127 (signed) or 0 to 255 (unsigned)
		// 为了安全，生成 0-127 的值，兼容有符号和无符号
		if strings.Contains(strings.ToUpper(sqlType), "UNSIGNED") {
			return uint8(mathrand.Intn(256))
		}
		return int8(mathrand.Intn(128)) // 0-127，避免负数
	}
	if strings.Contains(sqlType, "SMALLINT") {
		// SMALLINT: -32768 to 32767 (signed) or 0 to 65535 (unsigned)
		if strings.Contains(strings.ToUpper(sqlType), "UNSIGNED") {
			return uint16(mathrand.Intn(65536))
		}
		return int16(mathrand.Intn(32768)) // 0-32767，避免负数
	}
	if strings.Contains(sqlType, "MEDIUMINT") {
		// MEDIUMINT: -8388608 to 8388607 (signed) or 0 to 16777215 (unsigned)
		if strings.Contains(strings.ToUpper(sqlType), "UNSIGNED") {
			return int32(mathrand.Intn(16777216))
		}
		return int32(mathrand.Intn(8388608)) // 0-8388607，避免负数
	}
	if strings.Contains(sqlType, "BIGINT") {
		// BIGINT: 使用正数范围避免溢出
		return mathrand.Int63n(9223372036854775807) + 1 // 1 to 9223372036854775807
	}
	if strings.Contains(sqlType, "INT") {
		// INT: -2147483648 to 2147483647 (signed) or 0 to 4294967295 (unsigned)
		if strings.Contains(strings.ToUpper(sqlType), "UNSIGNED") {
			return int64(mathrand.Intn(4294967296))
		}
		return int32(mathrand.Intn(2147483648)) // 0-2147483647，避免负数
	}

	// 浮点数类型
	if strings.Contains(sqlType, "FLOAT") {
		return mathrand.Float32() * 1000
	}
	if strings.Contains(sqlType, "DOUBLE") || strings.Contains(sqlType, "REAL") {
		return mathrand.Float64() * 1000
	}
	if strings.Contains(sqlType, "DECIMAL") || strings.Contains(sqlType, "NUMERIC") {
		return fmt.Sprintf("%.2f", mathrand.Float64()*1000)
	}

	// 字符串类型
	if strings.Contains(sqlType, "VARCHAR") || strings.Contains(sqlType, "CHAR") {
		// 从SQL类型中提取实际长度限制
		size := g.extractSizeFromSQLType(sqlType)
		if size == 0 {
			size = field.MaxSize
		}
		if size == 0 {
			size = 255 // 默认值
		}
		// 确保size不超过1024（模板中max_field_size的限制）
		if size > 1024 {
			size = 1024
		}
		// 如果模板指定了maxFieldSize，也要考虑这个限制
		// 但这里 field.MaxSize 已经包含了这个信息
		
		// 生成随机字符串，大小可变（1到size字节），但不超过字段定义的长度
		// 对于 CHAR 类型，MySQL 会填充空格，但输入数据不能超过定义长度
		// 对于 VARCHAR 类型，输入数据不能超过定义长度
		// 为了安全，生成的数据长度应该严格小于等于字段定义的长度
		actualSize := mathrand.Intn(size) + 1
		// 确保不超过限制（使用 <= 而不是 <）
		if actualSize > size {
			actualSize = size
		}
		// 再次确保不超过限制（防止边界情况）
		if actualSize < 1 {
			actualSize = 1
		}
		// 生成字符串并确保字节长度不超过 actualSize
		result := g.generateRandomString(actualSize)
		// 如果由于某种原因长度超限，截断到指定字节数
		if len(result) > size {
			result = result[:size]
		}
		return result
	}
	if strings.Contains(sqlType, "TEXT") || strings.Contains(sqlType, "CLOB") {
		// TEXT类型，最大1024字节（根据模板限制）
		// 但需要确保不超过字段的实际限制
		maxSize := 1024
		if field.MaxSize > 0 && field.MaxSize < maxSize {
			maxSize = field.MaxSize
		}
		size := mathrand.Intn(maxSize) + 1
		// 确保不超过限制
		if size > maxSize {
			size = maxSize
		}
		return g.generateRandomString(size)
	}

	// 二进制类型
	if strings.Contains(sqlType, "BLOB") || strings.Contains(sqlType, "BYTEA") {
		size := mathrand.Intn(1024) + 1
		return g.generateRandomBytes(size)
	}

	// 日期时间类型
	if strings.Contains(sqlType, "DATE") {
		return time.Now().AddDate(0, 0, -mathrand.Intn(365)).Format("2006-01-02")
	}
	if strings.Contains(sqlType, "TIMESTAMP") || strings.Contains(sqlType, "DATETIME") {
		return time.Now().Add(-time.Duration(mathrand.Intn(86400*365)) * time.Second).Format("2006-01-02 15:04:05")
	}

	return fmt.Sprintf("value_%d", rowID)
}

// generateRandomString 生成随机字符串（确保字节长度不超过指定大小）
func (g *DataGenerator) generateRandomString(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// 确保 size 是有效的
	if size <= 0 {
		size = 1
	}
	if size > 1024 {
		size = 1024 // 最大限制
	}
	
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[mathrand.Intn(len(charset))]
	}
	
	// 确保返回的字符串字节长度不超过 size
	result := string(b)
	if len(result) > size {
		// 如果由于UTF-8编码导致长度超限，截断到指定字节数
		result = result[:size]
	}
	return result
}

// generateRandomBytes 生成随机字节
func (g *DataGenerator) generateRandomBytes(size int) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

// createTable 创建表
func (g *DataGenerator) createTable(ctx context.Context) error {
	columns := make([]string, len(g.schema.Fields))
	for i, field := range g.schema.Fields {
		nullable := ""
		if !field.Nullable {
			nullable = "NOT NULL"
		}
		if i == 0 {
			nullable += " PRIMARY KEY"
		}
		columns[i] = fmt.Sprintf("%s %s %s",
			g.quoteIdentifier(field.Name),
			field.SQLType,
			nullable,
		)
	}

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		g.quoteIdentifier(g.schema.TableName),
		strings.Join(columns, ","),
	)

	_, err := g.db.ExecContext(ctx, query)
	return err
}

// getPlaceholders 获取占位符（根据数据库类型）
func (g *DataGenerator) getPlaceholders() string {
	placeholders := make([]string, len(g.schema.Fields))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return "(" + strings.Join(placeholders, ",") + ")"
}

// extractSizeFromSQLType 从SQL类型中提取长度限制
func (g *DataGenerator) extractSizeFromSQLType(sqlType string) int {
	// 提取 VARCHAR(100) 或 CHAR(100) 中的数字
	var size int
	sqlTypeUpper := strings.ToUpper(sqlType)
	if strings.Contains(sqlTypeUpper, "VARCHAR") {
		// 匹配 VARCHAR(数字) 或 VARCHAR(数字) UNSIGNED 等
		if n, err := fmt.Sscanf(sqlType, "VARCHAR(%d", &size); n == 1 && err == nil {
			return size
		}
		// 尝试匹配大小写不敏感的情况
		if n, err := fmt.Sscanf(sqlTypeUpper, "VARCHAR(%d", &size); n == 1 && err == nil {
			return size
		}
	} else if strings.Contains(sqlTypeUpper, "CHAR") && !strings.Contains(sqlTypeUpper, "VARCHAR") {
		// CHAR类型（排除VARCHAR）
		if n, err := fmt.Sscanf(sqlType, "CHAR(%d", &size); n == 1 && err == nil {
			return size
		}
		// 尝试匹配大小写不敏感的情况
		if n, err := fmt.Sscanf(sqlTypeUpper, "CHAR(%d", &size); n == 1 && err == nil {
			return size
		}
	}
	return size
}

// quoteIdentifier 引用标识符（根据数据库类型）
func (g *DataGenerator) quoteIdentifier(name string) string {
	switch g.mapper.dbType {
	case "mysql", "gbase":
		return "`" + name + "`"
	case "kingbase", "vastbase":
		return `"` + name + `"`
	default:
		return name
	}
}

