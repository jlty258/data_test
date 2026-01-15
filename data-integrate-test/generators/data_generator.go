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
	return &DataGenerator{
		db:        db,
		schema:    schema,
		mapper:    mapper,
		batchSize: 10000, // 默认1万条一批
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
	args := make([]interface{}, 0, (endID-startID)*len(g.schema.Fields))

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

	// 整数类型
	if strings.Contains(sqlType, "TINYINT") {
		return int8(mathrand.Intn(256) - 128)
	}
	if strings.Contains(sqlType, "SMALLINT") {
		return int16(mathrand.Intn(65536) - 32768)
	}
	if strings.Contains(sqlType, "INT") && !strings.Contains(sqlType, "BIGINT") {
		return mathrand.Int31()
	}
	if strings.Contains(sqlType, "BIGINT") {
		return mathrand.Int63()
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
		size := field.MaxSize
		if size == 0 {
			size = 255
		}
		// 生成随机字符串，大小可变（1到MaxSize字节）
		actualSize := mathrand.Intn(size) + 1
		return g.generateRandomString(actualSize)
	}
	if strings.Contains(sqlType, "TEXT") || strings.Contains(sqlType, "CLOB") {
		// TEXT类型，最大1024字节
		size := mathrand.Intn(1024) + 1
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

// generateRandomString 生成随机字符串
func (g *DataGenerator) generateRandomString(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[mathrand.Intn(len(charset))]
	}
	return string(b)
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

