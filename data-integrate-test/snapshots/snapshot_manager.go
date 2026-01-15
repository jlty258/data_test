package snapshots

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"data-integrate-test/generators"
	"encoding/json"
	"fmt"
	"os"
	"time"
	_ "github.com/mattn/go-sqlite3"
)

// SnapshotManager 快照管理器（借鉴SQLMesh核心思想）
type SnapshotManager struct {
	db        *sql.DB
	snapshotDir string
}

// Snapshot 数据快照元数据
type Snapshot struct {
	ID          int64
	Name        string
	ModelName   string
	SchemaHash  string    // Schema的哈希值
	RowCount    int64
	CreatedAt   time.Time
	DataPath    string    // 数据存储路径（可选）
}

func NewSnapshotManager(snapshotDir string) (*SnapshotManager, error) {
	// 确保目录存在
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, err
	}
	
	// 初始化SQLite数据库
	dbPath := fmt.Sprintf("%s/snapshots.db", snapshotDir)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	
	sm := &SnapshotManager{
		db:          db,
		snapshotDir: snapshotDir,
	}
	
	// 创建表
	if err := sm.initDB(); err != nil {
		return nil, err
	}
	
	return sm, nil
}

func (sm *SnapshotManager) initDB() error {
	query := `
	CREATE TABLE IF NOT EXISTS snapshots (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		model_name TEXT NOT NULL,
		schema_hash TEXT NOT NULL,
		row_count INTEGER NOT NULL,
		created_at DATETIME NOT NULL,
		data_path TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_schema_hash_row_count ON snapshots(schema_hash, row_count);
	`
	_, err := sm.db.Exec(query)
	return err
}

func (sm *SnapshotManager) Close() error {
	return sm.db.Close()
}

// CreateSnapshot 创建快照（如果数据已存在，直接使用）
func (sm *SnapshotManager) CreateSnapshot(
	ctx context.Context,
	modelName string,
	schema *generators.SchemaDefinition,
	rowCount int64,
) (*Snapshot, error) {
	// 计算schema哈希
	schemaHash := sm.hashSchema(schema)
	
	// 检查是否已存在相同快照
	existing, err := sm.findSnapshot(schemaHash, rowCount)
	if err == nil && existing != nil {
		fmt.Printf("发现已存在的快照: %s，可以复用数据\n", existing.Name)
		return existing, nil
	}
	
	// 创建新快照
	snapshot := &Snapshot{
		Name:       fmt.Sprintf("snap_%s_%d_%d", modelName, rowCount, time.Now().Unix()),
		ModelName:  modelName,
		SchemaHash: schemaHash,
		RowCount:   rowCount,
		CreatedAt:  time.Now(),
	}
	
	// 保存到数据库
	result, err := sm.db.ExecContext(ctx,
		`INSERT INTO snapshots (name, model_name, schema_hash, row_count, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		snapshot.Name, snapshot.ModelName, snapshot.SchemaHash,
		snapshot.RowCount, snapshot.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	
	id, _ := result.LastInsertId()
	snapshot.ID = id
	
	return snapshot, nil
}

// findSnapshot 查找已存在的快照
func (sm *SnapshotManager) findSnapshot(schemaHash string, rowCount int64) (*Snapshot, error) {
	var s Snapshot
	err := sm.db.QueryRow(
		`SELECT id, name, model_name, schema_hash, row_count, created_at, data_path
		 FROM snapshots
		 WHERE schema_hash = ? AND row_count = ?
		 ORDER BY created_at DESC
		 LIMIT 1`,
		schemaHash, rowCount,
	).Scan(&s.ID, &s.Name, &s.ModelName, &s.SchemaHash, &s.RowCount, &s.CreatedAt, &s.DataPath)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	
	return &s, nil
}

// hashSchema 计算schema哈希
func (sm *SnapshotManager) hashSchema(schema *generators.SchemaDefinition) string {
	// 序列化schema并计算哈希
	data, _ := json.Marshal(schema)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// ListSnapshots 列出所有快照
func (sm *SnapshotManager) ListSnapshots() ([]*Snapshot, error) {
	rows, err := sm.db.Query(
		`SELECT id, name, model_name, schema_hash, row_count, created_at, data_path
		 FROM snapshots
		 ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var snapshots []*Snapshot
	for rows.Next() {
		var s Snapshot
		if err := rows.Scan(&s.ID, &s.Name, &s.ModelName, &s.SchemaHash, &s.RowCount, &s.CreatedAt, &s.DataPath); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, &s)
	}
	
	return snapshots, nil
}

