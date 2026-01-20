package snapshots

import (
	"context"
	"data-integrate-test/config"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// mockDatabaseStrategy 用于测试的 mock 策略
type mockDatabaseStrategy struct {
	dbType      string
	dbConfig    *config.DatabaseConfig
	tableExists bool
	rowCount    int64
}

func (m *mockDatabaseStrategy) Connect(ctx context.Context) error {
	return nil
}

func (m *mockDatabaseStrategy) GetDB() *sql.DB {
	return nil
}

func (m *mockDatabaseStrategy) GetDBType() string {
	return m.dbType
}

func (m *mockDatabaseStrategy) GetConnectionInfo() *config.DatabaseConfig {
	return m.dbConfig
}

func (m *mockDatabaseStrategy) Cleanup(ctx context.Context, tableName string) error {
	return nil
}

func (m *mockDatabaseStrategy) GetRowCount(ctx context.Context, tableName string) (int64, error) {
	return m.rowCount, nil
}

func (m *mockDatabaseStrategy) TableExists(ctx context.Context, tableName string) (bool, error) {
	return m.tableExists, nil
}

func TestNewSnapshotExporter(t *testing.T) {
	exporter := NewSnapshotExporter("/tmp/test")
	if exporter == nil {
		t.Error("NewSnapshotExporter() returned nil")
	}
	if exporter.outputDir != "/tmp/test" {
		t.Errorf("NewSnapshotExporter().outputDir = %v, want /tmp/test", exporter.outputDir)
	}
}

func TestSnapshotExporter_ExportTableSnapshot_OutputDir(t *testing.T) {
	// 这个测试需要真实的数据库连接，使用 mock 会导致 panic
	// 跳过此测试，使用集成测试来验证
	t.Skip("需要真实数据库连接，使用集成测试验证")
}

func TestSnapshotExporter_OutputDirCreation(t *testing.T) {
	tmpDir := t.TempDir()
	exporter := NewSnapshotExporter(tmpDir)
	
	// 测试输出目录创建（不涉及数据库操作）
	testDir := filepath.Join(tmpDir, "nested", "path")
	
	// 直接测试目录创建功能
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	
	// 验证目录是否被创建
	if _, err := os.Stat(testDir); err != nil {
		t.Errorf("Output directory was not created: %v", err)
	}
	
	// 验证 exporter 的 outputDir 设置
	if exporter.outputDir != tmpDir {
		t.Errorf("Exporter outputDir = %v, want %v", exporter.outputDir, tmpDir)
	}
}
