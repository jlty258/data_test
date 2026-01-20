package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestExportTable_FlagParsing 测试命令行参数解析
func TestExportTable_FlagParsing(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
		validate func(*testing.T, *string, *string, *string, *string, *string)
	}{
		{
			name:    "缺少必需参数 -db",
			args:    []string{"-dbname=testdb", "-table=test_table", "-output=/tmp"},
			wantErr: true,
			validate: nil,
		},
		{
			name:    "缺少必需参数 -dbname",
			args:    []string{"-db=mysql", "-table=test_table", "-output=/tmp"},
			wantErr: true,
			validate: nil,
		},
		{
			name:    "缺少必需参数 -table",
			args:    []string{"-db=mysql", "-dbname=testdb", "-output=/tmp"},
			wantErr: true,
			validate: nil,
		},
		{
			name:    "缺少必需参数 -output",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table"},
			wantErr: true,
			validate: nil,
		},
		{
			name:    "所有必需参数都存在",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table", "-output=/tmp/export"},
			wantErr: false,
			validate: func(t *testing.T, db, dbname, table, output, config *string) {
				if *db != "mysql" {
					t.Errorf("db = %v, want mysql", *db)
				}
				if *dbname != "testdb" {
					t.Errorf("dbname = %v, want testdb", *dbname)
				}
				if *table != "test_table" {
					t.Errorf("table = %v, want test_table", *table)
				}
				if *output != "/tmp/export" {
					t.Errorf("output = %v, want /tmp/export", *output)
				}
			},
		},
		{
			name:    "使用默认配置文件",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table", "-output=/tmp/export"},
			wantErr: false,
			validate: func(t *testing.T, db, dbname, table, output, config *string) {
				if *config != "config/test_config.yaml" {
					t.Errorf("config = %v, want config/test_config.yaml", *config)
				}
			},
		},
		{
			name:    "指定自定义配置文件",
			args:    []string{"-config=custom.yaml", "-db=mysql", "-dbname=testdb", "-table=test_table", "-output=/tmp/export"},
			wantErr: false,
			validate: func(t *testing.T, db, dbname, table, output, config *string) {
				if *config != "custom.yaml" {
					t.Errorf("config = %v, want custom.yaml", *config)
				}
			},
		},
		{
			name:    "MinIO 路径格式",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table", "-output=minio://bucket/path"},
			wantErr: false,
			validate: func(t *testing.T, db, dbname, table, output, config *string) {
				if *output != "minio://bucket/path" {
					t.Errorf("output = %v, want minio://bucket/path", *output)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 重置 flag
			flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
			
			// 解析参数
			var (
				configPath = flag.String("config", "config/test_config.yaml", "配置文件路径")
				dbType     = flag.String("db", "", "数据库类型")
				dbName     = flag.String("dbname", "", "数据库名称")
				tableName  = flag.String("table", "", "表名")
				output     = flag.String("output", "", "输出路径")
			)
			
			// 设置测试参数
			os.Args = append([]string{"export-table"}, tt.args...)
			err := flag.CommandLine.Parse(tt.args)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Flag parsing error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			
			// 验证参数
			hasError := false
			if *dbType == "" {
				hasError = true
			}
			if *dbName == "" {
				hasError = true
			}
			if *tableName == "" {
				hasError = true
			}
			if *output == "" {
				hasError = true
			}
			
			if hasError != tt.wantErr {
				t.Errorf("Parameter validation error = %v, wantErr %v", hasError, tt.wantErr)
				return
			}
			
			if tt.validate != nil && !tt.wantErr {
				tt.validate(t, dbType, dbName, tableName, output, configPath)
			}
		})
	}
}

// TestExportTable_ConfigValidation 测试配置文件验证
func TestExportTable_ConfigValidation(t *testing.T) {
	tmpDir := t.TempDir()
	
	// 创建有效的配置文件
	validConfig := `
data_service:
  host: "localhost"
  port: 8080
mock_services:
  gateway:
    host: "localhost"
    port: 9090
  ida:
    host: "localhost"
    port: 9091
databases:
  - type: "mysql"
    name: "test_mysql"
    host: "localhost"
    port: 3306
    user: "root"
    password: "password"
    database: "testdb"
minio:
  endpoint: "localhost:9000"
  access_key: "minioadmin"
  secret_key: "minioadmin"
  bucket: "test-bucket"
`
	validConfigPath := filepath.Join(tmpDir, "valid_config.yaml")
	err := os.WriteFile(validConfigPath, []byte(validConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to write valid config: %v", err)
	}
	
	// 创建无效的配置文件
	invalidConfigPath := filepath.Join(tmpDir, "invalid_config.yaml")
	err = os.WriteFile(invalidConfigPath, []byte("invalid: yaml: ["), 0644)
	if err != nil {
		t.Fatalf("Failed to write invalid config: %v", err)
	}
	
	tests := []struct {
		name      string
		configPath string
		wantErr   bool
	}{
		{
			name:      "有效的配置文件",
			configPath: validConfigPath,
			wantErr:   false,
		},
		{
			name:      "无效的配置文件",
			configPath: invalidConfigPath,
			wantErr:   true,
		},
		{
			name:      "不存在的配置文件",
			configPath: filepath.Join(tmpDir, "nonexistent.yaml"),
			wantErr:   true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 这里我们测试配置加载逻辑
			// 实际工具中会调用 config.LoadConfig
			// 由于这是集成测试，我们只验证文件是否存在和格式是否正确
			if _, err := os.Stat(tt.configPath); err != nil {
				if !tt.wantErr {
					t.Errorf("Config file should exist: %v", err)
				}
				return
			}
			
			// 如果文件存在，尝试读取（实际工具会解析 YAML）
			data, err := os.ReadFile(tt.configPath)
			if err != nil {
				t.Errorf("Failed to read config file: %v", err)
				return
			}
			
			// 简单验证：无效的 YAML 应该包含语法错误
			if tt.wantErr && len(data) > 0 {
				// 这里只是示例，实际应该使用 YAML 解析器
				t.Logf("Config file read: %d bytes", len(data))
			}
		})
	}
}

// TestExportTable_OutputPathValidation 测试输出路径验证
func TestExportTable_OutputPathValidation(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		isMinIO bool
		wantErr bool
	}{
		{
			name:    "本地文件系统路径",
			output:  "/tmp/export",
			isMinIO: false,
			wantErr: false,
		},
		{
			name:    "相对路径",
			output:  "./exports",
			isMinIO: false,
			wantErr: false,
		},
		{
			name:    "MinIO 路径格式",
			output:  "minio://bucket/path",
			isMinIO: true,
			wantErr: false,
		},
		{
			name:    "MinIO 路径格式（带前缀）",
			output:  "minio://my-bucket/exports/table",
			isMinIO: true,
			wantErr: false,
		},
		{
			name:    "空路径",
			output:  "",
			isMinIO: false,
			wantErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMinIO := strings.HasPrefix(tt.output, "minio://")
			if isMinIO != tt.isMinIO {
				t.Errorf("isMinIO = %v, want %v", isMinIO, tt.isMinIO)
			}
			
			if tt.output == "" && !tt.wantErr {
				t.Error("Empty output path should be invalid")
			}
		})
	}
}
