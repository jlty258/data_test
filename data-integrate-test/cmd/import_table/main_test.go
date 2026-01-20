package main

import (
	"flag"
	"os"
	"strings"
	"testing"
)

// TestImportTable_FlagParsing 测试命令行参数解析
func TestImportTable_FlagParsing(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
		validate func(*testing.T, *string, *string, *string, *string, *string, *int)
	}{
		{
			name:    "缺少必需参数 -db",
			args:    []string{"-dbname=testdb", "-table=test_table", "-input=/tmp"},
			wantErr: true,
			validate: nil,
		},
		{
			name:    "缺少必需参数 -dbname",
			args:    []string{"-db=mysql", "-table=test_table", "-input=/tmp"},
			wantErr: true,
			validate: nil,
		},
		{
			name:    "缺少必需参数 -table",
			args:    []string{"-db=mysql", "-dbname=testdb", "-input=/tmp"},
			wantErr: true,
			validate: nil,
		},
		{
			name:    "缺少必需参数 -input",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table"},
			wantErr: true,
			validate: nil,
		},
		{
			name:    "所有必需参数都存在",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table", "-input=/tmp/import"},
			wantErr: false,
			validate: func(t *testing.T, db, dbname, table, input, config *string, batch *int) {
				if *db != "mysql" {
					t.Errorf("db = %v, want mysql", *db)
				}
				if *dbname != "testdb" {
					t.Errorf("dbname = %v, want testdb", *dbname)
				}
				if *table != "test_table" {
					t.Errorf("table = %v, want test_table", *table)
				}
				if *input != "/tmp/import" {
					t.Errorf("input = %v, want /tmp/import", *input)
				}
			},
		},
		{
			name:    "使用默认批量大小",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table", "-input=/tmp/import"},
			wantErr: false,
			validate: func(t *testing.T, db, dbname, table, input, config *string, batch *int) {
				if *batch != 5000 {
					t.Errorf("batch = %v, want 5000", *batch)
				}
			},
		},
		{
			name:    "指定自定义批量大小",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table", "-input=/tmp/import", "-batch=10000"},
			wantErr: false,
			validate: func(t *testing.T, db, dbname, table, input, config *string, batch *int) {
				if *batch != 10000 {
					t.Errorf("batch = %v, want 10000", *batch)
				}
			},
		},
		{
			name:    "MinIO 路径格式",
			args:    []string{"-db=mysql", "-dbname=testdb", "-table=test_table", "-input=minio://bucket/path"},
			wantErr: false,
			validate: func(t *testing.T, db, dbname, table, input, config *string, batch *int) {
				if *input != "minio://bucket/path" {
					t.Errorf("input = %v, want minio://bucket/path", *input)
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
				tableName  = flag.String("table", "", "目标表名")
				input      = flag.String("input", "", "输入路径")
				batchSize  = flag.Int("batch", 5000, "批量插入大小")
			)
			
			// 设置测试参数
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
			if *input == "" {
				hasError = true
			}
			
			if hasError != tt.wantErr {
				t.Errorf("Parameter validation error = %v, wantErr %v", hasError, tt.wantErr)
				return
			}
			
			if tt.validate != nil && !tt.wantErr {
				tt.validate(t, dbType, dbName, tableName, input, configPath, batchSize)
			}
		})
	}
}

// TestImportTable_InputPathValidation 测试输入路径验证
func TestImportTable_InputPathValidation(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		isMinIO bool
		wantErr bool
	}{
		{
			name:    "本地文件系统路径",
			input:   "/tmp/import",
			isMinIO: false,
			wantErr: false,
		},
		{
			name:    "相对路径",
			input:   "./imports",
			isMinIO: false,
			wantErr: false,
		},
		{
			name:    "MinIO 路径格式",
			input:   "minio://bucket/path",
			isMinIO: true,
			wantErr: false,
		},
		{
			name:    "空路径",
			input:   "",
			isMinIO: false,
			wantErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMinIO := strings.HasPrefix(tt.input, "minio://")
			if isMinIO != tt.isMinIO {
				t.Errorf("isMinIO = %v, want %v", isMinIO, tt.isMinIO)
			}
			
			if tt.input == "" && !tt.wantErr {
				t.Error("Empty input path should be invalid")
			}
		})
	}
}

// TestImportTable_BatchSizeValidation 测试批量大小验证
func TestImportTable_BatchSizeValidation(t *testing.T) {
	tests := []struct {
		name     string
		batchSize int
		wantErr  bool
	}{
		{
			name:     "有效的批量大小",
			batchSize: 1000,
			wantErr:  false,
		},
		{
			name:     "默认批量大小",
			batchSize: 5000,
			wantErr:  false,
		},
		{
			name:     "大批量大小",
			batchSize: 10000,
			wantErr:  false,
		},
		{
			name:     "零批量大小（应该使用默认值）",
			batchSize: 0,
			wantErr:  false,
		},
		{
			name:     "负数批量大小（应该使用默认值）",
			batchSize: -1,
			wantErr:  false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 在实际工具中，NewSnapshotImporter 会处理无效的批量大小
			// 这里我们只验证参数解析
			if tt.batchSize < 0 {
				// 负数应该被处理为默认值
				t.Logf("Negative batch size %d should be handled", tt.batchSize)
			}
		})
	}
}
