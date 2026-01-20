package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name      string
		configYAML string
		wantErr   bool
		validate  func(*testing.T, *TestConfig)
	}{
		{
			name: "有效的配置文件",
			configYAML: `
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
`,
			wantErr: false,
			validate: func(t *testing.T, cfg *TestConfig) {
				if cfg.DataService.Host != "localhost" {
					t.Errorf("DataService.Host = %v, want localhost", cfg.DataService.Host)
				}
				if cfg.DataService.Port != 8080 {
					t.Errorf("DataService.Port = %v, want 8080", cfg.DataService.Port)
				}
				if len(cfg.Databases) != 1 {
					t.Errorf("Databases length = %v, want 1", len(cfg.Databases))
				}
				if cfg.Databases[0].Type != "mysql" {
					t.Errorf("Database type = %v, want mysql", cfg.Databases[0].Type)
				}
				if cfg.MinIO.Endpoint != "localhost:9000" {
					t.Errorf("MinIO.Endpoint = %v, want localhost:9000", cfg.MinIO.Endpoint)
				}
			},
		},
		{
			name: "空配置文件",
			configYAML: ``,
			wantErr: false,
			validate: func(t *testing.T, cfg *TestConfig) {
				if cfg == nil {
					t.Error("Config should not be nil")
				}
			},
		},
		{
			name: "无效的 YAML",
			configYAML: `invalid: yaml: [`,
			wantErr: true,
			validate: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建临时文件
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "test_config.yaml")
			
			err := os.WriteFile(configPath, []byte(tt.configYAML), 0644)
			if err != nil {
				t.Fatalf("Failed to write test config: %v", err)
			}

			cfg, err := LoadConfig(configPath)
			
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr && cfg == nil {
				t.Error("LoadConfig() returned nil config without error")
				return
			}
			
			if tt.validate != nil && !tt.wantErr {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("nonexistent_config.yaml")
	if err == nil {
		t.Error("LoadConfig() should return error for nonexistent file")
	}
}

func TestGetDatabaseConfig(t *testing.T) {
	cfg := &TestConfig{
		Databases: []DatabaseConfig{
			{Type: "mysql", Name: "mysql1", Host: "localhost", Port: 3306},
			{Type: "kingbase", Name: "kingbase1", Host: "localhost", Port: 54321},
			{Type: "gbase", Name: "gbase1", Host: "localhost", Port: 5258},
		},
	}

	tests := []struct {
		name    string
		dbType  string
		wantErr bool
		wantType string
	}{
		{
			name:     "找到 MySQL 配置",
			dbType:   "mysql",
			wantErr:  false,
			wantType: "mysql",
		},
		{
			name:     "找到 KingBase 配置",
			dbType:   "kingbase",
			wantErr:  false,
			wantType: "kingbase",
		},
		{
			name:     "找到 GBase 配置",
			dbType:   "gbase",
			wantErr:  false,
			wantType: "gbase",
		},
		{
			name:    "未找到配置",
			dbType:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbConfig, err := cfg.GetDatabaseConfig(tt.dbType)
			
			if (err != nil) != tt.wantErr {
				t.Errorf("GetDatabaseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr {
				if dbConfig == nil {
					t.Error("GetDatabaseConfig() returned nil config without error")
					return
				}
				if dbConfig.Type != tt.wantType {
					t.Errorf("GetDatabaseConfig().Type = %v, want %v", dbConfig.Type, tt.wantType)
				}
			}
		})
	}
}

func TestGetDatabaseConfig_EmptyConfig(t *testing.T) {
	cfg := &TestConfig{
		Databases: []DatabaseConfig{},
	}
	
	_, err := cfg.GetDatabaseConfig("mysql")
	if err == nil {
		t.Error("GetDatabaseConfig() should return error for empty config")
	}
}
