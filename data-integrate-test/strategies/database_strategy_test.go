package strategies

import (
	"data-integrate-test/config"
	"testing"
)

func TestNewDatabaseStrategyFactory(t *testing.T) {
	factory := NewDatabaseStrategyFactory()
	if factory == nil {
		t.Error("NewDatabaseStrategyFactory() returned nil")
	}
}

func TestCreateStrategy(t *testing.T) {
	factory := NewDatabaseStrategyFactory()

	tests := []struct {
		name     string
		dbConfig *config.DatabaseConfig
		wantErr  bool
		wantType string
	}{
		{
			name: "创建 MySQL 策略",
			dbConfig: &config.DatabaseConfig{
				Type:     "mysql",
				Name:     "test_mysql",
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Database: "testdb",
			},
			wantErr:  false,
			wantType: "mysql",
		},
		{
			name: "创建 KingBase 策略",
			dbConfig: &config.DatabaseConfig{
				Type:     "kingbase",
				Name:     "test_kingbase",
				Host:     "localhost",
				Port:     54321,
				User:     "kingbase",
				Password: "password",
				Database: "testdb",
			},
			wantErr:  false,
			wantType: "kingbase",
		},
		{
			name: "创建 GBase 策略",
			dbConfig: &config.DatabaseConfig{
				Type:     "gbase",
				Name:     "test_gbase",
				Host:     "localhost",
				Port:     5258,
				User:     "gbase",
				Password: "password",
				Database: "testdb",
			},
			wantErr:  false,
			wantType: "gbase",
		},
		{
			name: "创建 VastBase 策略",
			dbConfig: &config.DatabaseConfig{
				Type:     "vastbase",
				Name:     "test_vastbase",
				Host:     "localhost",
				Port:     5432,
				User:     "vastbase",
				Password: "password",
				Database: "testdb",
			},
			wantErr:  false,
			wantType: "vastbase",
		},
		{
			name: "不支持的数据库类型",
			dbConfig: &config.DatabaseConfig{
				Type: "unsupported",
			},
			wantErr: true,
		},
		{
			name:     "nil 配置",
			dbConfig: nil,
			wantErr:  true,
			wantType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := factory.CreateStrategy(tt.dbConfig)
			
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateStrategy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr {
				if strategy == nil {
					t.Error("CreateStrategy() returned nil strategy without error")
					return
				}
				
				// 验证策略类型
				if strategy.GetDBType() != tt.wantType {
					t.Errorf("GetDBType() = %v, want %v", strategy.GetDBType(), tt.wantType)
				}
				
				// 验证连接信息
				connInfo := strategy.GetConnectionInfo()
				if connInfo == nil {
					t.Error("GetConnectionInfo() returned nil")
				} else if connInfo.Type != tt.wantType {
					t.Errorf("GetConnectionInfo().Type = %v, want %v", connInfo.Type, tt.wantType)
				}
			}
		})
	}
}

func TestStrategy_GetConnectionInfo(t *testing.T) {
	factory := NewDatabaseStrategyFactory()
	
	dbConfig := &config.DatabaseConfig{
		Type:     "mysql",
		Name:     "test_mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		Database: "testdb",
	}
	
	strategy, err := factory.CreateStrategy(dbConfig)
	if err != nil {
		t.Fatalf("CreateStrategy() error = %v", err)
	}
	
	connInfo := strategy.GetConnectionInfo()
	if connInfo == nil {
		t.Fatal("GetConnectionInfo() returned nil")
	}
	
	if connInfo.Type != "mysql" {
		t.Errorf("GetConnectionInfo().Type = %v, want mysql", connInfo.Type)
	}
	if connInfo.Host != "localhost" {
		t.Errorf("GetConnectionInfo().Host = %v, want localhost", connInfo.Host)
	}
	if connInfo.Port != 3306 {
		t.Errorf("GetConnectionInfo().Port = %v, want 3306", connInfo.Port)
	}
}
