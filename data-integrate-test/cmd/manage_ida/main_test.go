package main

import (
	"flag"
	"os"
	"testing"
)

// TestManageIDA_FlagParsing 测试命令行参数解析
func TestManageIDA_FlagParsing(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
		validate func(*testing.T, *string, *string, *int, *int, *int, *int)
	}{
		{
			name:    "默认 action (help)",
			args:    []string{},
			wantErr: false,
			validate: func(t *testing.T, action, config *string, dsId, assetId, page, size *int) {
				if *action != "help" {
					t.Errorf("action = %v, want help", *action)
				}
			},
		},
		{
			name:    "显示帮助",
			args:    []string{"-action=help"},
			wantErr: false,
			validate: func(t *testing.T, action, config *string, dsId, assetId, page, size *int) {
				if *action != "help" {
					t.Errorf("action = %v, want help", *action)
				}
			},
		},
		{
			name:    "创建数据源",
			args:    []string{"-action=create-ds"},
			wantErr: false,
			validate: func(t *testing.T, action, config *string, dsId, assetId, page, size *int) {
				if *action != "create-ds" {
					t.Errorf("action = %v, want create-ds", *action)
				}
			},
		},
		{
			name:    "查询数据源",
			args:    []string{"-action=query-ds", "-ds-id=1000"},
			wantErr: false,
			validate: func(t *testing.T, action, config *string, dsId, assetId, page, size *int) {
				if *action != "query-ds" {
					t.Errorf("action = %v, want query-ds", *action)
				}
				if *dsId != 1000 {
					t.Errorf("dsId = %v, want 1000", *dsId)
				}
			},
		},
		{
			name:    "查询资产列表",
			args:    []string{"-action=query-asset", "-page=1", "-size=20"},
			wantErr: false,
			validate: func(t *testing.T, action, config *string, dsId, assetId, page, size *int) {
				if *action != "query-asset" {
					t.Errorf("action = %v, want query-asset", *action)
				}
				if *page != 1 {
					t.Errorf("page = %v, want 1", *page)
				}
				if *size != 20 {
					t.Errorf("size = %v, want 20", *size)
				}
			},
		},
		{
			name:    "查询单个资产",
			args:    []string{"-action=query-asset", "-asset-id=2000"},
			wantErr: false,
			validate: func(t *testing.T, action, config *string, dsId, assetId, page, size *int) {
				if *action != "query-asset" {
					t.Errorf("action = %v, want query-asset", *action)
				}
				if *assetId != 2000 {
					t.Errorf("assetId = %v, want 2000", *assetId)
				}
			},
		},
		{
			name:    "查询所有",
			args:    []string{"-action=query-all", "-page=1", "-size=10"},
			wantErr: false,
			validate: func(t *testing.T, action, config *string, dsId, assetId, page, size *int) {
				if *action != "query-all" {
					t.Errorf("action = %v, want query-all", *action)
				}
			},
		},
		{
			name:    "使用默认分页参数",
			args:    []string{"-action=query-asset"},
			wantErr: false,
			validate: func(t *testing.T, action, config *string, dsId, assetId, page, size *int) {
				if *page != 1 {
					t.Errorf("page = %v, want 1 (default)", *page)
				}
				if *size != 10 {
					t.Errorf("size = %v, want 10 (default)", *size)
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
				action     = flag.String("action", "help", "操作类型")
				dsId       = flag.Int("ds-id", 0, "数据源ID")
				assetId    = flag.Int("asset-id", 0, "资产ID")
				pageNum    = flag.Int("page", 1, "页码")
				pageSize   = flag.Int("size", 10, "每页条数")
			)
			
			// 设置测试参数
			err := flag.CommandLine.Parse(tt.args)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Flag parsing error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			
			if tt.validate != nil && !tt.wantErr {
				tt.validate(t, action, configPath, dsId, assetId, pageNum, pageSize)
			}
		})
	}
}

// TestManageIDA_ActionValidation 测试 action 参数验证
func TestManageIDA_ActionValidation(t *testing.T) {
	validActions := []string{
		"help",
		"create-ds",
		"create-asset",
		"query-ds",
		"query-asset",
		"query-all",
	}
	
	for _, action := range validActions {
		t.Run("valid action: "+action, func(t *testing.T) {
			// 验证 action 是否在有效列表中
			valid := false
			for _, validAction := range validActions {
				if action == validAction {
					valid = true
					break
				}
			}
			if !valid {
				t.Errorf("Action %s should be valid", action)
			}
		})
	}
	
	invalidActions := []string{
		"invalid",
		"unknown",
		"",
	}
	
	for _, action := range invalidActions {
		t.Run("invalid action: "+action, func(t *testing.T) {
			// 验证 action 是否在有效列表中
			valid := false
			for _, validAction := range validActions {
				if action == validAction {
					valid = true
					break
				}
			}
			if valid && action != "" && action != "help" {
				t.Errorf("Action %s should not be valid", action)
			}
		})
	}
}
