package service

import (
	"fmt"
	"strings"
	"testing"
)

func TestCheckStreamloaderOutput(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		wantErr bool
		errMsg  string
	}{
		{
			name: "Success status with loaded rows",
			output: `{
				"Status": "Success",
				"Message": "Load completed successfully",
				"TotalRows": 1000,
				"LoadedRows": 1000,
				"FailLoadRows": 0
			}`,
			wantErr: false,
		},
		{
			name: "OK status (case insensitive)",
			output: `{
				"Status": "ok",
				"Message": "All good",
				"TotalRows": 500,
				"LoadedRows": 500,
				"FailLoadRows": 0
			}`,
			wantErr: false,
		},
		{
			name: "Business failure - FailLoadRows > 0",
			output: `{
				"Status": "Success",
				"Message": "OK",
				"TotalRows": 10000,
				"LoadedRows": 0,
				"FailLoadRows": 10000
			}`,
			wantErr: true,
			errMsg:  "doris-streamloader business failure: status=Success, failLoadRows=10000, loadedRows=0, totalRows=10000, message=OK",
		},
		{
			name: "Business failure - TotalRows > 0 but LoadedRows = 0",
			output: `{
				"Status": "Success",
				"Message": "OK",
				"TotalRows": 5000,
				"LoadedRows": 0,
				"FailLoadRows": 0
			}`,
			wantErr: true,
			errMsg:  "doris-streamloader business failure: status=Success, failLoadRows=0, loadedRows=0, totalRows=5000, message=OK",
		},
		{
			name: "Partial success - some rows failed",
			output: `{
				"Status": "Success",
				"Message": "OK",
				"TotalRows": 1000,
				"LoadedRows": 800,
				"FailLoadRows": 200
			}`,
			wantErr: true,
			errMsg:  "doris-streamloader business failure: status=Success, failLoadRows=200, loadedRows=800, totalRows=1000, message=OK",
		},
		{
			name: "Failed status",
			output: `{
				"Status": "Failed",
				"Message": "Connection timeout",
				"TotalRows": 0,
				"LoadedRows": 0,
				"FailLoadRows": 0
			}`,
			wantErr: true,
			errMsg:  "status=Failed, message=Connection timeout",
		},
		{
			name: "Error status with detailed message",
			output: `{
				"Status": "Error",
				"Message": "Table not found: test_table",
				"TotalRows": 0,
				"LoadedRows": 0,
				"FailLoadRows": 0
			}`,
			wantErr: true,
			errMsg:  "status=Error, message=Table not found: test_table",
		},
		{
			name:    "No status but contains 'fail'",
			output:  `Load operation failed due to network issues`,
			wantErr: true,
			errMsg:  "status=Unknown",
		},
		{
			name:    "No status but contains 'error'",
			output:  `An error occurred during processing`,
			wantErr: true,
			errMsg:  "status=Unknown",
		},
		{
			name:    "No status, no error keywords",
			output:  `Processing data... completed normally`,
			wantErr: false,
		},
		{
			name:    "Empty output",
			output:  "",
			wantErr: false,
		},
		{
			name: "Status with extra whitespace",
			output: `{
				"Status"  :  "Failed"  ,
				"Message"  :  "Invalid format",
				"TotalRows": 0,
				"LoadedRows": 0,
				"FailLoadRows": 0
			}`,
			wantErr: true,
			errMsg:  "status=Failed, message=Invalid format",
		},
		{
			name: "Mixed case SUCCESS",
			output: `{
				"Status": "SUCCESS",
				"Message": "Data loaded",
				"TotalRows": 100,
				"LoadedRows": 100,
				"FailLoadRows": 0
			}`,
			wantErr: false,
		},
		{
			name: "Real streamloader success output",
			output: `{
				"TxnId": 12345,
				"Label": "test_label",
				"Status": "Success",
				"Message": "OK",
				"NumberTotalRows": 1000,
				"NumberLoadedRows": 1000,
				"NumberFilteredRows": 0,
				"NumberUnselectedRows": 0,
				"LoadBytes": 102400,
				"LoadTimeMs": 1500,
				"TotalRows": 1000,
				"LoadedRows": 1000,
				"FailLoadRows": 0
			}`,
			wantErr: false,
		},
		{
			name: "Real streamloader failure output - all rows failed",
			output: `{
				"TxnId": -1,
				"Label": "test_label",
				"Status": "Success",
				"Message": "OK",
				"NumberTotalRows": 0,
				"NumberLoadedRows": 0,
				"NumberFilteredRows": 1000,
				"NumberUnselectedRows": 0,
				"LoadBytes": 0,
				"LoadTimeMs": 500,
				"TotalRows": 10000,
				"LoadedRows": 0,
				"FailLoadRows": 10000
			}`,
			wantErr: true,
			errMsg:  "doris-streamloader business failure: status=Success, failLoadRows=10000, loadedRows=0, totalRows=10000, message=OK",
		},
		{
			name: "Zero total rows - should not fail",
			output: `{
				"Status": "Success",
				"Message": "OK",
				"TotalRows": 0,
				"LoadedRows": 0,
				"FailLoadRows": 0
			}`,
			wantErr: false,
		},
		{
			name: "Missing business metrics - only status check",
			output: `{
				"Status": "Success",
				"Message": "OK"
			}`,
			wantErr: false,
		},
		{
			name: "Missing business metrics - failed status",
			output: `{
				"Status": "Failed",
				"Message": "Connection error"
			}`,
			wantErr: true,
			errMsg:  "status=Failed, message=Connection error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkStreamloaderOutput(tt.output)

			if tt.wantErr {
				if err == nil {
					t.Errorf("checkStreamloaderOutput() expected error but got nil")
					return
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("checkStreamloaderOutput() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("checkStreamloaderOutput() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestCheckStreamloaderOutputEdgeCases(t *testing.T) {
	// 测试正则表达式边界情况
	tests := []struct {
		name    string
		output  string
		wantErr bool
	}{
		{
			name:    "Status in middle of text",
			output:  `Some text "Status": "Failed" more text`,
			wantErr: true,
		},
		{
			name:    "Multiple Status fields - first one wins",
			output:  `{"Status": "Failed", "Status": "Success"}`,
			wantErr: true,
		},
		{
			name:    "Nested JSON with Status",
			output:  `{"data": {"Status": "Failed"}, "Status": "Success"}`,
			wantErr: false, // 外层的 Success 应该被匹配
		},
		{
			name:    "Status with special characters in message",
			output:  `{"Status": "Failed", "Message": "Error: \"invalid syntax\""}`,
			wantErr: true,
		},
		{
			name:    "Business metrics with special characters",
			output:  `{"Status": "Success", "TotalRows": 1000, "LoadedRows": 0, "FailLoadRows": 1000, "Message": "Error: \"data format\""}`,
			wantErr: true,
		},
		{
			name:    "Large numbers in metrics",
			output:  `{"Status": "Success", "TotalRows": 999999, "LoadedRows": 0, "FailLoadRows": 999999}`,
			wantErr: true,
		},
		{
			name:    "Negative numbers in metrics",
			output:  `{"Status": "Success", "TotalRows": -100, "LoadedRows": 0, "FailLoadRows": 0}`,
			wantErr: false, // 负数会被解析为0，不触发业务失败
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkStreamloaderOutput(tt.output)
			if tt.wantErr && err == nil {
				t.Errorf("checkStreamloaderOutput() expected error but got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("checkStreamloaderOutput() unexpected error = %v", err)
			}
		})
	}
}

func TestCheckStreamloaderOutputBusinessMetrics(t *testing.T) {
	// 专门测试业务指标的各种组合
	tests := []struct {
		name        string
		totalRows   int
		loadedRows  int
		failRows    int
		status      string
		wantErr     bool
		description string
	}{
		{name: "empty", totalRows: 0, loadedRows: 0, failRows: 0, status: "Success", wantErr: false, description: "空数据，应该成功"},
		{name: "all-success", totalRows: 1000, loadedRows: 1000, failRows: 0, status: "Success", wantErr: false, description: "全部成功"},
		{name: "partial-fail", totalRows: 1000, loadedRows: 800, failRows: 200, status: "Success", wantErr: true, description: "部分失败"},
		{name: "all-fail", totalRows: 1000, loadedRows: 0, failRows: 1000, status: "Success", wantErr: true, description: "全部失败"},
		{name: "no-loaded", totalRows: 1000, loadedRows: 0, failRows: 0, status: "Success", wantErr: true, description: "有数据但未加载"},
		{name: "no-total-but-fail", totalRows: 0, loadedRows: 0, failRows: 100, status: "Success", wantErr: true, description: "无总数据但有失败"},
		{name: "status-failed", totalRows: 1000, loadedRows: 1000, failRows: 0, status: "Failed", wantErr: true, description: "状态失败"},
		{name: "status-error", totalRows: 1000, loadedRows: 1000, failRows: 0, status: "Error", wantErr: true, description: "状态错误"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			output := `{
				"Status": "` + tt.status + `",
				"Message": "Test message",
				"TotalRows": ` + fmt.Sprintf("%d", tt.totalRows) + `,
				"LoadedRows": ` + fmt.Sprintf("%d", tt.loadedRows) + `,
				"FailLoadRows": ` + fmt.Sprintf("%d", tt.failRows) + `
			}`

			err := checkStreamloaderOutput(output)
			if tt.wantErr && err == nil {
				t.Errorf("Expected error for %s but got nil", tt.description)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error for %s: %v", tt.description, err)
			}
		})
	}
}
