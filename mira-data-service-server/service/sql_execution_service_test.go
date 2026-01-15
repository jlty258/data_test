package service

import (
	"testing"

	"data-service/mocks"

	"github.com/golang/mock/gomock"
)

func TestSqlExecutionService_insertBatchToTable(t *testing.T) {
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// 创建 mock DorisService
	mockDorisService := mocks.NewMockIDorisService(ctrl)

	// 创建测试用的 SqlExecutionService 实例
	service := &SqlExecutionService{
		dorisService: mockDorisService,
	}

	tests := []struct {
		name      string
		tableName string
		columns   []string
		rows      [][]interface{}
		wantErr   bool
		setupMock func()
	}{
		{
			name:      "empty rows should return nil",
			tableName: "test_table",
			columns:   []string{"id", "name"},
			rows:      [][]interface{}{},
			wantErr:   false,
			setupMock: func() {
				// 空行不需要调用 mock
			},
		},
		{
			name:      "single row insert",
			tableName: "test_table",
			columns:   []string{"id", "name"},
			rows: [][]interface{}{
				{1, "Alice"},
			},
			wantErr: false,
			setupMock: func() {
				mockDorisService.EXPECT().
					ExecuteUpdate(
						"INSERT INTO test_table (id,name) VALUES (?,?)", // 精确的 SQL
						1, "Alice", // 精确的参数值
					).
					Return(int64(1), nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 设置 mock 期望
			tt.setupMock()

			// 执行测试
			err := service.insertBatchToTable(tt.tableName, tt.columns, tt.rows)

			// 验证结果
			if (err != nil) != tt.wantErr {
				t.Errorf("insertBatchToTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
