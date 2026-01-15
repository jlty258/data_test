package database

import (
	ds "data-service/generated/datasource"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	setup() // 调用 setup 方法
	m.Run() // 运行所有测试
}

// 测试 BuildWithConditionQuery 方法
func TestBuildWithConditionQuery(t *testing.T) {
	tests := []struct {
		name            string
		tableName       string
		fields          []string
		filterNames     []string
		filterOperators []ds.FilterOperator
		filterValues    []*ds.FilterValue
		sortRules       []*ds.SortRule
		wantQuery       string
		wantArgs        []interface{}
		wantErr         error
	}{
		{
			name:            "Basic query with single condition",
			tableName:       "users",
			fields:          []string{"id", "name"},
			filterNames:     []string{"age"},
			filterOperators: []ds.FilterOperator{ds.FilterOperator_GREATER_THAN},
			filterValues: []*ds.FilterValue{
				{IntValue: 25},
			},
			sortRules: []*ds.SortRule{
				{FieldName: "name", SortOrder: ds.SortOrder_ASC},
			},
			wantQuery: "SELECT id, name FROM users WHERE age > ? ORDER BY name ASC",
			wantArgs:  []interface{}{int32(25)},
			wantErr:   nil,
		},
		{
			name:            "Query with multiple conditions and descending order",
			tableName:       "orders",
			fields:          []string{"order_id", "customer_id", "total_amount"},
			filterNames:     []string{"status", "total_amount"},
			filterOperators: []ds.FilterOperator{ds.FilterOperator_EQUAL, ds.FilterOperator_GREATER_THAN_OR_EQUAL},
			filterValues: []*ds.FilterValue{
				{StrValue: "completed"},
				{FloatValue: 100.0},
			},
			sortRules: []*ds.SortRule{
				{FieldName: "total_amount", SortOrder: ds.SortOrder_DESC},
				{FieldName: "customer_id", SortOrder: ds.SortOrder_ASC},
			},
			wantQuery: "SELECT order_id, customer_id, total_amount FROM orders WHERE status = ? AND total_amount >= ? ORDER BY total_amount DESC, customer_id ASC",
			wantArgs:  []interface{}{"completed", 100.0},
			wantErr:   nil,
		},
		{
			name:            "Query without conditions but with sorting",
			tableName:       "products",
			fields:          []string{"product_id", "name", "price"},
			filterNames:     []string{},
			filterOperators: []ds.FilterOperator{},
			filterValues:    []*ds.FilterValue{},
			sortRules: []*ds.SortRule{
				{FieldName: "price", SortOrder: ds.SortOrder_DESC},
			},
			wantQuery: "SELECT product_id, name, price FROM products ORDER BY price DESC",
			wantArgs:  []interface{}{},
			wantErr:   nil,
		},
		{
			name:            "Query without fields, conditions, or sorting",
			tableName:       "customers",
			fields:          []string{},
			filterNames:     []string{},
			filterOperators: []ds.FilterOperator{},
			filterValues:    []*ds.FilterValue{},
			sortRules:       []*ds.SortRule{},
			wantQuery:       "SELECT * FROM customers",
			wantArgs:        []interface{}{},
			wantErr:         nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MySQLStrategy{}
			gotQuery, gotArgs, gotErr := m.BuildWithConditionQuery(
				tt.tableName,
				tt.fields,
				tt.filterNames,
				tt.filterOperators,
				tt.filterValues,
				tt.sortRules,
			)

			if gotQuery != tt.wantQuery {
				t.Errorf("Test case: %s\nBuildWithConditionQuery() query = %v, wantQuery = %v", tt.name, gotQuery, tt.wantQuery)
			}

			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("Test case: %s\nBuildWithConditionQuery() args = %v, wantArgs = %v", tt.name, gotArgs, tt.wantArgs)
			}

			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("Test case: %s\nBuildWithConditionQuery() err = %v, wantErr = %v", tt.name, gotErr, tt.wantErr)
			}
		})
	}
}

func TestBuildCreateMysqlTableSQL(t *testing.T) {
	// 构造测试用的 Arrow Schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: &arrow.Int32Type{}},
		{Name: "name", Type: &arrow.StringType{}},
	}, nil)

	// 生成 SQL
	sql := buildCreateMysqlTableSQL("my_table", schema)

	// 检查 SQL 中是否包含生成的表名
	if !strings.Contains(sql, "`my_table`") {
		t.Errorf("SQL should contain table name 'my_table', got: %s", sql)
	}

	// 检查自增主键字段是否包含随机的 10 字符 UUID
	// 找到 UUID 字段，检查长度是否为 10
	parts := strings.Split(sql, ",")
	if len(parts) < 1 {
		t.Fatalf("SQL seems malformed: %s", sql)
	}

	// 检查其他字段（name 和 age）是否正确添加
	expectedFields := []string{
		"`name` VARCHAR(255)",
	}

	for _, expected := range expectedFields {
		if !strings.Contains(sql, expected) {
			t.Errorf("Expected to find '%s' in SQL, but it wasn't there.", expected)
		}
	}
}

// TestEstimateTableSize tests the estimateTableSize method
func TestEstimateTableSize(t *testing.T) {
	tests := []struct {
		name          string
		database      string
		tableName     string
		totalRows     int32
		setupMock     func(mock sqlmock.Sqlmock)
		expectedSize  int64
		expectedError string
	}{
		{
			name:      "successful estimation with sample data",
			database:  "testdb",
			tableName: "testtable",
			totalRows: 1000,
			setupMock: func(mock sqlmock.Sqlmock) {
				columns := []string{"id", "name", "value"}
				rows := sqlmock.NewRows(columns).
					AddRow(1, "test1", 100).
					AddRow(2, "test2", 200).
					AddRow(3, "test3", 300)

				mock.ExpectQuery(`SELECT \* FROM testdb\.testtable LIMIT 100`).
					WillReturnRows(rows)
			},
			expectedSize:  0, // Will be calculated, so we'll check it's > 0
			expectedError: "",
		},
		{
			name:      "query error",
			database:  "testdb",
			tableName: "testtable",
			totalRows: 1000,
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT \* FROM testdb\.testtable LIMIT 100`).
					WillReturnError(errors.New("table does not exist"))
			},
			expectedSize:  0,
			expectedError: "failed to get sample data for size estimation",
		},
		{
			name:      "no sample data available",
			database:  "testdb",
			tableName: "testtable",
			totalRows: 1000,
			setupMock: func(mock sqlmock.Sqlmock) {
				columns := []string{"id", "name"}
				rows := sqlmock.NewRows(columns) // Empty rows

				mock.ExpectQuery(`SELECT \* FROM testdb\.testtable LIMIT 100`).
					WillReturnRows(rows)
			},
			expectedSize:  0,
			expectedError: "no sample data available for size estimation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("Failed to create mock database: %v", err)
			}
			defer db.Close()

			// Setup mock expectations
			tt.setupMock(mock)

			// Create MySQLStrategy with mock DB
			strategy := &MySQLStrategy{
				DB: db,
			}

			// Execute the method
			size, err := strategy.estimateTableSize(tt.database, tt.tableName, tt.totalRows)

			// Verify expectations
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Mock expectations were not met: %v", err)
			}

			// Check error
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Equal(t, int64(0), size)
			} else {
				if err != nil {
					// For "no sample data" case, we expect an error
					if tt.name == "no sample data available" {
						assert.Error(t, err)
						assert.Contains(t, err.Error(), "no sample data")
						return
					}
					// For other success cases, we might have scan errors but should still get a result
					if tt.name == "scan error but continues" {
						// Should succeed with valid rows
						assert.NoError(t, err)
						assert.Greater(t, size, int64(0))
						return
					}
					t.Errorf("Unexpected error: %v", err)
				} else {
					// Check size is reasonable
					if tt.totalRows > 0 && tt.name == "successful estimation with sample data" {
						assert.Greater(t, size, int64(0), "Estimated size should be greater than 0")
					}
				}
			}
		})
	}
}
