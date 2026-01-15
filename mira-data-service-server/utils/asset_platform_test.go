package utils

import (
	pb2 "data-service/generated/datasource"
	"testing"
)

// TestBuildQuery 测试 BuildQuery 函数
func TestBuildQuery(t *testing.T) {
	tests := []struct {
		tableName  string
		fields     []string
		sourceType pb2.DataSourceType
		want       string
	}{
		{
			tableName:  "users",
			fields:     []string{"id", "name"},
			sourceType: pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			want:       "SELECT id, name FROM \"users\"",
		},
		{
			tableName:  "users",
			fields:     []string{},
			sourceType: pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			want:       "SELECT * FROM \"users\"",
		},
		// Add more test cases as needed
		{
			tableName:  "users",
			fields:     []string{},
			sourceType: pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			want:       "SELECT * FROM users",
		},
		{
			tableName:  "users",
			fields:     []string{"id"},
			sourceType: pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			want:       "SELECT id FROM users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := BuildQuery(tt.tableName, tt.fields, tt.sourceType)
			if got != tt.want {
				t.Errorf("BuildQuery() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBuildQueryWithOrder tests the BuildQueryWithOrder function
func TestBuildQueryWithOrder(t *testing.T) {
	tests := []struct {
		tableName string
		fields    []string
		sortRules []*pb2.SortRule
		want      string
	}{
		{
			tableName: "users",
			fields:    []string{"id", "name"},
			sortRules: nil,
			want:      "SELECT id, name FROM users",
		},
		{
			tableName: "users",
			fields:    nil,
			sortRules: nil,
			want:      "SELECT * FROM users",
		},
		{
			tableName: "users",
			fields:    []string{"id"},
			sortRules: []*pb2.SortRule{{FieldName: "name", SortOrder: pb2.SortOrder_DESC}},
			want:      "SELECT id FROM users ORDER BY name DESC",
		},
		{
			tableName: "employees",
			fields:    []string{"id", "name", "age", "salary"},
			sortRules: []*pb2.SortRule{
				{FieldName: "age", SortOrder: pb2.SortOrder_ASC},
				{FieldName: "salary", SortOrder: pb2.SortOrder_DESC},
			},
			want: "SELECT id, name, age, salary FROM employees ORDER BY age ASC, salary DESC",
		},
		{
			tableName: "orders",
			fields:    []string{"order_id", "customer_id", "order_date"},
			sortRules: []*pb2.SortRule{
				{FieldName: "order_date", SortOrder: pb2.SortOrder_ASC},
				{FieldName: "customer_id", SortOrder: pb2.SortOrder_ASC},
			},
			want: "SELECT order_id, customer_id, order_date FROM orders ORDER BY order_date ASC, customer_id ASC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := BuildQueryWithOrder(tt.tableName, tt.fields, tt.sortRules)
			if got != tt.want {
				t.Errorf("BuildQueryWithOrder() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBuildSelectQuery 测试 BuildSelectQuery 函数
func TestBuildSelectQuery(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		args       []interface{}
		dbType     pb2.DataSourceType
		want       string
		wantErr    bool
		errMessage string
	}{
		{
			name:       "MySQL placeholder with sufficient args",
			query:      "SELECT * FROM users WHERE id = ?",
			args:       []interface{}{1},
			dbType:     pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			want:       "SELECT * FROM users WHERE id = 1",
			wantErr:    false,
			errMessage: "",
		},
		{
			name:       "MySQL placeholder with insufficient args",
			query:      "SELECT * FROM users WHERE id = ?",
			args:       []interface{}{},
			dbType:     pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			want:       "",
			wantErr:    true,
			errMessage: "insufficient arguments for query",
		},
		{
			name:       "Kingbase placeholder with sufficient args",
			query:      "SELECT * FROM users WHERE id = $1",
			args:       []interface{}{1},
			dbType:     pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			want:       "SELECT * FROM users WHERE id = 1",
			wantErr:    false,
			errMessage: "",
		},
		{
			name:       "Kingbase placeholder with insufficient args",
			query:      "SELECT * FROM users WHERE id = $1",
			args:       []interface{}{},
			dbType:     pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			want:       "",
			wantErr:    true,
			errMessage: "insufficient arguments for query",
		},
		{
			name:       "Kingbase complex query with multiple placeholders",
			query:      "SELECT order_id, customer_id, total_amount FROM orders WHERE status = $1 AND total_amount >= $2 ORDER BY total_amount DESC, customer_id ASC",
			args:       []interface{}{"completed", 100.01},
			dbType:     pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			want:       "SELECT order_id, customer_id, total_amount FROM orders WHERE status = 'completed' AND total_amount >= 100.01 ORDER BY total_amount DESC, customer_id ASC",
			wantErr:    false,
			errMessage: "",
		},
		{
			name:       "Extra arguments",
			query:      "SELECT * FROM users",
			args:       []interface{}{1, 2},
			dbType:     pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			want:       "",
			wantErr:    true,
			errMessage: "too many arguments for query",
		},
		// Add more test cases as necessary
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildSelectQuery(tt.query, tt.args, tt.dbType)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildSelectQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && err.Error() != tt.errMessage {
				t.Errorf("BuildSelectQuery() error = %v, errMessage %v", err, tt.errMessage)
				return
			}
			if got != tt.want {
				t.Errorf("BuildSelectQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestContainsIgnoreCase is a test function for containsIgnoreCase.
func TestContainsIgnoreCase(t *testing.T) {
	testCases := []struct {
		s        string
		substr   string
		expected bool
	}{
		{"timestamp without time zone", "timestamp", true},
	}

	for _, tc := range testCases {
		t.Run(tc.s+"_"+tc.substr, func(t *testing.T) {
			result := containsIgnoreCase(tc.s, tc.substr)
			if result != tc.expected {
				t.Errorf("containsIgnoreCase(%q, %q) = %v; expected %v", tc.s, tc.substr, result, tc.expected)
			}
		})
	}
}
