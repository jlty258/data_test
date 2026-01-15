package database

import (
	ds "data-service/generated/datasource"
	"data-service/log"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func setup() {
	viper.Set("LoggerConfig.Level", "debug") // 模拟配置项
	viper.Set("TestConfig", "true")
	viper.AutomaticEnv()        // 启用自动环境变量加载（如果有需要）
	viper.SetConfigType("yaml") // 确保配置类型是 yaml（如果依赖 yaml 类型）
	viper.SetConfigFile("")     // 禁用文件读取，避免默认路径读取配置文件

	log.InitLogger()
}

// TestBuildWithConditionQuery tests the BuildWithConditionQuery function
func TestBuildWithConditionQueryKingbase(t *testing.T) {
	// 测试用例
	testCases := []struct {
		name            string
		tableName       string
		fields          []string
		filterNames     []string
		filterOperators []ds.FilterOperator
		filterValues    []*ds.FilterValue
		sortRules       []*ds.SortRule
		expectedQuery   string
		expectedArgs    []interface{}
	}{
		{
			name:            "Test Greater Than Operator with ORDER BY",
			tableName:       "users",
			fields:          []string{"id", "name"},
			filterNames:     []string{"age"},
			filterOperators: []ds.FilterOperator{ds.FilterOperator_GREATER_THAN},
			filterValues:    []*ds.FilterValue{{IntValues: []int32{30}}},
			sortRules: []*ds.SortRule{
				{FieldName: "name", SortOrder: ds.SortOrder_ASC},
			},
			expectedQuery: `SELECT "id", "name" FROM "users" WHERE age > $1 ORDER BY name ASC`,
			expectedArgs:  []interface{}{int32(30)},
		},
		{
			name:            "Test Multiple Conditions with ORDER BY",
			tableName:       "orders",
			fields:          []string{"order_id", "customer_id", "total_amount"},
			filterNames:     []string{"status", "total_amount"},
			filterOperators: []ds.FilterOperator{ds.FilterOperator_EQUAL, ds.FilterOperator_GREATER_THAN_OR_EQUAL},
			filterValues: []*ds.FilterValue{
				{StrValues: []string{"completed"}},
				{FloatValues: []float64{100.0}},
			},
			sortRules: []*ds.SortRule{
				{FieldName: "total_amount", SortOrder: ds.SortOrder_DESC},
				{FieldName: "customer_id", SortOrder: ds.SortOrder_ASC},
			},
			expectedQuery: `SELECT "order_id", "customer_id", "total_amount" FROM "orders" WHERE status = $1 AND total_amount >= $2 ORDER BY total_amount DESC, customer_id ASC`,
			expectedArgs:  []interface{}{"completed", float64(100.0)},
		},
		{
			name:            "Test Query with ORDER BY Only",
			tableName:       "products",
			fields:          []string{"product_id", "name", "price"},
			filterNames:     []string{},
			filterOperators: []ds.FilterOperator{},
			filterValues:    []*ds.FilterValue{},
			sortRules: []*ds.SortRule{
				{FieldName: "price", SortOrder: ds.SortOrder_DESC},
			},
			expectedQuery: `SELECT "product_id", "name", "price" FROM "products" ORDER BY price DESC`,
			expectedArgs:  []interface{}{},
		},
		{
			name:            "Test Query without Sorting or Conditions",
			tableName:       "customers",
			fields:          []string{"customer_id", "first_name", "last_name"},
			filterNames:     []string{},
			filterOperators: []ds.FilterOperator{},
			filterValues:    []*ds.FilterValue{},
			sortRules:       []*ds.SortRule{},
			expectedQuery:   `SELECT "customer_id", "first_name", "last_name" FROM "customers"`,
			expectedArgs:    []interface{}{},
		},
	}

	// 遍历测试用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kbStrategy := KingbaseStrategy{}
			query, args, err := kbStrategy.BuildWithConditionQuery(tc.tableName, tc.fields, tc.filterNames, tc.filterOperators, tc.filterValues, tc.sortRules)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// 检查生成的查询
			if query != tc.expectedQuery {
				t.Errorf("Expected query: %s, got: %s", tc.expectedQuery, query)
			}

			// 检查生成的参数
			if !reflect.DeepEqual(args, tc.expectedArgs) {
				t.Errorf("Expected args: %v, got: %v", tc.expectedArgs, args)
			}
		})
	}
}

func TestBuildCreateKingbaseTableSQL(t *testing.T) {
	// Define your test cases
	testCases := []struct {
		name      string
		schema    *arrow.Schema
		tableName string
		wantSQL   string
	}{
		{
			name: "Test with String and Decimal columns",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "name", Type: &arrow.StringType{}},
				{Name: "amount", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
			}, nil),
			tableName: "test_table",
			wantSQL:   "CREATE TABLE \"test_table\" (\"name\" VARCHAR(255), \"amount\" DECIMAL(10,2))",
		},
		// Add more test cases as necessary
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotSQL := buildCreateKingbaseTableSQL(tc.tableName, tc.schema)
			assert.Equal(t, tc.wantSQL, gotSQL, "SQL statement does not match expected output")
		})
	}
}

func TestRowsToArrowBatch(t *testing.T) {
	setup()

}
