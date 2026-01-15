/*
*

	@author: shiliang
	@date: 2025/2/25
	@note:

*
*/
package database

import (
	pb "data-service/generated/datasource"
	"reflect"
	"testing"
)

func TestExtractQueryArgs(t *testing.T) {
	tests := []struct {
		name         string
		filterValues []*pb.FilterValue
		want         []interface{}
	}{
		{
			name: "Extracts string values",
			filterValues: []*pb.FilterValue{
				{StrValues: []string{"value1", "value2"}},
			},
			want: []interface{}{"value1", "value2"},
		},
		{
			name: "Extracts integer values",
			filterValues: []*pb.FilterValue{
				{IntValues: []int32{10, 20}},
			},
			want: []interface{}{int32(10), int32(20)},
		},
		{
			name: "Extracts float values",
			filterValues: []*pb.FilterValue{
				{FloatValues: []float64{1.1, 2.2}},
			},
			want: []interface{}{1.1, 2.2},
		},
		{
			name: "Extracts boolean values",
			filterValues: []*pb.FilterValue{
				{BoolValues: []bool{true, false}},
			},
			want: []interface{}{true, false},
		},
		{
			name:         "No values",
			filterValues: []*pb.FilterValue{},
			want:         []interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryBuilder := &QueryBuilder{}
			got := queryBuilder.ExtractQueryArgs(tt.filterValues)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractQueryArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOperatorToString(t *testing.T) {
	qb := &QueryBuilder{}

	tests := []struct {
		name     string
		operator pb.FilterOperator
		expected string
	}{
		{"GreaterThan", pb.FilterOperator_GREATER_THAN, ">"},
		{"LessThan", pb.FilterOperator_LESS_THAN, "<"},
		{"GreaterThanOrEqual", pb.FilterOperator_GREATER_THAN_OR_EQUAL, ">="},
		{"LessThanOrEqual", pb.FilterOperator_LESS_THAN_OR_EQUAL, "<="},
		{"NotEqual", pb.FilterOperator_NOT_EQUAL, "!="},
		{"LikeOperator", pb.FilterOperator_LIKE_OPERATOR, "LIKE"},
		{"InOperator", pb.FilterOperator_IN_OPERATOR, "IN"},
		{"Default", pb.FilterOperator(999), "="}, // 使用一个不存在的枚举值来测试默认分支
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.OperatorToString(tt.operator)
			if result != tt.expected {
				t.Errorf("OperatorToString(%v) = %v, want %v", tt.operator, result, tt.expected)
			}
		})
	}
}

func TestQueryBuilder_BuildGroupCountQuery(t *testing.T) {
	type args struct {
		tableName       string
		groupBy         []string
		filterNames     []string
		filterOperators []pb.FilterOperator
		filterValues    []*pb.FilterValue
		dbType          pb.DataSourceType
	}
	tests := []struct {
		name       string
		args       args
		wantQuery  string
		wantArgs   []interface{}
		wantErr    bool
		errMessage string
	}{
		{
			name: "空分组字段测试",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{}, // 允许为空
				filterNames:     []string{},
				filterOperators: []pb.FilterOperator{},
				filterValues:    []*pb.FilterValue{},
				dbType:          pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			},
			wantQuery: "SELECT COUNT(*) as count FROM test_table",
			wantArgs:  []interface{}{},
			wantErr:   false, // 现在不再报错
		},
		{
			name: "MySQL基本分组查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category", "region"},
				filterNames:     []string{},
				filterOperators: []pb.FilterOperator{},
				filterValues:    []*pb.FilterValue{},
				dbType:          pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			},
			wantQuery: "SELECT COUNT(*) as count FROM test_table GROUP BY category, region",
			wantArgs:  []interface{}{},
			wantErr:   false,
		},
		{
			name: "Kingbase基本分组查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category", "region"},
				filterNames:     []string{},
				filterOperators: []pb.FilterOperator{},
				filterValues:    []*pb.FilterValue{},
				dbType:          pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			},
			wantQuery: "SELECT COUNT(*) as count FROM \"test_table\" GROUP BY \"category\", \"region\"",
			wantArgs:  []interface{}{},
			wantErr:   false,
		},
		{
			name: "MySQL带等于条件的分组查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category"},
				filterNames:     []string{"status"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL},
				filterValues: []*pb.FilterValue{
					{
						StrValue: "active",
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			},
			wantQuery: "SELECT COUNT(*) as count FROM test_table WHERE status = ? GROUP BY category",
			wantArgs:  []interface{}{"active"},
			wantErr:   false,
		},
		{
			name: "Kingbase带等于条件的分组查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category"},
				filterNames:     []string{"status"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL},
				filterValues: []*pb.FilterValue{
					{
						StrValue: "active",
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			},
			wantQuery: "SELECT COUNT(*) as count FROM \"test_table\" WHERE \"status\" = $1 GROUP BY \"category\"",
			wantArgs:  []interface{}{"active"},
			wantErr:   false,
		},
		{
			name: "MySQL带IN条件的分组查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category"},
				filterNames:     []string{"region"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_IN_OPERATOR},
				filterValues: []*pb.FilterValue{
					{
						StrValues: []string{"北京", "上海", "广州"},
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			},
			wantQuery: "SELECT COUNT(*) as count FROM test_table WHERE region IN (?, ?, ?) GROUP BY category",
			wantArgs:  []interface{}{"北京", "上海", "广州"},
			wantErr:   false,
		},
		{
			name: "Kingbase带IN条件的分组查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category"},
				filterNames:     []string{"region"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_IN_OPERATOR},
				filterValues: []*pb.FilterValue{
					{
						StrValues: []string{"北京", "上海", "广州"},
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			},
			wantQuery: "SELECT COUNT(*) as count FROM \"test_table\" WHERE \"region\" IN ($1, $2, $3) GROUP BY \"category\"",
			wantArgs:  []interface{}{"北京", "上海", "广州"},
			wantErr:   false,
		},
		{
			name: "MySQL带多个条件的分组查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category", "year"},
				filterNames:     []string{"status", "price"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL, pb.FilterOperator_GREATER_THAN},
				filterValues: []*pb.FilterValue{
					{
						StrValue: "active",
					},
					{
						FloatValue: 100.0,
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			},
			wantQuery: "SELECT COUNT(*) as count FROM test_table WHERE status = ? AND price > ? GROUP BY category, year",
			wantArgs:  []interface{}{"active", float64(100.0)},
			wantErr:   false,
		},
		{
			name: "Kingbase带多个条件的分组查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category", "year"},
				filterNames:     []string{"status", "price"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL, pb.FilterOperator_GREATER_THAN},
				filterValues: []*pb.FilterValue{
					{
						StrValue: "active",
					},
					{
						FloatValue: 100.0,
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			},
			wantQuery: "SELECT COUNT(*) as count FROM \"test_table\" WHERE \"status\" = $1 AND \"price\" > $2 GROUP BY \"category\", \"year\"",
			wantArgs:  []interface{}{"active", float64(100.0)},
			wantErr:   false,
		},
		{
			name: "过滤条件参数不匹配测试",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category"},
				filterNames:     []string{"status", "price"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL},
				filterValues: []*pb.FilterValue{
					{
						StrValue: "active",
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			},
			wantQuery:  "",
			wantArgs:   nil,
			wantErr:    true,
			errMessage: "filterNames, filterOperators, and filterValues must have the same length",
		},
		{
			name: "无有效过滤值测试",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{"category"},
				filterNames:     []string{"status"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL},
				filterValues: []*pb.FilterValue{
					{}, // 空过滤值
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			},
			wantQuery:  "",
			wantArgs:   nil,
			wantErr:    true,
			errMessage: "no valid values found for filter 'status'",
		},
		// 添加有WHERE条件但没有GROUP BY的测试
		{
			name: "MySQL有WHERE条件无GROUP BY的查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{}, // 无分组字段
				filterNames:     []string{"status"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL},
				filterValues: []*pb.FilterValue{
					{
						StrValue: "inactive",
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL,
			},
			wantQuery: "SELECT COUNT(*) as count FROM test_table WHERE status = ?",
			wantArgs:  []interface{}{"inactive"},
			wantErr:   false,
		},
		{
			name: "Kingbase有WHERE条件无GROUP BY的查询",
			args: args{
				tableName:       "test_table",
				groupBy:         []string{}, // 无分组字段
				filterNames:     []string{"status"},
				filterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL},
				filterValues: []*pb.FilterValue{
					{
						StrValue: "inactive",
					},
				},
				dbType: pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE,
			},
			wantQuery: "SELECT COUNT(*) as count FROM \"test_table\" WHERE \"status\" = $1",
			wantArgs:  []interface{}{"inactive"},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &QueryBuilder{}
			gotQuery, gotArgs, err := q.BuildGroupCountQuery(
				tt.args.tableName,
				tt.args.groupBy,
				tt.args.filterNames,
				tt.args.filterOperators,
				tt.args.filterValues,
				tt.args.dbType,
			)

			// 检查错误
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildGroupCountQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// 如果期望错误，检查错误消息
			if tt.wantErr && err != nil && err.Error() != tt.errMessage {
				t.Errorf("BuildGroupCountQuery() error message = %v, want %v", err.Error(), tt.errMessage)
				return
			}

			// 检查查询字符串
			if gotQuery != tt.wantQuery {
				t.Errorf("BuildGroupCountQuery() gotQuery = %v, want %v", gotQuery, tt.wantQuery)
			}

			// 检查参数
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("BuildGroupCountQuery() gotArgs = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}
