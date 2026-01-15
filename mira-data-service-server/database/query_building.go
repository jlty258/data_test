/*
*

	@author: shiliang
	@date: 2025/2/21
	@note: 查询构建功能

*
*/
package database

import (
	pb "data-service/generated/datasource"
	"fmt"
	"strings"
)

type QueryBuilding interface {
	ExtractQueryArgs(filters []*pb.FilterValue) []interface{}
	OperatorToString(operator pb.FilterOperator) string
	BuildGroupCountQuery(tableName string, groupBy []string, filterNames []string, filterOperators []pb.FilterOperator, filterValues []*pb.FilterValue) (string, []interface{}, error)
}

type QueryBuilder struct {
}

func (q *QueryBuilder) ExtractQueryArgs(filters []*pb.FilterValue) []interface{} {
	args := make([]interface{}, 0) // 确保返回值是空切片，而非 nil
	for _, v := range filters {
		// 处理单值字段
		if v.StrValue != "" {
			args = append(args, v.StrValue)
		}
		if v.IntValue != 0 { // 默认值为 0，需根据业务逻辑判断是否允许 0
			args = append(args, int32(v.IntValue))
		}
		if v.FloatValue != 0.0 { // 默认值为 0.0，需根据业务逻辑判断是否允许 0.0
			args = append(args, v.FloatValue)
		}
		if v.BoolValue { // 仅在值为 true 时添加，假如 false 不被视为有效值
			args = append(args, v.BoolValue)
		}

		// 处理数组字段
		if len(v.StrValues) > 0 {
			for _, str := range v.StrValues {
				args = append(args, str)
			}
		}
		if len(v.IntValues) > 0 {
			for _, intval := range v.IntValues {
				args = append(args, int32(intval))
			}
		}
		if len(v.FloatValues) > 0 {
			for _, floatval := range v.FloatValues {
				args = append(args, floatval)
			}
		}
		if len(v.BoolValues) > 0 {
			for _, boolval := range v.BoolValues {
				args = append(args, boolval)
			}
		}
	}
	return args
}

func (q *QueryBuilder) OperatorToString(operator pb.FilterOperator) string {
	switch operator {
	case pb.FilterOperator_GREATER_THAN:
		return ">"
	case pb.FilterOperator_LESS_THAN:
		return "<"
	case pb.FilterOperator_GREATER_THAN_OR_EQUAL:
		return ">="
	case pb.FilterOperator_LESS_THAN_OR_EQUAL:
		return "<="
	case pb.FilterOperator_NOT_EQUAL:
		return "!="
	case pb.FilterOperator_LIKE_OPERATOR:
		return "LIKE"
	case pb.FilterOperator_IN_OPERATOR:
		return "IN"
	default:
		return "="
	}
}

// BuildGroupCountQuery 构建分组计数查询的SQL语句
func (q *QueryBuilder) BuildGroupCountQuery(tableName string, groupBy []string, filterNames []string,
	filterOperators []pb.FilterOperator, filterValues []*pb.FilterValue, dbType pb.DataSourceType) (string, []interface{}, error) {

	var queryBuilder strings.Builder
	args := []interface{}{}
	paramIndex := 1 // 用于Kingbase等使用$1,$2形式参数的数据库

	// 确定参数占位符格式
	var placeholder string
	if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
		placeholder = "$%d"
	} else {
		placeholder = "?"
	}

	// 构建 SELECT 子句，包含 COUNT(*)
	queryBuilder.WriteString("SELECT COUNT(*) as count FROM ")

	// 处理表名，Kingbase需要添加双引号
	if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
		queryBuilder.WriteString(fmt.Sprintf("\"%s\"", tableName))
	} else {
		queryBuilder.WriteString(tableName)
	}

	// 构建 WHERE 子句（如果有过滤条件）
	if len(filterNames) > 0 && len(filterOperators) > 0 && len(filterValues) > 0 {
		if len(filterNames) != len(filterOperators) || len(filterOperators) != len(filterValues) {
			return "", nil, fmt.Errorf("filterNames, filterOperators, and filterValues must have the same length")
		}

		queryBuilder.WriteString(" WHERE ")

		conditions := make([]string, len(filterNames))
		for i := range filterNames {
			operator := filterOperators[i]
			filterValue := filterValues[i]
			filterName := filterNames[i]

			// Kingbase需要给字段名添加双引号
			if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
				filterName = fmt.Sprintf("\"%s\"", filterName)
			}

			// 提取有效值
			argsFromFilter := q.ExtractQueryArgs([]*pb.FilterValue{filterValue})
			if len(argsFromFilter) == 0 {
				return "", nil, fmt.Errorf("no valid values found for filter '%s'", filterName)
			}

			switch operator {
			case pb.FilterOperator_IN_OPERATOR:
				// 构建 IN 操作符的条件
				var inPlaceholders []string
				for _, v := range argsFromFilter {
					if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
						inPlaceholders = append(inPlaceholders, fmt.Sprintf(placeholder, paramIndex))
						paramIndex++
					} else {
						inPlaceholders = append(inPlaceholders, placeholder)
					}
					args = append(args, v)
				}
				conditions[i] = fmt.Sprintf("%s IN (%s)", filterName, strings.Join(inPlaceholders, ", "))

			case pb.FilterOperator_GREATER_THAN, pb.FilterOperator_LESS_THAN,
				pb.FilterOperator_GREATER_THAN_OR_EQUAL, pb.FilterOperator_LESS_THAN_OR_EQUAL,
				pb.FilterOperator_NOT_EQUAL:
				// 构建比较操作符的条件
				if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
					conditions[i] = fmt.Sprintf("%s %s %s", filterName, q.OperatorToString(operator), fmt.Sprintf(placeholder, paramIndex))
					paramIndex++
				} else {
					conditions[i] = fmt.Sprintf("%s %s %s", filterName, q.OperatorToString(operator), placeholder)
				}
				args = append(args, argsFromFilter[0]) // 取第一个有效值

			case pb.FilterOperator_LIKE_OPERATOR:
				// 构建 LIKE 操作符的条件
				if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
					conditions[i] = fmt.Sprintf("%s LIKE %s", filterName, fmt.Sprintf(placeholder, paramIndex))
					paramIndex++
				} else {
					conditions[i] = fmt.Sprintf("%s LIKE %s", filterName, placeholder)
				}
				args = append(args, argsFromFilter[0]) // 取第一个有效值

			default:
				// 默认等于操作
				if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
					conditions[i] = fmt.Sprintf("%s = %s", filterName, fmt.Sprintf(placeholder, paramIndex))
					paramIndex++
				} else {
					conditions[i] = fmt.Sprintf("%s = %s", filterName, placeholder)
				}
				args = append(args, argsFromFilter[0]) // 取第一个有效值
			}
		}

		queryBuilder.WriteString(strings.Join(conditions, " AND "))
	}

	// 如果 groupBy 不为空，添加 GROUP BY 子句
	if len(groupBy) > 0 {
		queryBuilder.WriteString(" GROUP BY ")

		// 处理字段名，Kingbase需要添加双引号
		var processedGroupBy []string
		if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
			processedGroupBy = make([]string, len(groupBy))
			for i, field := range groupBy {
				processedGroupBy[i] = fmt.Sprintf("\"%s\"", field)
			}
		} else {
			processedGroupBy = groupBy
		}

		queryBuilder.WriteString(strings.Join(processedGroupBy, ", "))
	}

	return queryBuilder.String(), args, nil
}
