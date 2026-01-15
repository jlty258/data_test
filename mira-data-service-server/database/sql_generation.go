/*
*

	@author: shiliang
	@date: 2025/2/21
	@note: sql生成器

*
*/
package database

import (
	"data-service/common"
	"data-service/config"
	pb "data-service/generated/datasource"
	"data-service/log"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v15/arrow"
)

type SQLGeneration interface {
	GenerateInsertSQL(tableName string, rowData []interface{}, schema *arrow.Schema, dbType pb.DataSourceType) (string, error)
	BuildExportSQL(request *pb.ExportCsvFileFromDorisRequest, conf *config.DataServiceConf) string
	BuildSelectIntoOutfileSQL(request *pb.ExportCsvFileFromDorisRequest, conf *config.DataServiceConf) string
}

type SQLGenerator struct {
}

func (s *SQLGenerator) GenerateInsertSQL(tableName string, rowData []interface{}, schema *arrow.Schema, dbType pb.DataSourceType) (string, error) {
	numCols := schema.NumFields()
	if len(rowData)%numCols != 0 {
		return "", fmt.Errorf("row data length (%d) does not match schema length (%d)", len(rowData), numCols)
	}

	// 构建列名
	var columns []string
	for i := 0; i < numCols; i++ {
		fieldName := schema.Field(i).Name
		if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
			// Kingbase 使用双引号
			columns = append(columns, fmt.Sprintf("\"%s\"", fieldName))
		} else {
			// MySQL 及其他数据库使用反引号
			columns = append(columns, fmt.Sprintf("`%s`", fieldName))
		}
	}
	columnsStr := strings.Join(columns, ", ")

	// 构建占位符与插入值
	var placeholders []string
	var values []string
	rowCount := len(rowData) / numCols
	var sql string
	if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
		// Kingbase 占位符从 $1 开始递增
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			var singleRowPlaceholders []string
			for colIdx := 0; colIdx < numCols; colIdx++ {
				singleRowPlaceholders = append(singleRowPlaceholders, fmt.Sprintf("$%d", rowIdx*numCols+colIdx+1))
				values = append(values, fmt.Sprintf("%v", rowData[rowIdx*numCols+colIdx]))
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(singleRowPlaceholders, ", ")))
		}
		placeholdersStr := strings.Join(placeholders, ", ")

		// 构建 SQL 语句
		sql = fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s", tableName, columnsStr, placeholdersStr)
	} else if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL {
		// MySQL 占位符使用问号
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			var singleRowPlaceholders []string
			for colIdx := 0; colIdx < numCols; colIdx++ {
				singleRowPlaceholders = append(singleRowPlaceholders, "?")
				values = append(values, fmt.Sprintf("%v", rowData[rowIdx*numCols+colIdx]))
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(singleRowPlaceholders, ", ")))
		}
		placeholdersStr := strings.Join(placeholders, ", ")

		// 构建 SQL 语句
		sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columnsStr, placeholdersStr)
	}
	log.Logger.Debugf("Generated SQL: %s", sql)
	return sql, nil
}

// buildExportSQL 构建用于从Doris导出CSV文件的EXPORT SQL语句
//
// 该方法根据导出请求参数和系统配置，构建完整的EXPORT SQL语句，用于将Doris表中的数据
// 导出为CSV格式并存储到S3兼容的对象存储中（如MinIO）。
//
// 参数:
//   - request: 导出请求参数，包含数据库名、表名、列名、分隔符等配置信息
//   - conf: 系统配置信息，包含OSS/S3连接配置
//
// 返回值:
//   - string: 完整的EXPORT SQL语句
//
// 功能说明:
//  1. 构建S3目标路径，格式为 s3://bucket/jobInstanceId/export_
//  2. 配置CSV格式参数，包括列分隔符、行分隔符、列选择等
//  3. 配置S3连接参数，包括端点、区域、访问密钥等
//  4. 组装完整的EXPORT TABLE SQL语句
//
// 示例生成的SQL:
//
//	EXPORT TABLE db.table TO "s3://bucket/job123/export_"
//	PROPERTIES (
//	    "format" = "csv_with_names",
//	    "column_separator" = ",",
//	    "line_delimiter" = "\n",
//	    "columns" = "col1,col2,col3"
//	) WITH s3 (
//	    "s3.endpoint" = "http://minio:9000",
//	    "s3.region" = "us-east-1",
//	    "s3.secret_key" = "secret",
//	    "s3.access_key" = "access"
//	)
//
// BuildExportSQL 构建 EXPORT TABLE ... WITH s3 的导出SQL
func (s *SQLGenerator) BuildExportSQL(request *pb.ExportCsvFileFromDorisRequest, conf *config.DataServiceConf, labelName string) string {
	// 构建目标路径
	targetPath := fmt.Sprintf("s3://%s/%s/export_", common.BATCH_DATA_BUCKET_NAME, request.JobInstanceId)

	// 构建列选择
	columnsClause := ""
	if len(request.Columns) > 0 {
		columnsClause = fmt.Sprintf(`"columns" = "%s"`, strings.Join(request.Columns, ","))
	}

	// 构建分隔符配置
	columnSeparator := request.ColumnSeparator
	if columnSeparator == "" {
		columnSeparator = ","
	}

	lineDelimiter := request.LineDelimiter
	if lineDelimiter == "" {
		lineDelimiter = "\\n"
	} else {
		lineDelimiter = strings.ReplaceAll(lineDelimiter, "\n", "\\n")
		lineDelimiter = strings.ReplaceAll(lineDelimiter, "\r", "\\r")
		lineDelimiter = strings.ReplaceAll(lineDelimiter, "\r\n", "\\r\\n")
	}

	// 构建PROPERTIES部分
	properties := []string{
		fmt.Sprintf(`"label" = "%s"`, labelName),
		`"format" = "csv_with_names"`,
		fmt.Sprintf(`"column_separator" = "%s"`, columnSeparator),
		fmt.Sprintf(`"line_delimiter" = "%s"`, lineDelimiter),
	}

	if columnsClause != "" {
		properties = append(properties, columnsClause)
	}

	// 构建S3配置
	config := config.GetConfigMap()
	s3Config := []string{
		fmt.Sprintf(`"s3.endpoint" = "http://%s:%d"`, config.OSSConfig.Host, config.OSSConfig.Port),
		`"s3.region" = "us-east-l"`,
		fmt.Sprintf(`"s3.secret_key" = "%s"`, config.OSSConfig.SecretKey),
		fmt.Sprintf(`"s3.access_key" = "%s"`, config.OSSConfig.AccessKey),
		`"use_path_style" = "true"`,
	}

	// 构建完整的EXPORT SQL
	exportSQL := fmt.Sprintf(`
		EXPORT TABLE %s.%s TO "%s" 
		PROPERTIES (
			%s
		) WITH s3 (
			%s
		)
	`,
		request.DbName,
		request.TableName,
		targetPath,
		strings.Join(properties, ",\n\t\t"),
		strings.Join(s3Config, ",\n\t\t"),
	)

	return exportSQL
}

// BuildSelectIntoOutfileSQL 构建带排序/过滤的 SELECT ... INTO OUTFILE 导出SQL
func (s *SQLGenerator) BuildSelectIntoOutfileSQL(request *pb.ExportCsvFileFromDorisRequest, conf *config.DataServiceConf) string {
	// 列选择
	columnsClause := "*"
	if len(request.Columns) > 0 {
		columnsClause = strings.Join(request.Columns, ", ")
	}

	// WHERE 子句
	var whereParts []string
	for _, fc := range request.FilterConditions {
		if fc == nil || fc.FieldName == "" {
			continue
		}
		whereParts = append(whereParts, buildSingleCondition(fc))
	}
	whereClause := ""
	if len(whereParts) > 0 {
		whereClause = " WHERE " + strings.Join(whereParts, " AND ")
	}

	// ORDER BY 子句
	var orderParts []string
	for _, sr := range request.SortRules {
		if sr == nil || sr.FieldName == "" {
			continue
		}
		dir := "ASC"
		if sr.SortOrder == pb.SortOrder_DESC {
			dir = "DESC"
		}
		orderParts = append(orderParts, fmt.Sprintf("%s %s", sr.FieldName, dir))
	}
	orderClause := ""
	if len(orderParts) > 0 {
		orderClause = " ORDER BY " + strings.Join(orderParts, ", ")
	}

	// 目标路径
	targetPath := fmt.Sprintf("s3://%s/%s/export_", common.BATCH_DATA_BUCKET_NAME, request.JobInstanceId)

	// 添加分隔符配置
	columnSeparator := request.ColumnSeparator
	if columnSeparator == "" {
		columnSeparator = string([]byte{0x01}) // \x01
	}

	lineDelimiter := request.LineDelimiter
	if lineDelimiter == "" {
		lineDelimiter = "\\n"
	} else {
		lineDelimiter = strings.ReplaceAll(lineDelimiter, "\n", "\\n")
		lineDelimiter = strings.ReplaceAll(lineDelimiter, "\r", "\\r")
		lineDelimiter = strings.ReplaceAll(lineDelimiter, "\r\n", "\\r\\n")
	}

	maxFileSize := conf.DorisConfig.S3ExportMaxFileSize
	if maxFileSize == "" {
		maxFileSize = "512MB"
	}
	requestTimeout := conf.DorisConfig.S3ExportRequestTimeout
	if requestTimeout == 0 {
		requestTimeout = 600000
	}
	connectionTimeout := conf.DorisConfig.S3ExportConnectionTimeout
	if connectionTimeout == 0 {
		connectionTimeout = 60000
	}
	connectionMaximum := conf.DorisConfig.S3ExportConnectionMaximum
	if connectionMaximum == 0 {
		connectionMaximum = 256
	}

	// S3 配置
	s3Props := []string{
		fmt.Sprintf(`"s3.endpoint" = "http://%s:%d"`, conf.OSSConfig.Host, conf.OSSConfig.Port),
		`"s3.region" = "us-east-1"`,
		fmt.Sprintf(`"s3.secret_key" = "%s"`, conf.OSSConfig.SecretKey),
		fmt.Sprintf(`"s3.access_key" = "%s"`, conf.OSSConfig.AccessKey),
		`"use_path_style" = "true"`,
		`"max_file_size" = "200MB"`,
		// `"s3.connection.request.timeout" = "600000"`,
		// `"s3.connection.timeout" = "60000"`,
		// `"s3.connection.maximum" = "256"`,
		// fmt.Sprintf(`"column_separator" = "%s"`, columnSeparator), // 添加分隔符
		// fmt.Sprintf(`"line_delimiter" = "%s"`, lineDelimiter),     // 添加换行符
	}

	// 组装 SELECT ... INTO OUTFILE
	sql := fmt.Sprintf(`
		SELECT %s FROM %s.%s%s%s
		INTO OUTFILE "%s"
		FORMAT AS parquet
		PROPERTIES (
			%s
		)
	`,
		columnsClause,
		request.DbName, request.TableName,
		whereClause, orderClause,
		targetPath,
		strings.Join(s3Props, ",\n\t\t\t"),
	)
	return sql
}

// 组装单个过滤条件
func buildSingleCondition(fc *pb.FilterCondition) string {
	field := fc.FieldName
	switch fc.Operator {
	case pb.FilterOperator_EQUAL:
		return fmt.Sprintf(`%s = %s`, field, formatFilterValue(fc.FieldValue))
	case pb.FilterOperator_NOT_EQUAL:
		return fmt.Sprintf(`%s <> %s`, field, formatFilterValue(fc.FieldValue))
	case pb.FilterOperator_GREATER_THAN:
		return fmt.Sprintf(`%s > %s`, field, formatFilterValue(fc.FieldValue))
	case pb.FilterOperator_LESS_THAN:
		return fmt.Sprintf(`%s < %s`, field, formatFilterValue(fc.FieldValue))
	case pb.FilterOperator_GREATER_THAN_OR_EQUAL:
		return fmt.Sprintf(`%s >= %s`, field, formatFilterValue(fc.FieldValue))
	case pb.FilterOperator_LESS_THAN_OR_EQUAL:
		return fmt.Sprintf(`%s <= %s`, field, formatFilterValue(fc.FieldValue))
	case pb.FilterOperator_LIKE_OPERATOR:
		return fmt.Sprintf(`%s LIKE %s`, field, formatFilterValue(fc.FieldValue))
	case pb.FilterOperator_IN_OPERATOR:
		return fmt.Sprintf(`%s IN (%s)`, field, formatInList(fc.FieldValue))
	default:
		return "1=1"
	}
}

// 将 FilterValue 转为 SQL 字面量
func formatFilterValue(v *pb.FilterValue) string {
	if v == nil {
		return "NULL"
	}
	if v.StrValue != "" {
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v.StrValue, "'", "''"))
	}
	if len(v.StrValues) > 0 || len(v.IntValues) > 0 || len(v.FloatValues) > 0 || len(v.BoolValues) > 0 {
		// 在 IN 子句中处理
		return fmt.Sprintf("(%s)", formatInList(v))
	}
	// 标量数值/布尔
	if v.BoolValue {
		return "true"
	}
	if v.FloatValue != 0 {
		return fmt.Sprintf("%v", v.FloatValue)
	}
	// int_value 为 0 时与缺省难区分，这里直接输出
	return fmt.Sprintf("%d", v.IntValue)
}

// IN 列表
func formatInList(v *pb.FilterValue) string {
	if v == nil {
		return ""
	}
	var items []string
	if len(v.StrValues) > 0 {
		for _, sv := range v.StrValues {
			items = append(items, fmt.Sprintf("'%s'", strings.ReplaceAll(sv, "'", "''")))
		}
		return strings.Join(items, ", ")
	}
	if len(v.IntValues) > 0 {
		for _, iv := range v.IntValues {
			items = append(items, fmt.Sprintf("%d", iv))
		}
		return strings.Join(items, ", ")
	}
	if len(v.FloatValues) > 0 {
		for _, fv := range v.FloatValues {
			items = append(items, fmt.Sprintf("%v", fv))
		}
		return strings.Join(items, ", ")
	}
	if len(v.BoolValues) > 0 {
		for _, bv := range v.BoolValues {
			if bv {
				items = append(items, "true")
			} else {
				items = append(items, "false")
			}
		}
		return strings.Join(items, ", ")
	}
	return ""
}
