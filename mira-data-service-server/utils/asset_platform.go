/*
*

	@author: shiliang
	@date: 2024/9/11
	@note: 获取数据资产相关信息

*
*/
package utils

import (
	"bytes"
	"data-service/clients"
	pb2 "data-service/generated/datasource"
	"data-service/log"
	"fmt"
	"strconv"
	"strings"

	pb "chainweaver.org.cn/chainweaver/mira/mira-ida-access-service/pb/mirapb"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func GetDatasourceByAssetName(requestId string, assetName string, chainId string, alias string) (*pb2.ConnectionInfo, error) {
	request := &pb.GetPrivateAssetInfoByEnNameRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId:   requestId,
			ChainInfoId: chainId,
			Alias:       alias,
		},
		AssetEnName: assetName,
	}
	log.Logger.Infof("Requesting asset info: %+v", request)
	response, err := clients.GetMiraGateway().GetPrivateAssetInfoByEnName(request)
	if err != nil {
		log.Logger.Errorf("Failed to get asset info: %v", err)
		return nil, err
	}

	if response == nil || response.GetData() == nil {
		log.Logger.Errorf("Asset info response is empty")
		return nil, fmt.Errorf("response data is nil")
	}
	dataInfo := response.GetData().DataInfo
	log.Logger.Infof("Got asset info: %+v", dataInfo)

	// Get database connection info
	connRequest := &pb.GetPrivateDBConnInfoRequest{
		RequestId: requestId,
		DbConnId:  dataInfo.DataSourceId,
	}
	log.Logger.Infof("Requesting database connection info: %+v", connRequest)
	connResponse, err := clients.GetMiraGateway().GetPrivateDBConnInfo(connRequest)
	if err != nil {
		log.Logger.Errorf("Failed to get database connection info: %v", err)
		return nil, err
	}

	if connResponse == nil || connResponse.GetData() == nil {
		return nil, fmt.Errorf("connection response data is nil")
	}

	// Map SaveTableColumnItem to ColumnItem
	var columns []*pb2.ColumnItem
	for _, item := range dataInfo.ItemList {
		column := &pb2.ColumnItem{
			Name:     item.GetName(),     // Map name
			DataType: item.GetDataType(), // Map data_type
		}
		columns = append(columns, column)
	}

	var tlsConfig *pb2.DatasourceTlsConfig
	if connResponse.GetData().TlsConfig != nil {
		tlsConfig = &pb2.DatasourceTlsConfig{
			UseTls:     connResponse.GetData().TlsConfig.UseTls,
			Mode:       connResponse.GetData().TlsConfig.Mode,
			ServerName: connResponse.GetData().TlsConfig.ServerName,
			CaCert:     connResponse.GetData().TlsConfig.CaCert,
			ClientCert: connResponse.GetData().TlsConfig.ClientCert,
			ClientKey:  connResponse.GetData().TlsConfig.ClientKey,
		}
	}
	connInfo := &pb2.ConnectionInfo{
		Host:      connResponse.GetData().Host,
		Port:      connResponse.GetData().Port,
		User:      connResponse.GetData().Username,
		DbName:    connResponse.GetData().DbName,
		Password:  connResponse.GetData().Password,
		Dbtype:    connResponse.GetData().Type,
		Columns:   columns,
		TableName: dataInfo.TableName,
		TlsConfig: tlsConfig,
	}
	return connInfo, nil
}

// 转换数据源类型
func ConvertDataSourceType(dbType int32) pb2.DataSourceType {
	switch dbType {
	case 1:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL
	case 2:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE
	case 4:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_TIDB
	case 5:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_TDSQL
	case 6:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_VASTBASE
	case 7:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_GBASE
	case 8:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_DORIS
	default:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_UNKNOWN
	}
}

func ConvertDBType(dbType string) pb2.DataSourceType {
	switch dbType {
	case "mysql":
		return pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL
	case "kingbase_pgsql":
		return pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE
	case "tdsql":
		return pb2.DataSourceType_DATA_SOURCE_TYPE_TDSQL
	case "tidb":
		return pb2.DataSourceType_DATA_SOURCE_TYPE_TIDB
	case "vastbase":
		return pb2.DataSourceType_DATA_SOURCE_TYPE_VASTBASE
	case "gbase":
		return pb2.DataSourceType_DATA_SOURCE_TYPE_GBASE
	default:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_UNKNOWN
	}
}

func GetDbTypeName(dbType int32) string {
	switch dbType {
	case 1:
		return "mysql"
	case 2:
		return "kingbase_pgsql"
	case 4:
		return "tidb"
	case 5:
		return "tdsql"
	case 6:
		return "vastbase"
	case 7:
		return "gbase"
	case 8:
		return "doris"
	default:
		return "unknown"
	}
}

func GetDbTypeNameFromBackend(dbType int32) string {
	switch dbType {
	case 1:
		return "mysql"
	case 2:
		return "tidb"
	case 3:
		return "tdsql"
	case 4:
		return "kingbase_pgsql"
	case 5:
		return "vastbase"
	case 6:
		return "gbase"
	default:
		return "unknown"
	}
}

func GetDbTypeFromName(dbTypeName string) int32 {
	switch dbTypeName {
	case "mysql":
		return 1
	case "kingbase_pgsql":
		return 2
	case "tidb":
		return 4
	case "tdsql":
		return 5
	case "vastbase":
		return 6
	case "gbase":
		return 7
	case "doris":
		return 8
	default:
		return 0
	}
}

func BuildSelectQuery(query string, args []interface{}, dbType pb2.DataSourceType) (string, error) {
	var sb strings.Builder
	argIndex := 0

	for i := 0; i < len(query); i++ {
		if query[i] == '?' { // 处理 MySQL 的占位符
			if argIndex >= len(args) {
				return "", fmt.Errorf("insufficient arguments for query")
			}
			arg, err := formatArgument(args[argIndex])
			if err != nil {
				return "", err
			}
			sb.WriteString(arg)
			argIndex++
		} else if dbType == pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE && query[i] == '$' && i+1 < len(query) && isDigit(query[i+1]) {
			// 处理 Kingbase 的占位符 $1, $2
			if argIndex >= len(args) {
				return "", fmt.Errorf("insufficient arguments for query")
			}
			arg, err := formatArgument(args[argIndex])
			if err != nil {
				return "", err
			}
			sb.WriteString(arg)
			argIndex++

			// 跳过占位符后面的数字
			for i+1 < len(query) && isDigit(query[i+1]) {
				i++
			}
		} else {
			sb.WriteByte(query[i])
		}
	}

	// 检查是否有多余参数
	if argIndex < len(args) {
		return "", fmt.Errorf("too many arguments for query")
	}

	return sb.String(), nil
}

// 格式化参数并转义
func formatArgument(arg interface{}) (string, error) {
	switch v := arg.(type) {
	case string:
		return fmt.Sprintf("'%s'", v), nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return fmt.Sprintf("%t", v), nil
	default:
		return "", fmt.Errorf("unsupported argument type: %T", arg)
	}
}

// 判断字符是否为数字
func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func BuildQuery(tableName string, fields []string, sourceType pb2.DataSourceType) string {
	// 检查是否需要加双引号（仅对表名）
	quoteTableName := func(identifier string) string {
		if sourceType == pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
			return fmt.Sprintf("\"%s\"", identifier)
		}
		return identifier
	}

	// 处理列名（不加双引号）
	var columns string
	if len(fields) > 0 {
		columns = strings.Join(fields, ", ")
	} else {
		columns = "*"
	}

	// 处理表名（仅对 Kingbase 加双引号）
	tableNameQuoted := quoteTableName(tableName)

	// 构建查询语句
	query := fmt.Sprintf("SELECT %s FROM %s", columns, tableNameQuoted)
	return query
}

func BuildQueryWithOrder(tableName string, fields []string, sortRules []*pb2.SortRule) string {
	var columns string
	if len(fields) > 0 {
		columns = strings.Join(fields, ", ")
	} else {
		columns = "*"
	}

	// 构建排序部分
	var orderClause string
	if len(sortRules) > 0 {
		orderClauses := make([]string, len(sortRules))
		for i, rule := range sortRules {
			// 获取排序方向的字符串表示
			var sortOrder string
			switch rule.GetSortOrder() {
			case pb2.SortOrder_ASC:
				sortOrder = "ASC"
			case pb2.SortOrder_DESC:
				sortOrder = "DESC"
			default:
				sortOrder = "ASC" // 默认为升序
			}
			orderClauses[i] = fmt.Sprintf("%s %s", rule.GetFieldName(), sortOrder)
		}
		orderClause = " ORDER BY " + strings.Join(orderClauses, ", ")
	}

	return fmt.Sprintf("SELECT %s FROM %s%s", columns, tableName, orderClause)
}

func BuildQueryWithCondition(tableName string, fields []string, filterNames []string, filterValues []string) string {
	// 构建列名
	columns := "*"
	if len(fields) > 0 {
		columns = fmt.Sprintf(`"%s"`, fields[0])
		for _, field := range fields[1:] {
			columns += fmt.Sprintf(`, "%s"`, field)
		}
	}

	// 构建 WHERE 条件
	var conditions []string
	if len(filterNames) > 0 && len(filterValues) > 0 {
		for i := 0; i < len(filterNames); i++ {
			conditions = append(conditions, fmt.Sprintf(`"%s" = '%s'`, filterNames[i], filterValues[i]))
		}
		whereClause := " WHERE " + strings.Join(conditions, " AND ")
		return fmt.Sprintf("SELECT %s FROM %s%s", columns, tableName, whereClause)
	}

	return fmt.Sprintf("SELECT %s FROM %s", columns, tableName)
}

// ConvertToEmptyArrowBatch 将 ColumnItem 转换为一个包含空数据和 schema 的 ArrowBatch
func ConvertToEmptyArrowBatch(columns []*pb2.ColumnItem) ([]byte, error) {
	// 创建 Arrow 字段列表
	var fields []arrow.Field
	for _, column := range columns {
		var fieldType arrow.DataType

		// 根据 ColumnItem 的数据类型设置正确的 Arrow 类型
		dataTypeStr := column.DataType
		log.Logger.Debugf("data type: %s", dataTypeStr)
		if dataTypeStr == "integer" || dataTypeStr == "int" || dataTypeStr == "unsigned int" || dataTypeStr == "uint" {
			fieldType = arrow.PrimitiveTypes.Int32
		} else if dataTypeStr == "bigint" {
			fieldType = arrow.PrimitiveTypes.Int64
		} else if dataTypeStr == "float" {
			fieldType = arrow.PrimitiveTypes.Float32
		} else if dataTypeStr == "double" {
			fieldType = arrow.PrimitiveTypes.Float64
		} else if dataTypeStr == "varchar" || dataTypeStr == "char" {
			fieldType = arrow.BinaryTypes.String
		} else if dataTypeStr == "text" {
			fieldType = arrow.BinaryTypes.String
		} else if dataTypeStr == "date" {
			fieldType = arrow.FixedWidthTypes.Date32
		} else if dataTypeStr == "decimal" || dataTypeStr == "numeric" {
			// 将 decimal 和 numeric 转换为 Float64
			fieldType = arrow.PrimitiveTypes.Float64
		} else if containsIgnoreCase(dataTypeStr, "timestamp") {
			// 使用毫秒精度的时间戳
			fieldType = &arrow.TimestampType{Unit: arrow.Millisecond}
		} else {
			return nil, fmt.Errorf("unsupported data type: %s", column.DataType)
		}

		fields = append(fields, arrow.Field{Name: column.Name, Type: fieldType})
	}

	// 创建 Arrow schema
	schema := arrow.NewSchema(fields, nil)

	// 使用默认分配器
	allocator := memory.DefaultAllocator

	// 创建一个空的 array.Record，字段数量与 schema 一致
	recordArrays := make([]arrow.Array, len(fields))

	for i, field := range fields {
		// 创建与字段类型相匹配的空 array
		var builder array.Builder
		switch field.Type.ID() {
		case arrow.INT32:
			builder = array.NewInt32Builder(allocator)
		case arrow.INT64:
			builder = array.NewInt64Builder(allocator)
		case arrow.FLOAT32:
			builder = array.NewFloat32Builder(allocator)
		case arrow.FLOAT64:
			builder = array.NewFloat64Builder(allocator)
		case arrow.STRING:
			builder = array.NewStringBuilder(allocator)
		case arrow.LARGE_STRING:
			builder = array.NewLargeStringBuilder(allocator)
		case arrow.DATE32:
			builder = array.NewDate32Builder(allocator)
		case arrow.TIMESTAMP:
			timestampType, ok := field.Type.(*arrow.TimestampType)
			if !ok {
				return nil, fmt.Errorf("invalid timestamp type for field: %s", field.Name)
			}
			builder = array.NewTimestampBuilder(allocator, timestampType)
		default:
			return nil, fmt.Errorf("unsupported field type: %s", field.Type)
		}
		defer builder.Release()

		// 创建并添加空数组
		arr := builder.NewArray()
		defer arr.Release()
		recordArrays[i] = arr
	}

	// 创建一个空的 Record（不包含数据，但包含 schema）
	record := array.NewRecord(schema, recordArrays, 0)
	defer record.Release()

	// 使用 IPC Writer 将 Arrow 表转换为字节流
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("failed to write empty Arrow Table: %v", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close Arrow IPC writer: %v", err)
	}

	// 返回 Arrow 字节流
	return buf.Bytes(), nil
}

// 辅助函数：忽略大小写检查子字符串
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

func OperationModeToString(m pb2.OperationMode) string {
	switch m {
	case pb2.OperationMode_OPERATION_MODE_QUERY:
		return "query"
	case pb2.OperationMode_OPERATION_MODE_WRITE:
		return "write"
	case pb2.OperationMode_OPERATION_MODE_SORT:
		return "sort"
	case pb2.OperationMode_OPERATION_MODE_COUNT:
		return "count"
	case pb2.OperationMode_OPERATION_MODE_GROUPBY_COUNT:
		return "groupby_count"
	case pb2.OperationMode_OPERATION_MODE_JOIN:
		return "join"
	case pb2.OperationMode_OPERATION_MODE_ADD_HASH_COLUMN:
		return "add_hash_column"
	case pb2.OperationMode_OPERATION_MODE_PSI_JOIN:
		return "psi_join"
	default:
		return "unknown"
	}
}

func StorageTypeToString(s pb2.StorageType) string {
	switch s {
	case pb2.StorageType_STORAGE_TYPE_DB:
		return "db"
	case pb2.StorageType_STORAGE_TYPE_MINIO:
		return "minio"
	default:
		return "unknown"
	}
}

func JoinTypeToString(jt pb2.JoinType) string {
	switch jt {
	case pb2.JoinType_JOIN_TYPE_INNER:
		return "inner"
	case pb2.JoinType_JOIN_TYPE_LEFT:
		return "left"
	case pb2.JoinType_JOIN_TYPE_RIGHT:
		return "right"
	case pb2.JoinType_JOIN_TYPE_OUTER:
		return "outer"
	default:
		return "inner" // 默认值
	}
}
