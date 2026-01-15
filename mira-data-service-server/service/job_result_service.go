package service

import (
	"bytes"
	"context"
	"data-service/clients"
	"data-service/common"
	"data-service/config"
	"data-service/database"
	pb "data-service/generated/datasource"
	log "data-service/log"
	"data-service/oss"
	"data-service/utils"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow/go/v15/arrow"

	"chainweaver.org.cn/chainweaver/mira/mira-common/types"
	mirapb "chainweaver.org.cn/chainweaver/mira/mira-ida-access-service/pb/mirapb"
	"github.com/jinzhu/copier"
)

type JobResultService interface {
	PushJobResultToExternalDB(ctx context.Context, req *pb.PushJobResultRequest) (*pb.PushJobResultResponse, error)
}

// DefaultJobResultService 是 JobResultService 的默认实现
type DefaultJobResultService struct {
	// 可以添加依赖，如 logger、db 等
}

// fileReadCloser 自定义 ReadCloser，在关闭时删除临时文件
type fileReadCloser struct {
	*os.File
	filePath string
}

func (f *fileReadCloser) Close() error {
	// 先关闭文件
	if err := f.File.Close(); err != nil {
		log.Logger.Errorf("Failed to close file %s: %v", f.filePath, err)
		return err
	}
	// 然后删除临时文件
	if err := os.Remove(f.filePath); err != nil {
		log.Logger.Warnf("Failed to remove temporary file %s: %v", f.filePath, err)
	}
	return nil
}

// NewJobResultService 创建 JobResultService 实例
func NewJobResultService() JobResultService {
	return &DefaultJobResultService{}
}

// PushJobResultToExternalDB 实现
func (s *DefaultJobResultService) PushJobResultToExternalDB(ctx context.Context, req *pb.PushJobResultRequest) (*pb.PushJobResultResponse, error) {
	// 给mira-gateway发送计算结果推送的请求
	err := clients.GetMiraGateway().PushJobResult(req.ChainInfoId, req.JobInstanceId, req.PartyId, req.DataId)
	if err != nil {
		log.Logger.Errorf("Failed to send job result request to gateway: %v", err)
		return nil, err
	}

	// // 清除doris中间表
	// dorisService, err := NewDorisService()
	// if err != nil {
	// 	log.Logger.Errorf("Failed to create doris service: %v", err)
	// 	return nil, err
	// }
	// err = dorisService.cleanDorisTableWithPrefix(req.JobInstanceId)
	// if err != nil {
	// 	log.Logger.Errorf("Failed to clean doris table: %v", err)
	// 	return nil, err
	// }

	// 查询外部存储配置
	storageInfo, err := clients.GetMiraGateway().GetResultStorageConfig(&mirapb.GetResultStorageConfigRequest{
		JobInstanceId: req.JobInstanceId,
		ParticipantId: req.PartyId,
	})
	if err != nil {
		log.Logger.Errorf("Failed to get result storage config: %v", err)
		return nil, err
	}

	log.Logger.Infof("Storage config for job %s: Type=%d, Host=%s:%d, DB=%s, User=%s",
		req.JobInstanceId,
		storageInfo.GetResultStorageConfig().GetResultStorageType(),
		storageInfo.GetResultStorageConfig().GetHost(),
		storageInfo.GetResultStorageConfig().GetPort(),
		storageInfo.GetResultStorageConfig().GetDb(),
		storageInfo.GetResultStorageConfig().GetUser())

	if storageInfo.GetResultStorageConfig().GetResultStorageType() == int32(pb.DataSourceType_DATA_SOURCE_TYPE_UNKNOWN) {
		log.Logger.Infof("存储到MINIO，跳过数据同步，直接返回成功, jobInstanceId: %s", req.JobInstanceId)
		return &pb.PushJobResultResponse{
			Success: true,
			Message: "success",
		}, nil
	}

	var tlsConfig *pb.DatasourceTlsConfig
	if storageInfo.GetResultStorageConfig().GetTlsConfig() != nil {
		tlsConfig = &pb.DatasourceTlsConfig{
			UseTls:     storageInfo.GetResultStorageConfig().GetTlsConfig().GetUseTls(),
			Mode:       storageInfo.GetResultStorageConfig().GetTlsConfig().GetMode(),
			ServerName: storageInfo.GetResultStorageConfig().GetTlsConfig().GetServerName(),
			CaCert:     storageInfo.GetResultStorageConfig().GetTlsConfig().GetCaCert(),
			ClientCert: storageInfo.GetResultStorageConfig().GetTlsConfig().GetClientCert(),
			ClientKey:  storageInfo.GetResultStorageConfig().GetTlsConfig().GetClientKey(),
		}
	}

	// 计算结果存储类型 0:minio 1:mysql 2:TIDB, 3:TDSQL, 4:Kingbase 5:VastBase 6: GBase
	err = syncResultToDB(storageInfo.GetResultStorageConfig().GetResultStorageType(), &types.CalculationResultStorage{
		Host:     storageInfo.GetResultStorageConfig().GetHost(),
		Port:     int(storageInfo.GetResultStorageConfig().GetPort()),
		User:     storageInfo.GetResultStorageConfig().GetUser(),
		Password: storageInfo.GetResultStorageConfig().GetPassword(),
		DB:       storageInfo.GetResultStorageConfig().GetDb(),
	}, req, tlsConfig)
	if err != nil {
		log.Logger.Errorf("syncResultToDB Failed, err: %v", err)
		return nil, err
	}

	return &pb.PushJobResultResponse{
		Success: true,
		Message: "success",
	}, nil
}

func syncResultToDB(dbType int32, storageInfo *types.CalculationResultStorage, req *pb.PushJobResultRequest, tlsConfig *pb.DatasourceTlsConfig) error {
	// 1.获取CSV文件流
	dataId := strings.TrimPrefix(req.DataId, "tee_")
	objectName := fmt.Sprintf("%s/%s/%s/%s", req.ChainInfoId, req.JobInstanceId, req.PartyId, dataId)
	log.Logger.Infof("getResultContent | objectName: %s", objectName)

	// 使用流式读取
	csvReader, err := getResultContentReader(req.IsEncrypted, req.PubKey, objectName)
	if err != nil {
		log.Logger.Errorf("getResultContentReader Failed, err: %v", err)
		return err
	}
	// defer csvReader.Close()

	// 建立数据库连接
	connInfo := &pb.ConnectionInfo{
		Host:      storageInfo.Host,
		Port:      int32(storageInfo.Port),
		User:      storageInfo.User,
		Password:  storageInfo.Password,
		DbName:    storageInfo.DB,
		TlsConfig: tlsConfig,
	}
	dbTypeName := utils.GetDbTypeNameFromBackend(int32(dbType))
	DBType := utils.ConvertDBType(dbTypeName)
	dbStrategy, err := database.DatabaseFactory(DBType, connInfo)
	if err != nil {
		log.Logger.Errorf("DatabaseFactory Failed, err: %v", err)
		return err
	}

	if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
		log.Logger.Errorf("Failed to connect to database: %v", err)
		return err
	}

	db := database.GetDB(dbStrategy)
	tableName := req.JobInstanceId

	// 流式处理CSV数据
	var headers []string
	isFirstBatch := true

	err = processCSVStreaming(csvReader, func(records [][]string) error {
		if len(records) == 0 {
			return nil
		}

		// 处理表头（仅第一批）
		if isFirstBatch {
			if len(records[0]) == 0 {
				return errors.New("records header is empty")
			}

			headers = records[0]
			var originHeaders []string
			err := copier.Copy(&originHeaders, &headers)
			if err != nil {
				return fmt.Errorf("copy headers failed: %v", err)
			}

			// 创建表结构
			columns, _ := generateStructModel(originHeaders, headers)
			schema := convertColumnsToArrowSchema(columns)
			err = utils.WithRetry(3, 200*time.Millisecond, func() error {
				return dbStrategy.CreateTemporaryTableIfNotExists(tableName, schema)
			}, utils.IsRetryableNetErr)
			if err != nil {
				return fmt.Errorf("CreateTemporaryTableIfNotExists Failed after retry, err: %v", err)
			}

			isFirstBatch = false

			// 跳过表头，处理数据行
			if len(records) > 1 {
				dataRows := records[1:]
				return batchInsertData(db, tableName, headers, dataRows, dbType)
			}

			return nil
		}

		// 批量插入数据
		return batchInsertData(db, tableName, headers, records, dbType)
	})

	if err != nil {
		log.Logger.Errorf("processCSVStreaming Failed, err: %v", err)
		return err
	}

	log.Logger.Infof("syncResultToDB | success, instanceId: %s", req.JobInstanceId)
	return nil
}

// TransformSoftwareFormat 将多个JSON字符串合并并转换格式
// 输入格式: 详见https://www.tapd.cn/51081496/markdown_wikis/show/#1151081496001001856
// 输出格式: [{"column1":1,"column2":2},{"column1":1,"column2":2}]
func TransformSoftwareFormat(allResults []string) (string, error) {
	r, columnsIdx, err := dealRows(allResults)
	if err != nil {
		return "", fmt.Errorf("TransformSoftwareFormat | deal row failed: %v", err)
	}

	result, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("TransformSoftwareFormat | marshal result failed: %v", err)
	}

	// 解析所有输入的JSON字符串
	var columnMap = make(map[string][]interface{})
	maxLen := 0
	// 兼容空结果
	if checkResultIsEmpty(string(result)) {
		return "", fmt.Errorf("TransformSoftwareFormat | result is empty")
	}

	var resultMap map[string][]interface{}
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		return "", fmt.Errorf("TransformSoftwareFormat | unmarshal input JSON failed: %v", err)
	}

	// 合并所有列数据
	for col, values := range resultMap {
		columnMap[col] = values
		if len(values) > maxLen {
			maxLen = len(values)
		}
	}

	// 兼容空结果
	if len(columnMap) == 0 {
		return "", nil
	}

	// 构建转换后的结果
	// [{"column1":1,"column2":2},{"column1":1,"column2":2}]
	var transformedResults []string
	for i := 0; i < maxLen; i++ {
		row := "{"
		// 按排序后的列名顺序构建 row
		for _, col := range columnsIdx {
			values := columnMap[col]
			if i < len(values) {
				value, _ := json.Marshal(values[i])
				row += fmt.Sprintf("\"%s\":%s,", col, string(value))
			}
		}
		row = strings.TrimSuffix(row, ",")
		row += "}"
		transformedResults = append(transformedResults, row)
	}

	// 将结果转换回JSON字符串
	return "[" + strings.Join(transformedResults, ",") + "]", nil
}

func TransformHardwareFormat(allResults []string) (string, error) {
	var finalResult []map[string]interface{}

	for _, result := range allResults {
		if result == "" {
			continue
		}

		var inputMap map[string]interface{}
		if err := json.Unmarshal([]byte(result), &inputMap); err != nil {
			// 如果不是对象，尝试按原来的方式解析为数组
			var arrayResult []map[string]interface{}
			if err := json.Unmarshal([]byte(result), &arrayResult); err != nil {
				return "", fmt.Errorf("TransformHardware | unmarshal input JSON failed: %v", err)
			}
			finalResult = append(finalResult, arrayResult...)
			continue
		}

		// 检测是否为列式数据（所有值都是数组）
		isColumnFormat := true
		columnLengths := make(map[string]int)

		for key, value := range inputMap {
			if arrayValue, isArray := value.([]interface{}); isArray {
				columnLengths[key] = len(arrayValue)
			} else {
				isColumnFormat = false
				break
			}
		}

		// 如果是列式数据，转换为行式数据
		if isColumnFormat && len(columnLengths) > 0 {
			// 找出最大长度
			maxLength := 0
			for _, length := range columnLengths {
				if length > maxLength {
					maxLength = length
				}
			}

			// 创建行式数据
			for i := 0; i < maxLength; i++ {
				row := make(map[string]interface{})

				for key, value := range inputMap {
					if arrayValue, isArray := value.([]interface{}); isArray && i < len(arrayValue) {
						row[key] = arrayValue[i]
					}
				}

				if len(row) > 0 {
					finalResult = append(finalResult, row)
				}
			}
		} else {
			// 原有的处理逻辑
			for key, value := range inputMap {
				// 支持直接数组
				if arrayValue, isArray := value.([]interface{}); isArray {
					for _, item := range arrayValue {
						switch v := item.(type) {
						case map[string]interface{}:
							finalResult = append(finalResult, map[string]interface{}{key: v})
						default:
							finalResult = append(finalResult, map[string]interface{}{key: v})
						}
					}
					continue
				}
				// 兼容旧格式：字符串形式的JSON数组
				if strValue, ok := value.(string); ok && strings.HasPrefix(strValue, "[") && strings.HasSuffix(strValue, "]") {
					var innerArray []interface{}
					if err := json.Unmarshal([]byte(strValue), &innerArray); err != nil {
						continue
					}
					for _, innerObj := range innerArray {
						finalResult = append(finalResult, map[string]interface{}{key: innerObj})
					}
				}
			}
		}
	}

	if len(finalResult) == 0 {
		return "[]", nil
	}
	resultJSON, err := json.Marshal(finalResult)
	if err != nil {
		return "", fmt.Errorf("TransformHardware | marshal output JSON failed: %v", err)
	}
	return string(resultJSON), nil
}

func getResultContent(isEncrypted bool, pubKey string, objectName string) (string, error) {
	factory := oss.NewOSSFactory(config.GetConfigMap())
	client, err := factory.NewOSSClient()
	if err != nil {
		return "", fmt.Errorf("failed to create OSS client: %v", err)
	}
	log.Logger.Infof("getResultContent | objectName: %s", objectName)
	// GetObject 返回 io.ReadCloser
	reader, err := client.GetObject(context.Background(), common.RESULT_BUCKET_NAME, objectName, &oss.GetOptions{})

	if err != nil {
		return "", fmt.Errorf("failed to get object: %v", err)
	}
	defer reader.Close()

	// 读取内容
	content, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("failed to read object content: %v", err)
	}

	if len(content) == 0 {
		return "", errors.New("getResultContent | object content is empty, objectName: " + objectName)
	}

	if isEncrypted {
		plainText, err := decryptContent(string(content), pubKey)
		if err != nil {
			return "", err
		}
		return plainText, nil
	}
	return string(content), nil

}

// 解密内容
func decryptContent(content string, key string) (string, error) {
	resp, err := utils.GetIDAService().Client.Decrypt(context.Background(), &mirapb.KeyDecryptRequest{
		CipherText: content,
		AlgoType:   mirapb.AlgoType_SM2,
		PubKey:     key,
	})
	if err != nil {
		return "", fmt.Errorf("decryptContent | decrypt failed: %v", err)
	}

	return resp.GetPlainText(), nil
}

// 打开 CSV 文件并读取内容
func readContentFromCSV(content []byte) ([][]string, error) {
	reader := csv.NewReader(bytes.NewReader(content))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV file: %w", err)
	}
	return records, nil
}

// 动态生成结构体字段
// originHeaders 用于字段comment注释
func generateStructModel(originHeaders, headers []string) ([]string, error) {
	var structFields []reflect.StructField
	columns := make([]string, 0, len(headers))

	// 添加主键字段
	structField := reflect.StructField{
		Name: "AutoID",                // 字段名称
		Type: reflect.TypeOf(uint(0)), // 默认字符串类型
		Tag:  reflect.StructTag(fmt.Sprintf(`gorm:"column:_auto_id;primarykey;comment:%s"`, "主键ID")),
	}
	structFields = append(structFields, structField)

	// 根据表头动态推断字段类型
	for i, header := range headers {
		// 获取字段名称
		columnName := header
		// 1. 判断是否有as分割符 存在则取分割后的字字段名称
		// 使用正则表达式匹配不区分大小写的 'as'
		re := regexp.MustCompile(`(?i)\sas\s`)
		// 按照正则表达式分割字符串
		parts := re.Split(header, -1)
		// 获取最后一个元素
		columnName = strings.TrimSpace(parts[len(parts)-1])

		// 2. 判断字段长度是否超过64位,超过则使用自定义field字段别名
		if len(columnName) > 64 {
			columnName = fmt.Sprintf("field%d", i)
		}
		structField := reflect.StructField{
			Name: fmt.Sprintf("Field%d", i), // 字段名称
			Type: reflect.TypeOf(""),        // 默认字符串类型
			Tag:  reflect.StructTag(fmt.Sprintf(`gorm:"column:%s;comment:%s"`, columnName, originHeaders[i])),
		}
		structFields = append(structFields, structField)
		columns = append(columns, columnName)
	}

	return columns, nil
}

// 将columns转换为Arrow Schema
func convertColumnsToArrowSchema(columns []string) *arrow.Schema {
	fields := make([]arrow.Field, len(columns))

	for i, columnName := range columns {
		log.Logger.Infof("convertColumnsToArrowSchema | columnName: %s", columnName)
		fields[i] = arrow.Field{
			Name: columnName,
			Type: arrow.BinaryTypes.LargeString, // 所有字段类型都设为string
		}
	}

	return arrow.NewSchema(fields, nil)
}

// 批量插入数据
func batchInsertData(db *sql.DB, tableName string, headers []string, dataRows [][]string, dbType int32) error {
	batchSize := 1000

	for i := 0; i < len(dataRows); i += batchSize {
		end := i + batchSize
		if end > len(dataRows) {
			end = len(dataRows)
		}

		batch := dataRows[i:end]

		// 生成INSERT SQL
		insertSQL, args := generateInsertSQL(tableName, headers, batch, dbType)

		// 执行插入
		_, err := db.Exec(insertSQL, args...)
		if err != nil {
			return fmt.Errorf("failed to insert batch %d-%d: %v", i, end-1, err)
		}

		log.Logger.Debugf("Inserted batch %d-%d (%d records)", i, end-1, len(batch))
	}

	return nil
}

// 生成INSERT SQL语句
func generateInsertSQL(tableName string, headers []string, dataRows [][]string, dbType int32) (string, []interface{}) {
	var placeholders []string
	var args []interface{}
	paramIndex := 1 // 用于KingBase的参数索引

	for _, row := range dataRows {
		var rowPlaceholders []string
		for _, value := range row {
			// 根据数据库类型使用不同的占位符
			if dbType == 4 || dbType == 5 { // KingBase, VastBase
				rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", paramIndex))
				paramIndex++
			} else { // MySQL, TiDB, TDSQL等
				rowPlaceholders = append(rowPlaceholders, "?")
			}
			args = append(args, value)
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	// 为字段名添加反引号，如果字段名已经有反引号则不重复添加
	quotedHeaders := make([]string, len(headers))
	for i, header := range headers {
		log.Logger.Debugf("generateInsertSQL | Original field name: %s", header)

		if dbType == 4 || dbType == 5 { // KingBase - 使用双引号
			if strings.HasPrefix(header, "\"") && strings.HasSuffix(header, "\"") {
				// 字段名已经有双引号，直接使用
				quotedHeaders[i] = header
			} else {
				// 字段名没有双引号，添加双引号
				quotedHeaders[i] = fmt.Sprintf("\"%s\"", header)
			}
		} else { // MySQL, TiDB, TDSQL - 使用反引号
			if strings.HasPrefix(header, "`") && strings.HasSuffix(header, "`") {
				// 字段名已经有反引号，直接使用
				quotedHeaders[i] = header
			} else {
				// 字段名没有反引号，添加反引号
				quotedHeaders[i] = fmt.Sprintf("`%s`", header)
			}
		}
	}

	columns := strings.Join(quotedHeaders, ", ")
	values := strings.Join(placeholders, ", ")

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columns, values)

	return insertSQL, args
}

func ProcessFileFormat(result string) (string, error) {
	// 兼容空结果
	if strings.TrimSpace(result) == "" {
		return encodeContent("[]"), nil
	}

	var csvContent string
	var err error
	csvContent, err = utils.HardwareJsonToCSV([]byte(result))
	// if isHardware {
	// 	csvContent, err = utils.HardwareJsonToCSV([]byte(result))
	// } else {
	// 	csvContent, err = utils.JsonToCSV([]byte(result))
	// }
	if err != nil {
		return "", fmt.Errorf("processResultFormat | transform to csv failed. err: %v", err)
	}
	return encodeContent(string(csvContent)), nil
}

func encodeContent(content string) string {
	return base64.StdEncoding.EncodeToString([]byte(content))
}

// 2025.02.24 变更：软件执行引擎将参与方信息、表信息写入minIO, 解析后展示给用户以区分同名属性
//
//	输入：{"id": {
//		"value": [1, 2, 3, 4],
//		"party_id": "party_id_1",
//		"asset_name": "asset_name_1"
//		}], "id2"...}
//
// 输出：{"party_id_1.asset_name_1.id":[1, 2, 3, 4], "party_id_2.asset_name_2.id2":[1, 2, 3, 4]}
func dealRows(rows []string) (map[string][]interface{}, []string, error) {
	type rowInfo struct {
		Value      []interface{} `json:"value"`
		PartyId    string        `json:"party_id"`
		AssetName  string        `json:"asset_name"`
		PartyName  string        `json:"party_name"`
		ItemIdx    int           `json:"item_idx"`
		ColumnName string        `json:"column"`
	}
	res := make(map[string][]interface{})
	indexArray := make([]rowInfo, 0, len(rows))
	for _, row := range rows {
		rowInfoMap := make(map[string]rowInfo)
		err := json.Unmarshal([]byte(row), &rowInfoMap)
		if err != nil {
			return nil, nil, fmt.Errorf("error unmarshalling err: %v, row: %s", err, row)
		}

		for k, v := range rowInfoMap {
			if v.PartyName == "" || v.AssetName == "" {
				res[k] = v.Value
				indexArray = append(indexArray, rowInfo{ColumnName: k, ItemIdx: v.ItemIdx})
			} else {
				res[v.PartyName+"."+v.AssetName+"."+k] = v.Value
				indexArray = append(indexArray, rowInfo{
					ColumnName: v.PartyName + "." + v.AssetName + "." + k,
					ItemIdx:    v.ItemIdx,
				})
			}
		}
	}

	sort.Slice(indexArray, func(i, j int) bool {
		return indexArray[i].ItemIdx < indexArray[j].ItemIdx
	})

	columns := make([]string, 0, len(indexArray))
	for _, v := range indexArray {
		columns = append(columns, v.ColumnName)
	}

	return res, columns, nil
}

// 结果为空时也应该有表头之类的数据， 但各个执行引擎实现可能不一致， 这里对空结果做兼容
func checkResultIsEmpty(result string) bool {
	return strings.TrimSpace(result) == "" ||
		strings.TrimSpace(result) == "[]" ||
		strings.TrimSpace(result) == "{}"
}

func getResultContentReader(isEncrypted bool, pubKey string, objectName string) (io.ReadCloser, error) {
	factory := oss.NewOSSFactory(config.GetConfigMap())
	client, err := factory.NewOSSClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create OSS client: %v", err)
	}
	log.Logger.Infof("getResultContent | objectName: %s", objectName)

	// 生成本地文件路径
	fileName := fmt.Sprintf("result_%s_%d", filepath.Base(objectName), time.Now().Unix())
	localFilePath := filepath.Join(common.DATA_DIR, fileName)

	// 确保目录存在
	if err := os.MkdirAll(common.DATA_DIR, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	// 从 OSS 获取对象并下载到本地，使用重试机制处理网络断连
	const (
		maxRetries = 3
		baseDelay  = 500 * time.Millisecond
		maxDelay   = 5 * time.Second
	)

	downloadErr := utils.WithRetryCtx(
		context.Background(),
		maxRetries,
		baseDelay,
		maxDelay,
		func() error {
			// 从 OSS 获取对象
			reader, err := client.GetObject(context.Background(), common.RESULT_BUCKET_NAME, objectName, &oss.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get object: %v", err)
			}
			defer reader.Close()

			// 创建本地文件
			file, err := os.Create(localFilePath)
			if err != nil {
				return fmt.Errorf("failed to create local file: %v", err)
			}

			// 下载文件到本地
			_, err = io.Copy(file, reader)
			if err != nil {
				file.Close()
				os.Remove(localFilePath)
				return fmt.Errorf("failed to download object to local file: %v", err)
			}

			// 关闭写入的文件
			if err := file.Close(); err != nil {
				os.Remove(localFilePath)
				return fmt.Errorf("failed to close file after writing: %v", err)
			}

			log.Logger.Infof("Successfully downloaded OSS object to local path: %s", localFilePath)
			return nil
		},
		utils.IsRetryableNetErr,
	)

	if downloadErr != nil {
		return nil, fmt.Errorf("failed to download after retries: %v", downloadErr)
	}

	if isEncrypted {
		// 对于加密文件，读取并解密
		content, err := os.ReadFile(localFilePath)
		if err != nil {
			os.Remove(localFilePath)
			return nil, fmt.Errorf("failed to read encrypted content: %v", err)
		}

		// 解密完成后立即删除本地文件
		os.Remove(localFilePath)

		plainText, err := decryptContent(string(content), pubKey)
		if err != nil {
			return nil, err
		}

		// 返回解密后内容的Reader
		return io.NopCloser(strings.NewReader(plainText)), nil
	}

	// 非加密文件：重新打开文件用于流式读取
	localFile, err := os.Open(localFilePath)
	if err != nil {
		os.Remove(localFilePath)
		return nil, fmt.Errorf("failed to open local file for reading: %v", err)
	}

	// 返回自定义的 ReadCloser，关闭时自动删除临时文件
	return &fileReadCloser{
		File:     localFile,
		filePath: localFilePath,
	}, nil
}

func processCSVStreaming(reader io.ReadCloser, processor func([][]string) error) error {
	defer reader.Close()

	csvReader := csv.NewReader(reader)
	conf := config.GetConfigMap()
	batchSize := conf.StreamConfig.BatchLines

	var batch [][]string
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			// 处理最后一批数据
			if len(batch) > 0 {
				if err := processor(batch); err != nil {
					return err
				}
			}
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record: %v", err)
		}

		batch = append(batch, record)

		// 当批次达到指定大小时处理
		if len(batch) >= batchSize {
			if err := processor(batch); err != nil {
				return err
			}
			// 清空批次，释放内存
			batch = batch[:0] // 重用底层数组
		}
	}
	return nil
}
