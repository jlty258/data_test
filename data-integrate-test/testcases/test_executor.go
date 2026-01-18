package testcases

import (
	"context"
	"data-integrate-test/clients"
	"data-integrate-test/generators"
	"data-integrate-test/isolation"
	"data-integrate-test/strategies"
	"data-integrate-test/utils"
	"data-integrate-test/validators"
	"fmt"
	"time"
)

// TestExecutor 测试执行器
type TestExecutor struct {
	template     *TestTemplate
	idaClient    *clients.IDAServiceClient
	dataClient   *clients.DataServiceClient
	strategy     strategies.DatabaseStrategy
	namespaceMgr *isolation.NamespaceManager
	Namespace    string // 导出以便外部访问
	namespace    string
	tableName    string
	assetName    string
	dataSourceId int32
	assetId      int32
	schema       *generators.SchemaDefinition // 保存生成的schema
}

func NewTestExecutor(
	template *TestTemplate,
	idaClient *clients.IDAServiceClient,
	dataClient *clients.DataServiceClient,
	strategy strategies.DatabaseStrategy,
	namespaceMgr *isolation.NamespaceManager,
) *TestExecutor {
	namespace := namespaceMgr.GenerateNamespace(template.Name)

	return &TestExecutor{
		template:     template,
		idaClient:    idaClient,
		dataClient:   dataClient,
		strategy:     strategy,
		namespaceMgr: namespaceMgr,
		Namespace:    namespace,
		namespace:    namespace,
	}
}

// TestResult 测试结果
type TestResult struct {
	TemplateName string
	Namespace    string
	StartTime    time.Time
	EndTime      time.Time
	Duration     time.Duration
	TestResults  []*SingleTestResult
	HasFailure   bool
}

// SingleTestResult 单个测试结果
type SingleTestResult struct {
	TestType    string
	Expected    int64
	Actual      int64
	Diff        int64
	DiffPercent float64
	Passed      bool
	Message     string
	Error       string
	StartTime   time.Time
	EndTime     time.Time
	Duration    time.Duration
	Metrics     map[string]interface{}
}

// Execute 执行测试
func (te *TestExecutor) Execute(ctx context.Context) (*TestResult, error) {
	result := &TestResult{
		TemplateName: te.template.Name,
		Namespace:    te.namespace,
		StartTime:    time.Now(),
		TestResults:  make([]*SingleTestResult, 0),
	}

	// 1. 准备阶段
	if err := te.setup(ctx); err != nil {
		return nil, fmt.Errorf("准备阶段失败: %v", err)
	}
	defer te.cleanup(ctx)

	// 2. 执行测试
	for _, test := range te.template.Tests {
		testResult := te.executeTest(ctx, test)
		result.TestResults = append(result.TestResults, testResult)

		if !testResult.Passed {
			result.HasFailure = true
		}
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result, nil
}

// setup 准备阶段
func (te *TestExecutor) setup(ctx context.Context) error {
	// 连接数据库
	if err := te.strategy.Connect(ctx); err != nil {
		return fmt.Errorf("连接数据库失败: %v", err)
	}

	// 生成表名和资产名
	// 如果模板中指定了表名，直接使用（不加命名空间前缀）；否则自动生成（加前缀）
	if te.template.Schema.TableName != "" {
		// 如果指定了表名，直接使用，不加命名空间前缀
		te.tableName = te.template.Schema.TableName
	} else {
		// 默认使用 "test_table"，加上命名空间前缀以保证唯一性
		te.tableName = te.namespaceMgr.GenerateTableName(te.namespace, "test_table")
	}

	// 如果模板中指定了资产名，直接使用（不加命名空间前缀）；否则自动生成（加前缀）
	if te.template.Schema.AssetName != "" {
		// 如果指定了资产名，直接使用，不加命名空间前缀
		te.assetName = te.template.Schema.AssetName
	} else {
		// 默认使用模板名称，加上命名空间前缀以保证唯一性
		te.assetName = te.namespaceMgr.GenerateAssetName(te.namespace, te.template.Name)
	}

	// 生成schema
	mapper := generators.NewDatabaseTypeMapper(te.template.Database.Type)
	schema := mapper.GenerateSchema(
		te.tableName,
		te.template.Schema.FieldCount,
		te.template.Data.RowCount,
		te.template.Schema.FieldTypes,
		te.template.Schema.MaxFieldSize,
	)
	// 保存schema以便后续使用
	te.schema = schema

	// 检查表是否存在
	tableExists, err := te.strategy.TableExists(ctx, te.tableName)
	if err != nil {
		return fmt.Errorf("检查表是否存在失败: %v", err)
	}

	if tableExists {
		// 表已存在，检查数据行数是否符合要求
		actualCount, err := te.strategy.GetRowCount(ctx, te.tableName)
		if err != nil {
			return fmt.Errorf("查询表行数失败: %v", err)
		}

		expectedCount := te.template.Data.RowCount
		// 允许0.1%的误差
		diff := float64(actualCount-expectedCount) / float64(expectedCount) * 100
		if diff >= -0.1 && diff <= 0.1 {
			fmt.Printf("表 %s 已存在，数据行数符合要求 (%d 行)，跳过数据生成\n", te.tableName, actualCount)
			// 表存在且数据符合要求，跳过数据生成
		} else {
			fmt.Printf("表 %s 已存在，但数据行数不匹配 (期望: %d, 实际: %d)，将重新生成数据\n",
				te.tableName, expectedCount, actualCount)
			// 删除旧表，重新生成
			if err := te.strategy.Cleanup(ctx, te.tableName); err != nil {
				return fmt.Errorf("清理旧表失败: %v", err)
			}
			tableExists = false // 标记为不存在，继续生成数据
		}
	}

	// 如果表不存在或数据不符合要求，生成数据
	if !tableExists {
		// 生成数据（直接存储在配置的数据库中）
		generator := generators.NewDataGenerator(te.strategy.GetDB(), schema, mapper)
		if err := generator.GenerateAndInsert(ctx); err != nil {
			return fmt.Errorf("生成数据失败: %v", err)
		}
		fmt.Printf("数据生成完成: 表 %s，行数: %d\n", te.tableName, te.template.Data.RowCount)
	}

	// 注册到IDA-service
	if err := te.registerToIDAService(ctx); err != nil {
		return fmt.Errorf("注册到IDA-service失败: %v", err)
	}

	return nil
}

// executeTest 执行单个测试
func (te *TestExecutor) executeTest(ctx context.Context, test TestConfig) *SingleTestResult {
	testResult := &SingleTestResult{
		TestType:  test.Type,
		Expected:  test.Expected,
		StartTime: time.Now(),
		Metrics:   make(map[string]interface{}),
	}

	validator := validators.NewRowCountValidator(test.Expected, test.Tolerance)

	switch test.Type {
	case "read":
		testResult = te.testRead(ctx, validator, test)
	case "write":
		testResult = te.testWrite(ctx, validator, test)
	case "read_write":
		// 先写后读
		writeResult := te.testWrite(ctx, validator, test)
		if !writeResult.Passed {
			testResult = writeResult
			break
		}
		testResult = te.testRead(ctx, validator, test)
	case "get_table_info":
		testResult = te.testGetTableInfo(ctx, validator, test)
	case "read_internal":
		testResult = te.testReadInternalData(ctx, validator, test)
	case "write_internal":
		testResult = te.testWriteInternalData(ctx, validator, test)
	case "execute_sql":
		testResult = te.testExecuteSql(ctx, validator, test)
	case "read_new":
		testResult = te.testReadNew(ctx, validator, test)
	case "write_new":
		testResult = te.testWriteNew(ctx, validator, test)
	default:
		testResult.Passed = false
		testResult.Error = fmt.Sprintf("未知的测试类型: %s", test.Type)
	}

	testResult.EndTime = time.Now()
	testResult.Duration = testResult.EndTime.Sub(testResult.StartTime)

	return testResult
}


// registerToIDAService 注册到IDA-service
func (te *TestExecutor) registerToIDAService(ctx context.Context) error {
	// 获取数据库连接信息
	dbConfig := te.strategy.GetConnectionInfo()

	// 映射数据库类型到IDA的数据库类型
	dbTypeMap := map[string]int32{
		"mysql":    1,
		"kingbase": 2,
		"gbase":    3,
		"vastbase": 4,
	}
	dbType, ok := dbTypeMap[dbConfig.Type]
	if !ok {
		return fmt.Errorf("不支持的数据库类型: %s", dbConfig.Type)
	}

	// 1. 创建数据源
	createDSReq := &clients.CreateDataSourceRequest{
		Name:         fmt.Sprintf("test_datasource_%s", te.namespace),
		Host:         dbConfig.Host,
		Port:         int32(dbConfig.Port),
		DBType:       dbType,
		Username:     dbConfig.User,
		Password:     dbConfig.Password,
		DatabaseName: dbConfig.Database,
	}

	dsResp, err := te.idaClient.CreateDataSource(ctx, createDSReq)
	if err != nil {
		return fmt.Errorf("创建数据源失败: %v", err)
	}

	if !dsResp.Success {
		return fmt.Errorf("创建数据源失败: %s", dsResp.Message)
	}

	te.dataSourceId = dsResp.DataSourceId
	fmt.Printf("数据源创建成功: ID=%d\n", te.dataSourceId)

	// 2. 创建资产
	createAssetReq := &clients.CreateAssetRequest{
		AssetName:    te.template.Name,
		AssetEnName:  te.assetName,
		DataSourceId: te.dataSourceId,
		DBName:       dbConfig.Database,
		TableName:    te.tableName,
	}

	assetResp, err := te.idaClient.CreateAsset(ctx, createAssetReq)
	if err != nil {
		return fmt.Errorf("创建资产失败: %v", err)
	}

	if !assetResp.Success {
		return fmt.Errorf("创建资产失败: %s", assetResp.Message)
	}

	te.assetId = assetResp.AssetId
	fmt.Printf("资产创建成功: ID=%d, Name=%s\n", te.assetId, te.assetName)

	return nil
}

// cleanup 清理
func (te *TestExecutor) cleanup(ctx context.Context) {
	// 如果模板中指定了表名或设置了 keep_table，保留表不删除
	if te.template.Schema.TableName != "" || te.template.Data.KeepTable {
		fmt.Printf("保留表 %s（模板指定或设置了 keep_table）\n", te.tableName)
		return
	}

	// 否则删除表
	if te.strategy != nil {
		fmt.Printf("清理测试表: %s\n", te.tableName)
		te.strategy.Cleanup(ctx, te.tableName)
	}
}

// getFieldNames 获取字段名列表
func (te *TestExecutor) getFieldNames() []string {
	if te.schema == nil || len(te.schema.Fields) == 0 {
		// 如果没有schema，返回空数组让 data-service 使用 SELECT * FROM ...
		return []string{}
	}
	
	// 从schema获取实际字段名
	fieldNames := make([]string, 0, len(te.schema.Fields))
	for _, field := range te.schema.Fields {
		fieldNames = append(fieldNames, field.Name)
	}
	return fieldNames
}
