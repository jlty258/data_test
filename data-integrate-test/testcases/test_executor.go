package testcases

import (
	"context"
	"data-integrate-test/clients"
	"data-integrate-test/generators"
	"data-integrate-test/isolation"
	"data-integrate-test/snapshots"
	"data-integrate-test/strategies"
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
	snapshotMgr  *snapshots.SnapshotManager
	namespaceMgr *isolation.NamespaceManager
	Namespace    string // 导出以便外部访问
	namespace    string
	tableName    string
	assetName    string
	dataSourceId int32
	assetId      int32
}

func NewTestExecutor(
	template *TestTemplate,
	idaClient *clients.IDAServiceClient,
	dataClient *clients.DataServiceClient,
	strategy strategies.DatabaseStrategy,
	snapshotMgr *snapshots.SnapshotManager,
	namespaceMgr *isolation.NamespaceManager,
) *TestExecutor {
	namespace := namespaceMgr.GenerateNamespace(template.Name)

	return &TestExecutor{
		template:     template,
		idaClient:    idaClient,
		dataClient:   dataClient,
		strategy:     strategy,
		snapshotMgr:  snapshotMgr,
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
	te.tableName = te.namespaceMgr.GenerateTableName(te.namespace, "test_table")
	te.assetName = te.namespaceMgr.GenerateAssetName(te.namespace, te.template.Name)

	// 生成schema
	mapper := generators.NewDatabaseTypeMapper(te.template.Database.Type)
	schema := mapper.GenerateSchema(te.tableName, te.template.Schema.FieldCount, te.template.Data.RowCount)

	// 检查快照
	if te.template.Data.UseSnapshot {
		snapshot, err := te.snapshotMgr.CreateSnapshot(ctx, te.template.Name, schema, te.template.Data.RowCount)
		if err == nil && snapshot != nil {
			fmt.Printf("发现快照: %s，但需要检查数据是否存在\n", snapshot.Name)
			// TODO: 如果快照数据存在，可以跳过数据生成
		}
	}

	// 生成数据
	generator := generators.NewDataGenerator(te.strategy.GetDB(), schema, mapper)
	if err := generator.GenerateAndInsert(ctx); err != nil {
		return fmt.Errorf("生成数据失败: %v", err)
	}

	// 注册到IDA-service
	// TODO: 实现CreateDataSource和CreateAsset
	// te.dataSourceId, te.assetId = ...

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
	default:
		testResult.Passed = false
		testResult.Error = fmt.Sprintf("未知的测试类型: %s", test.Type)
	}

	testResult.EndTime = time.Now()
	testResult.Duration = testResult.EndTime.Sub(testResult.StartTime)

	return testResult
}

// testRead 测试读取
func (te *TestExecutor) testRead(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "read",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// TODO: 调用data-service的ReadStreamingData
	// 这里需要proto编译后才能实现
	// req := &datasource.StreamReadRequest{
	//     AssetName:   te.assetName,
	//     ChainInfoId: "test_chain",
	//     RequestId:   fmt.Sprintf("read_%d", time.Now().Unix()),
	//     DbFields:    te.getFieldNames(),
	// }
	//
	// readStart := time.Now()
	// responses, err := te.dataClient.ReadStreamingData(ctx, req)
	// readDuration := time.Since(readStart)
	//
	// if err != nil {
	//     result.Passed = false
	//     result.Error = err.Error()
	//     return result
	// }
	//
	// // 统计行数
	// actualCount := int64(0)
	// for _, resp := range responses {
	//     count, _ := utils.CountRowsFromArrow(resp.Data)
	//     actualCount += count
	// }
	//
	// // 验证行数
	// validationResult, err := validator.ValidateReadResult(ctx, actualCount)
	// if err != nil {
	//     result.Passed = false
	//     result.Error = err.Error()
	//     return result
	// }

	// 临时实现：从数据库直接查询
	actualCount, err := te.strategy.GetRowCount(ctx, te.tableName)
	if err != nil {
		result.Passed = false
		result.Error = err.Error()
		return result
	}

	validationResult, err := validator.ValidateReadResult(ctx, actualCount)
	if err != nil {
		result.Passed = false
		result.Error = err.Error()
		return result
	}

	result.Passed = validationResult.Passed
	result.Actual = validationResult.Actual
	result.Diff = validationResult.Diff
	result.DiffPercent = validationResult.DiffPercent
	result.Message = validationResult.Message

	return result
}

// testWrite 测试写入
func (te *TestExecutor) testWrite(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "write",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// TODO: 实现写入测试
	// 1. 生成测试数据
	// 2. 调用data-service的WriteInternalData
	// 3. 验证写入后的行数

	// 临时实现：验证当前表行数
	validationResult, err := validator.ValidateWriteResult(ctx, te.strategy.GetDB(), te.tableName)
	if err != nil {
		result.Passed = false
		result.Error = err.Error()
		return result
	}

	result.Passed = validationResult.Passed
	result.Actual = validationResult.Actual
	result.Diff = validationResult.Diff
	result.DiffPercent = validationResult.DiffPercent
	result.Message = validationResult.Message

	return result
}

// cleanup 清理
func (te *TestExecutor) cleanup(ctx context.Context) {
	if te.strategy != nil {
		te.strategy.Cleanup(ctx, te.tableName)
	}
}

// getFieldNames 获取字段名列表
func (te *TestExecutor) getFieldNames() []string {
	// TODO: 从schema获取
	return []string{"*"}
}
