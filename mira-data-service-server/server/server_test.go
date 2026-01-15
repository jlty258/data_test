/*
*

	@author: shiliang
	@date: 2024/12/18
	@note:

*
*/
package main

import (
	"context"
	"data-service/database"
	pb "data-service/generated/datasource"
	"data-service/log"
	"data-service/server/routes"
	"data-service/utils"
	"testing"

	"bou.ke/monkey"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type MockUtils struct {
	mock.Mock
}

func (m *MockUtils) GetDatasourceByAssetName(requestId string, assetName string, chainId int32, platformId int32) (*pb.ConnectionInfo, error) {
	args := m.Called(requestId, assetName, chainId, platformId)
	return args.Get(0).(*pb.ConnectionInfo), args.Error(1)
}

func (m *MockUtils) CreateSparkPod(clientset kubernetes.Interface, podName string, connInfo *pb.SparkDBConnInfo, sparkConfig *pb.SparkConfig) error {
	args := m.Called(clientset, podName, connInfo, sparkConfig)
	return args.Error(0)
}

func setup() {
	viper.Set("LoggerConfig.Level", "debug") // 模拟配置项
	viper.Set("TestConfig", "true")
	viper.AutomaticEnv()        // 启用自动环境变量加载（如果有需要）
	viper.SetConfigType("yaml") // 确保配置类型是 yaml（如果依赖 yaml 类型）
	viper.SetConfigFile("")     // 禁用文件读取，避免默认路径读取配置文件

	log.InitLogger()
}

func TestMain(m *testing.M) {
	setup() // 调用 setup 方法
	m.Run() // 运行所有测试
}

func TestReadBatchData_Success(t *testing.T) {
	t.Skip("Skipping test for now, will be implemented later")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// 创建一个测试服务器实例
	server := &Server{
		logger: log.Logger, // 你可以用一个假的logger来代替
	}

	// 模拟数据库查询
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock DB: %v", err)
	}
	defer db.Close() // 确保测试结束时关闭 db

	rows := sqlmock.NewRows([]string{"id", "title", "body"}).
		AddRow(1, "post 1", "hello").
		AddRow(2, "post 2", "world")
	mock.ExpectQuery("SELECT \\* FROM users").WillReturnRows(rows)

	// 设置请求对象 - 使用新的结构
	req := &pb.BatchReadRequest{
		RequestId: "req-123",
		SparkConfig: &pb.SparkConfig{
			ExecutorMemoryMB: 1024,
			ExecutorCores:    2,
		},
		// 使用外部数据源
		DataSource: &pb.BatchReadRequest_External{
			External: &pb.ExternalDataSource{
				AssetName:   "asset1",
				ChainInfoId: "100",
			},
		},
		// 使用查询操作
		Operation: &pb.BatchReadRequest_Query{
			Query: &pb.QueryOperation{
				DbFields:    []string{"field1", "field2"},
				DataObject:  "test_data.parquet",
				FilterNames: []string{"name1"},
				FilterValues: []*pb.FilterValue{
					{StrValue: "value1"},
				},
				FilterOperators: []pb.FilterOperator{pb.FilterOperator_EQUAL},
			},
		},
	}

	// 初始化 ColumnItem 数组
	columns := []*pb.ColumnItem{
		{
			Name:     "id",
			DataType: "int",
		},
		{
			Name:     "title",
			DataType: "string",
		},
		{
			Name:     "body",
			DataType: "string",
		},
	}

	// 初始化 ConnectionInfo
	connInfo := &pb.ConnectionInfo{
		Dbtype:    1, // 假设数据库类型为 1 (MySQL)
		Host:      "localhost",
		Port:      3306,
		TableName: "users",
		User:      "root",
		Password:  "password",
		DbName:    "test_db",
		Columns:   columns,
	}

	// 使用 monkey.Patch 来模拟 Kubernetes 客户端的获取函数
	monkey.Patch(rest.InClusterConfig, func() (*rest.Config, error) {
		// 返回一个简单的配置实例，不与实际 Kubernetes 集群通信
		return &rest.Config{}, nil
	})

	monkey.Patch(kubernetes.NewForConfig, func(config *rest.Config) (*kubernetes.Clientset, error) {
		// 返回一个空的 Kubernetes Clientset
		return &kubernetes.Clientset{}, nil
	})

	// 模拟 GetDatasourceByAssetName
	monkey.Patch(utils.GetDatasourceByAssetName, func(requestId string, assetName string, chainId int32, platformId int32) (*pb.ConnectionInfo, error) {
		return connInfo, nil
	})

	// 模拟数据库连接
	monkey.Patch((*database.MySQLStrategy).ConnectToDBWithPass, func(m *database.MySQLStrategy, info *pb.ConnectionInfo) error {
		// 将 mock 的 DB 注入到 MySQLStrategy
		m.DB = db
		// 返回 nil 表示成功连接
		return nil
	})

	// 模拟 Spark Pod 创建
	monkey.Patch(utils.CreateSparkPod, func(clientset *kubernetes.Clientset, podName string, info *pb.SparkDBConnInfo, sparkConfig *pb.SparkConfig, connInfo *pb.ConnectionInfo) (*v1.Pod, error) {
		// 返回一个空的 pod 对象和 nil 错误，模拟成功创建 Pod
		return &v1.Pod{}, nil
	})

	// 模拟 Spark job 返回的 Minio URL（模拟成功返回数据）
	go func() {
		// 立即向 MinioUrlChan 发送数据来模拟成功响应
		routes.MinioUrlChan <- struct {
			Data   map[string]interface{}
			Status pb.JobStatus
			Error  error
		}{
			Data: map[string]interface{}{
				"dbName":    "example-db",
				"tableName": "example-table",
			},
			Status: pb.JobStatus_JOB_STATUS_SUCCEEDED,
			Error:  nil,
		}
	}()

	resp, err := server.SubmitBatchJob(context.Background(), req)

	// 断言
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, pb.JobStatus_JOB_STATUS_SUCCEEDED, resp.Status)
}
