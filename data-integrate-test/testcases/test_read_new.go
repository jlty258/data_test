package testcases

import (
	"context"
	"fmt"
	"time"

	"data-integrate-test/config"
	"data-integrate-test/utils"
	"data-integrate-test/validators"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	pb "data-integrate-test/generated/datasource"
)

// ensureMinIOBucket 确保MinIO bucket存在，如果不存在则创建
func (te *TestExecutor) ensureMinIOBucket(ctx context.Context, bucketName string) error {
	// 从配置中获取MinIO连接信息
	// 配置文件路径：config/test_config.yaml
	cfg, err := config.LoadConfig("config/test_config.yaml")
	if err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	// 解析endpoint（格式：host:port）
	// 在Docker网络中，MinIO服务名是 "minio"
	endpoint := cfg.MinIO.Endpoint
	// 如果endpoint是localhost，替换为Docker网络中的服务名
	if endpoint == "localhost:9000" {
		endpoint = "minio:9000"
	}
	accessKey := cfg.MinIO.AccessKey
	secretKey := cfg.MinIO.SecretKey

	// 创建MinIO客户端
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false, // MinIO默认使用HTTP
	})
	if err != nil {
		return fmt.Errorf("创建MinIO客户端失败: %w", err)
	}

	// 检查bucket是否存在
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("检查bucket是否存在失败: %w", err)
	}

	// 如果bucket不存在，创建它
	if !exists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("创建bucket失败: %w", err)
		}
		fmt.Printf("MinIO bucket '%s' 已创建\n", bucketName)
	} else {
		fmt.Printf("MinIO bucket '%s' 已存在\n", bucketName)
	}

	return nil
}

// testReadNew 测试新版通用读接口
func (te *TestExecutor) testReadNew(
	ctx context.Context,
	validator *validators.RowCountValidator,
	test TestConfig,
) *SingleTestResult {
	result := &SingleTestResult{
		TestType:  "read_new",
		Expected:  test.Expected,
		StartTime: time.Now(),
	}

	// 在执行测试前，先检查并创建MinIO bucket
	// read_new接口会将数据导出到MinIO，需要bucket存在
	bucketName := "data-service" // 使用data-service的默认bucket名称
	if err := te.ensureMinIOBucket(ctx, bucketName); err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("确保MinIO bucket存在失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 构建外部数据源
	externalDS := &pb.ExternalDataSource{
		AssetName:   te.assetName,
		ChainInfoId: "test_chain_001",
		Alias:       "",
	}

	// 调用data-service的新版Read接口
	req := &pb.ReadRequest{
		DataSource: &pb.ReadRequest_External{
			External: externalDS,
		},
		Columns: te.getFieldNames(),
		// SortRules 和 FilterConditions 可以从 test.Params 中获取
	}

	readStart := time.Now()
	responses, err := te.dataClient.Read(ctx, req)
	readDuration := time.Since(readStart)

	if err != nil {
		result.Passed = false
		result.Error = fmt.Errorf("调用data-service新版Read接口失败: %w", err).Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// 统计行数（从所有Arrow响应中累计）
	actualCount := int64(0)
	for _, resp := range responses {
		if resp != nil {
			arrowBatch := resp.GetArrowBatch()
			// 跳过EOF标记和空批次
			if len(arrowBatch) == 0 {
				continue
			}
			// 检查是否是EOF标记
			if len(arrowBatch) == 3 && string(arrowBatch) == "EOF" {
				continue // EOF标记，跳过
			}
			count, err := utils.CountRowsFromArrow(arrowBatch)
			if err != nil {
				result.Passed = false
				result.Error = fmt.Errorf("解析Arrow数据失败: %w", err).Error()
				result.EndTime = time.Now()
				result.Duration = result.EndTime.Sub(result.StartTime)
				return result
			}
			actualCount += count
		}
	}

	// 验证行数
	validationResult, err := validator.ValidateReadResult(ctx, actualCount)
	if err != nil {
		result.Passed = false
		result.Error = err.Error()
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	result.Passed = validationResult.Passed
	result.Actual = validationResult.Actual
	result.Diff = validationResult.Diff
	result.DiffPercent = validationResult.DiffPercent
	result.Message = fmt.Sprintf("%s (data-service新版Read接口调用耗时: %v)", validationResult.Message, readDuration)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result
}

