package main

import (
	"context"
	"data-integrate-test/config"
	"data-integrate-test/snapshots"
	"data-integrate-test/strategies"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	var (
		configPath = flag.String("config", "config/test_config.yaml", "配置文件路径")
		dbType     = flag.String("db", "", "数据库类型（mysql/kingbase/gbase/vastbase，必需）")
		dbName     = flag.String("dbname", "", "数据库名称（必需）")
		tableName  = flag.String("table", "", "表名（必需）")
		output     = flag.String("output", "", "输出路径（本地文件系统路径或 MinIO 路径，格式：minio://bucket/path，必需）")
	)
	flag.Parse()

	// 参数验证
	if *dbType == "" {
		log.Fatalf("必须指定数据库类型: -db")
	}
	if *dbName == "" {
		log.Fatalf("必须指定数据库名称: -dbname")
	}
	if *tableName == "" {
		log.Fatalf("必须指定表名: -table")
	}
	if *output == "" {
		log.Fatalf("必须指定输出路径: -output")
	}

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 获取数据库配置
	var dbConfig *config.DatabaseConfig
	if *dbName != "" {
		// 查找指定名称的数据库配置
		for _, db := range cfg.Databases {
			if db.Name == *dbName && db.Type == *dbType {
				dbConfig = &config.DatabaseConfig{
					Type:     db.Type,
					Name:     db.Name,
					Host:     db.Host,
					Port:     db.Port,
					User:     db.User,
					Password: db.Password,
					Database: *dbName,
				}
				break
			}
		}
		if dbConfig == nil {
			// 如果找不到，尝试使用类型匹配，并使用指定的数据库名
			dbConfig, err = cfg.GetDatabaseConfig(*dbType)
			if err != nil {
				log.Fatalf("未找到数据库配置: type=%s, name=%s", *dbType, *dbName)
			}
			dbConfig.Database = *dbName
		}
	} else {
		dbConfig, err = cfg.GetDatabaseConfig(*dbType)
		if err != nil {
			log.Fatalf("获取数据库配置失败: %v", err)
		}
	}

	// 创建数据库策略
	strategyFactory := strategies.NewDatabaseStrategyFactory()
	strategy, err := strategyFactory.CreateStrategy(dbConfig)
	if err != nil {
		log.Fatalf("创建数据库策略失败: %v", err)
	}

	// 连接数据库
	ctx := context.Background()
	if err := strategy.Connect(ctx); err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	defer strategy.GetDB().Close()

	// 检查表是否存在
	exists, err := strategy.TableExists(ctx, *tableName)
	if err != nil {
		log.Fatalf("检查表是否存在失败: %v", err)
	}
	if !exists {
		log.Fatalf("表 %s 不存在", *tableName)
	}

	// 确定输出方式
	if strings.HasPrefix(*output, "minio://") {
		// MinIO 导出
		if err := exportToMinIO(ctx, strategy, *tableName, *output, cfg); err != nil {
			log.Fatalf("导出到 MinIO 失败: %v", err)
		}
	} else {
		// 本地文件系统导出
		if err := exportToLocal(ctx, strategy, *tableName, *output); err != nil {
			log.Fatalf("导出到本地文件系统失败: %v", err)
		}
	}

	fmt.Println("\n✅ 导出完成！")
}

// exportToLocal 导出到本地文件系统
func exportToLocal(ctx context.Context, strategy strategies.DatabaseStrategy, tableName, outputPath string) error {
	// 确保输出目录存在
	outputDir := filepath.Dir(outputPath)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return fmt.Errorf("创建输出目录失败: %v", err)
		}
	}

	// 生成文件名
	baseName := strings.TrimSuffix(filepath.Base(outputPath), filepath.Ext(outputPath))
	if baseName == "" {
		baseName = tableName
	}
	outputDir = filepath.Dir(outputPath)
	if outputDir == "" {
		outputDir = "."
	}

	schemaFile := filepath.Join(outputDir, baseName+"_schema.sql")
	dataFile := filepath.Join(outputDir, baseName+"_data.csv")

	fmt.Printf("导出表: %s\n", tableName)
	fmt.Printf("表结构文件: %s\n", schemaFile)
	fmt.Printf("表数据文件: %s\n", dataFile)

	// 创建快照导出器
	exporter := snapshots.NewSnapshotExporter(outputDir)

	// 导出快照
	return exporter.ExportTableSnapshot(ctx, strategy, baseName, tableName)
}

// exportToMinIO 导出到 MinIO
func exportToMinIO(ctx context.Context, strategy strategies.DatabaseStrategy, tableName, minioPath string, cfg *config.TestConfig) error {
	// 解析 MinIO 路径: minio://bucket/path
	// 例如: minio://my-bucket/snapshots/table_name
	path := strings.TrimPrefix(minioPath, "minio://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return fmt.Errorf("MinIO 路径格式错误，应为: minio://bucket/path")
	}
	bucket := parts[0]
	objectPrefix := parts[1]

	fmt.Printf("导出表: %s\n", tableName)
	fmt.Printf("MinIO Bucket: %s\n", bucket)
	fmt.Printf("MinIO 路径: %s\n", objectPrefix)

	// 先导出到临时目录
	tempDir, err := os.MkdirTemp("", "export_table_*")
	if err != nil {
		return fmt.Errorf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir) // 清理临时文件

	// 导出到临时目录
	exporter := snapshots.NewSnapshotExporter(tempDir)
	baseName := filepath.Base(objectPrefix)
	if baseName == "" {
		baseName = tableName
	}

	if err := exporter.ExportTableSnapshot(ctx, strategy, baseName, tableName); err != nil {
		return fmt.Errorf("导出快照失败: %v", err)
	}

	// 上传到 MinIO
	schemaFile := filepath.Join(tempDir, fmt.Sprintf("%s_%s_schema.sql", baseName, tableName))
	dataFile := filepath.Join(tempDir, fmt.Sprintf("%s_%s_data.csv", baseName, tableName))

	minioClient, err := newMinIOClient(cfg)
	if err != nil {
		return fmt.Errorf("创建 MinIO 客户端失败: %v", err)
	}

	// 上传表结构文件
	schemaObjectName := objectPrefix + "_schema.sql"
	if err := uploadFileToMinIO(minioClient, bucket, schemaObjectName, schemaFile); err != nil {
		return fmt.Errorf("上传表结构文件失败: %v", err)
	}
	fmt.Printf("✅ 表结构已上传: %s/%s\n", bucket, schemaObjectName)

	// 上传表数据文件
	dataObjectName := objectPrefix + "_data.csv"
	if err := uploadFileToMinIO(minioClient, bucket, dataObjectName, dataFile); err != nil {
		return fmt.Errorf("上传表数据文件失败: %v", err)
	}
	fmt.Printf("✅ 表数据已上传: %s/%s\n", bucket, dataObjectName)

	return nil
}

// newMinIOClient 创建 MinIO 客户端
func newMinIOClient(cfg *config.TestConfig) (*MinIOClientWrapper, error) {
	minioConfig := cfg.MinIO
	if minioConfig.Endpoint == "" {
		return nil, fmt.Errorf("MinIO 配置未找到，请在配置文件中配置 minio")
	}

	// 处理 endpoint（移除 http:// 或 https:// 前缀）
	endpoint := minioConfig.Endpoint
	secure := false
	if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
		secure = true
	} else if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
		secure = false
	}

	// 使用 minio-go 库创建客户端
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioConfig.AccessKey, minioConfig.SecretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 MinIO 客户端失败: %v", err)
	}

	return &MinIOClientWrapper{client: client}, nil
}

// MinIOClientWrapper MinIO 客户端包装
type MinIOClientWrapper struct {
	client *minio.Client
}

// uploadFileToMinIO 上传文件到 MinIO
func uploadFileToMinIO(client *MinIOClientWrapper, bucket, objectName, filePath string) error {
	ctx := context.Background()

	// 检查 bucket 是否存在，不存在则创建
	exists, err := client.client.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("检查 bucket 是否存在失败: %v", err)
	}
	if !exists {
		if err := client.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("创建 bucket 失败: %v", err)
		}
		fmt.Printf("  创建 bucket: %s\n", bucket)
	}

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("获取文件信息失败: %v", err)
	}

	fmt.Printf("  上传文件: %s -> %s/%s (大小: %s)\n",
		filePath, bucket, objectName, formatFileSize(fileInfo.Size()))

	// 上传文件
	_, err = client.client.PutObject(ctx, bucket, objectName, file, fileInfo.Size(), minio.PutObjectOptions{
		ContentType: getContentType(filePath),
	})
	if err != nil {
		return fmt.Errorf("上传文件失败: %v", err)
	}

	return nil
}

// getContentType 根据文件扩展名获取 Content-Type
func getContentType(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".sql":
		return "text/plain"
	case ".csv":
		return "text/csv"
	default:
		return "application/octet-stream"
	}
}

// formatFileSize 格式化文件大小
func formatFileSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}
