package main

import (
	"context"
	"data-integrate-test/config"
	"data-integrate-test/snapshots"
	"data-integrate-test/strategies"
	"flag"
	"fmt"
	"io"
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
		tableName  = flag.String("table", "", "目标表名（必需）")
		input      = flag.String("input", "", "输入路径（本地文件系统路径或 MinIO 路径，格式：minio://bucket/path，必需）")
		batchSize  = flag.Int("batch", 5000, "批量插入大小（默认5000行）")
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
		log.Fatalf("必须指定目标表名: -table")
	}
	if *input == "" {
		log.Fatalf("必须指定输入路径: -input")
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

	// 确定输入方式并导入
	if strings.HasPrefix(*input, "minio://") {
		// 从 MinIO 导入
		if err := importFromMinIO(ctx, strategy, *tableName, *input, cfg, *batchSize); err != nil {
			log.Fatalf("从 MinIO 导入失败: %v", err)
		}
	} else {
		// 从本地文件系统导入
		if err := importFromLocal(ctx, strategy, *tableName, *input, *batchSize); err != nil {
			log.Fatalf("从本地文件系统导入失败: %v", err)
		}
	}

	fmt.Println("\n✅ 导入完成！")
}

// importFromLocal 从本地文件系统导入
func importFromLocal(ctx context.Context, strategy strategies.DatabaseStrategy, tableName, inputPath string, batchSize int) error {
	// 确定文件路径
	var schemaFile, dataFile string

	// 如果输入是目录，查找文件
	if info, err := os.Stat(inputPath); err == nil && info.IsDir() {
		// 在目录中查找 schema.sql 和 data.csv 文件
		// 查找包含 tableName 的文件，或使用第一个找到的文件
		files, err := os.ReadDir(inputPath)
		if err != nil {
			return fmt.Errorf("读取目录失败: %v", err)
		}

		for _, file := range files {
			if strings.HasSuffix(file.Name(), "_schema.sql") {
				schemaFile = filepath.Join(inputPath, file.Name())
			} else if strings.HasSuffix(file.Name(), "_data.csv") {
				dataFile = filepath.Join(inputPath, file.Name())
			}
		}

		// 如果没找到，尝试使用 tableName 构建文件名
		if schemaFile == "" {
			schemaFile = filepath.Join(inputPath, tableName+"_schema.sql")
		}
		if dataFile == "" {
			dataFile = filepath.Join(inputPath, tableName+"_data.csv")
		}
	} else {
		// 输入是文件路径（可能是 schema 或 data 文件）
		if strings.HasSuffix(inputPath, "_schema.sql") {
			schemaFile = inputPath
			dataFile = strings.Replace(inputPath, "_schema.sql", "_data.csv", 1)
		} else if strings.HasSuffix(inputPath, "_data.csv") {
			dataFile = inputPath
			schemaFile = strings.Replace(inputPath, "_data.csv", "_schema.sql", 1)
		} else {
			// 假设是基础路径，添加后缀
			schemaFile = inputPath + "_schema.sql"
			dataFile = inputPath + "_data.csv"
		}
	}

	// 检查文件是否存在
	if _, err := os.Stat(schemaFile); os.IsNotExist(err) {
		return fmt.Errorf("表结构文件不存在: %s", schemaFile)
	}
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		return fmt.Errorf("表数据文件不存在: %s", dataFile)
	}

	fmt.Printf("导入表: %s\n", tableName)
	fmt.Printf("表结构文件: %s\n", schemaFile)
	fmt.Printf("表数据文件: %s\n", dataFile)

	// 创建导入器
	importer := snapshots.NewSnapshotImporter(batchSize)

	// 导入快照
	return importer.ImportTableSnapshot(ctx, strategy, schemaFile, dataFile, tableName)
}

// importFromMinIO 从 MinIO 导入
func importFromMinIO(ctx context.Context, strategy strategies.DatabaseStrategy, tableName, minioPath string, cfg *config.TestConfig, batchSize int) error {
	// 解析 MinIO 路径: minio://bucket/path
	// 例如: minio://my-bucket/snapshots/table_name
	path := strings.TrimPrefix(minioPath, "minio://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return fmt.Errorf("MinIO 路径格式错误，应为: minio://bucket/path")
	}
	bucket := parts[0]
	objectPrefix := parts[1]

	fmt.Printf("导入表: %s\n", tableName)
	fmt.Printf("MinIO Bucket: %s\n", bucket)
	fmt.Printf("MinIO 路径: %s\n", objectPrefix)

	// 创建 MinIO 客户端
	minioClient, err := newMinIOClient(cfg)
	if err != nil {
		return fmt.Errorf("创建 MinIO 客户端失败: %v", err)
	}

	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "import_table_*")
	if err != nil {
		return fmt.Errorf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir) // 清理临时文件

	// 下载文件
	schemaObjectName := objectPrefix + "_schema.sql"
	dataObjectName := objectPrefix + "_data.csv"

	schemaFile := filepath.Join(tempDir, "schema.sql")
	dataFile := filepath.Join(tempDir, "data.csv")

	fmt.Printf("  下载文件: %s/%s\n", bucket, schemaObjectName)
	if err := downloadFileFromMinIO(minioClient, bucket, schemaObjectName, schemaFile); err != nil {
		return fmt.Errorf("下载表结构文件失败: %v", err)
	}

	fmt.Printf("  下载文件: %s/%s\n", bucket, dataObjectName)
	if err := downloadFileFromMinIO(minioClient, bucket, dataObjectName, dataFile); err != nil {
		return fmt.Errorf("下载表数据文件失败: %v", err)
	}

	// 创建导入器
	importer := snapshots.NewSnapshotImporter(batchSize)

	// 导入快照
	return importer.ImportTableSnapshot(ctx, strategy, schemaFile, dataFile, tableName)
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

// downloadFileFromMinIO 从 MinIO 下载文件
func downloadFileFromMinIO(client *MinIOClientWrapper, bucket, objectName, localFilePath string) error {
	ctx := context.Background()

	// 获取对象
	object, err := client.client.GetObject(ctx, bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("获取对象失败: %v", err)
	}
	defer object.Close()

	// 获取对象信息
	stat, err := object.Stat()
	if err != nil {
		return fmt.Errorf("获取对象信息失败: %v", err)
	}

	fmt.Printf("    对象大小: %s\n", formatFileSize(stat.Size))

	// 创建本地文件
	file, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("创建本地文件失败: %v", err)
	}
	defer file.Close()

	// 下载文件
	_, err = io.Copy(file, object)
	if err != nil {
		return fmt.Errorf("下载文件失败: %v", err)
	}

	return nil
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
