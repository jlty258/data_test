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
)

func main() {
	var (
		snapshotDir = flag.String("snapshots", "snapshots/exported", "快照文件目录")
		configPath  = flag.String("config", "config/test_config.yaml", "配置文件路径")
		schemaFile  = flag.String("schema", "", "表结构文件路径（.sql文件）")
		dataFile    = flag.String("data", "", "表数据文件路径（.csv文件）")
		tableName   = flag.String("table", "", "目标表名（如果不指定，使用快照文件中的表名）")
		dbType      = flag.String("db", "", "数据库类型（mysql/kingbase/gbase/vastbase），如果不指定则从配置推断")
		batchSize   = flag.Int("batch", 5000, "批量插入大小（默认5000行）")
	)
	flag.Parse()

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 确定要导入的文件
	var schemaFiles, dataFiles []string
	if *schemaFile != "" && *dataFile != "" {
		// 指定了具体文件
		schemaFiles = []string{*schemaFile}
		dataFiles = []string{*dataFile}
	} else {
		// 扫描快照目录
		schemaFiles, dataFiles, err = scanSnapshotFiles(*snapshotDir)
		if err != nil {
			log.Fatalf("扫描快照文件失败: %v", err)
		}
		if len(schemaFiles) == 0 {
			log.Fatalf("未找到快照文件")
		}
	}

	// 创建导入器
	importer := snapshots.NewSnapshotImporter(*batchSize)

	// 导入每个快照
	for i := 0; i < len(schemaFiles); i++ {
		schemaFile := schemaFiles[i]
		dataFile := dataFiles[i]

		fmt.Printf("\n处理快照: %s\n", filepath.Base(schemaFile))

		// 确定表名
		targetTableName := *tableName
		if targetTableName == "" {
			// 从文件名提取表名
			targetTableName = extractTableNameFromFile(schemaFile)
		}

		// 确定数据库类型
		targetDBType := *dbType
		if targetDBType == "" {
			// 从配置推断（使用第一个匹配的数据库）
			if len(cfg.Databases) > 0 {
				targetDBType = cfg.Databases[0].Type
			} else {
				log.Fatalf("无法确定数据库类型，请使用 -db 参数指定")
			}
		}

		// 获取数据库配置
		dbConfig, err := cfg.GetDatabaseConfig(targetDBType)
		if err != nil {
			log.Fatalf("获取数据库配置失败: %v", err)
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

		// 导入快照
		if err := importer.ImportTableSnapshot(ctx, strategy, schemaFile, dataFile, targetTableName); err != nil {
			log.Fatalf("导入快照失败: %v", err)
		}
	}

	fmt.Println("\n✅ 所有快照导入完成！")
}

// scanSnapshotFiles 扫描快照目录，找到所有 schema.sql 和 data.csv 文件对
func scanSnapshotFiles(snapshotDir string) ([]string, []string, error) {
	var schemaFiles, dataFiles []string

	err := filepath.Walk(snapshotDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(path, "_schema.sql") {
			// 找到 schema 文件，查找对应的 data 文件
			dataFile := strings.Replace(path, "_schema.sql", "_data.csv", 1)
			if _, err := os.Stat(dataFile); err == nil {
				schemaFiles = append(schemaFiles, path)
				dataFiles = append(dataFiles, dataFile)
			}
		}

		return nil
	})

	return schemaFiles, dataFiles, err
}

// extractTableNameFromFile 从文件名提取表名
func extractTableNameFromFile(filePath string) string {
	fileName := filepath.Base(filePath)
	// 格式: {template_name}_{table_name}_schema.sql
	// 提取 {table_name} 部分
	if idx := strings.LastIndex(fileName, "_schema.sql"); idx > 0 {
		// 找到最后一个下划线之前的部分作为表名
		parts := strings.Split(fileName[:idx], "_")
		if len(parts) > 1 {
			// 返回最后两部分作为表名（通常是 template_name_table_name）
			return strings.Join(parts[len(parts)-2:], "_")
		}
		return parts[0]
	}
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}
