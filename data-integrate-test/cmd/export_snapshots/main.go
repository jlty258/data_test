package main

import (
	"context"
	"data-integrate-test/config"
	"data-integrate-test/isolation"
	"data-integrate-test/snapshots"
	"data-integrate-test/strategies"
	"data-integrate-test/testcases"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	var (
		templateDir = flag.String("templates", "templates", "模板文件目录")
		configPath  = flag.String("config", "config/test_config.yaml", "配置文件路径")
		outputDir   = flag.String("output", "snapshots/exported", "快照输出目录")
		template    = flag.String("template", "", "指定单个模板文件（可选，如果不指定则处理所有模板）")
	)
	flag.Parse()

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 创建快照导出器
	exporter := snapshots.NewSnapshotExporter(*outputDir)

	// 创建命名空间管理器（用于生成表名）
	nsMgr := isolation.NewNamespaceManager("test")

	// 处理模板
	if *template != "" {
		// 处理单个模板
		if err := processTemplate(context.Background(), exporter, cfg, nsMgr, *template); err != nil {
			log.Fatalf("处理模板失败: %v", err)
		}
	} else {
		// 处理所有模板
		if err := processAllTemplates(context.Background(), exporter, cfg, nsMgr, *templateDir); err != nil {
			log.Fatalf("处理模板失败: %v", err)
		}
	}

	fmt.Println("\n✅ 所有快照导出完成！")
}

// processAllTemplates 处理所有模板文件
func processAllTemplates(ctx context.Context, exporter *snapshots.SnapshotExporter, cfg *config.TestConfig, nsMgr *isolation.NamespaceManager, templateDir string) error {
	// 遍历模板目录
	return filepath.Walk(templateDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 只处理 YAML 文件
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".yaml") {
			fmt.Printf("\n处理模板: %s\n", path)
			if err := processTemplate(ctx, exporter, cfg, nsMgr, path); err != nil {
				log.Printf("⚠️  处理模板 %s 失败: %v", path, err)
				// 继续处理其他模板，不中断
			}
		}

		return nil
	})
}

// processTemplate 处理单个模板
func processTemplate(ctx context.Context, exporter *snapshots.SnapshotExporter, cfg *config.TestConfig, nsMgr *isolation.NamespaceManager, templatePath string) error {
	// 加载模板
	template, err := testcases.LoadTemplate(templatePath)
	if err != nil {
		return fmt.Errorf("加载模板失败: %v", err)
	}

	// 获取数据库配置
	var dbConfig *config.DatabaseConfig
	if template.Database.Name != "" {
		// 尝试通过名称查找配置
		for _, db := range cfg.Databases {
			if db.Name == template.Database.Name && db.Type == template.Database.Type {
				dbConfig = &config.DatabaseConfig{
					Type:     db.Type,
					Name:     db.Name,
					Host:     db.Host,
					Port:     db.Port,
					User:     db.User,
					Password: db.Password,
					Database: template.Database.Name,
				}
				break
			}
		}
		if dbConfig == nil {
			return fmt.Errorf("未找到数据库配置: name=%s, type=%s", template.Database.Name, template.Database.Type)
		}
	} else {
		// 使用类型匹配
		var err error
		dbConfig, err = cfg.GetDatabaseConfig(template.Database.Type)
		if err != nil {
			return fmt.Errorf("获取数据库配置失败: %v", err)
		}
	}

	// 创建数据库策略
	strategyFactory := strategies.NewDatabaseStrategyFactory()
	strategy, err := strategyFactory.CreateStrategy(dbConfig)
	if err != nil {
		return fmt.Errorf("创建数据库策略失败: %v", err)
	}

	// 连接数据库
	if err := strategy.Connect(ctx); err != nil {
		return fmt.Errorf("连接数据库失败: %v", err)
	}
	defer strategy.GetDB().Close()

	// 确定表名
	var tableName string
	if template.Schema.TableName != "" {
		// 如果模板指定了表名，直接使用
		tableName = template.Schema.TableName
	} else {
		// 否则，尝试查找可能的表名
		// 由于表名可能包含命名空间，我们需要查找所有可能的表
		// 先尝试查找所有表，然后匹配模板名称
		allTables, err := exporter.GetAllTables(ctx, strategy)
		if err != nil {
			return fmt.Errorf("获取表列表失败: %v", err)
		}

		// 查找匹配的表（包含模板名称的表）
		tableName = findMatchingTable(allTables, template.Name, template.Schema.TableName)
		if tableName == "" {
			log.Printf("⚠️  未找到匹配的表，跳过模板: %s", template.Name)
			return nil
		}
	}

	// 检查表是否存在
	exists, err := strategy.TableExists(ctx, tableName)
	if err != nil {
		return fmt.Errorf("检查表是否存在失败: %v", err)
	}

	if !exists {
		log.Printf("⚠️  表 %s 不存在，跳过", tableName)
		return nil
	}

	// 导出快照
	templateName := strings.TrimSuffix(filepath.Base(templatePath), filepath.Ext(templatePath))
	if err := exporter.ExportTableSnapshot(ctx, strategy, templateName, tableName); err != nil {
		return fmt.Errorf("导出快照失败: %v", err)
	}

	return nil
}

// findMatchingTable 查找匹配的表
func findMatchingTable(allTables []string, templateName, specifiedTableName string) string {
	// 如果指定了表名，直接查找
	if specifiedTableName != "" {
		for _, table := range allTables {
			if table == specifiedTableName {
				return table
			}
		}
		// 如果直接匹配失败，尝试查找包含指定表名的表
		for _, table := range allTables {
			if strings.Contains(table, specifiedTableName) {
				return table
			}
		}
		return ""
	}

	// 如果没有指定表名，尝试查找包含模板名称的表
	// 表名格式可能是: test_{template_name}_{timestamp}_{random}_test_table
	for _, table := range allTables {
		if strings.Contains(table, templateName) {
			return table
		}
	}

	// 如果找不到，返回第一个表（可能是测试表）
	if len(allTables) > 0 {
		return allTables[0]
	}

	return ""
}
