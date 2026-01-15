package main

import (
	"context"
	"data-integrate-test/clients"
	"data-integrate-test/config"
	"data-integrate-test/isolation"
	"data-integrate-test/snapshots"
	"data-integrate-test/strategies"
	"data-integrate-test/testcases"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	var (
		templatePath = flag.String("template", "", "测试模板路径（必需）")
		configPath   = flag.String("config", "config/test_config.yaml", "配置文件路径")
		parallel     = flag.Int("parallel", 1, "并发数")
	)
	flag.Parse()

	if *templatePath == "" {
		log.Fatal("必须指定测试模板路径: -template")
	}

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 加载模板
	template, err := testcases.LoadTemplate(*templatePath)
	if err != nil {
		log.Fatalf("加载模板失败: %v", err)
	}

	// 创建客户端
	idaClient, err := clients.NewIDAServiceClient(cfg.MockServices.IDA.Host, cfg.MockServices.IDA.Port)
	if err != nil {
		log.Fatalf("创建IDA客户端失败: %v", err)
	}
	defer idaClient.Close()

	dataClient, err := clients.NewDataServiceClient(cfg.DataService.Host, cfg.DataService.Port)
	if err != nil {
		log.Fatalf("创建Data Service客户端失败: %v", err)
	}
	defer dataClient.Close()

	// 创建管理器
	snapshotMgr, err := snapshots.NewSnapshotManager("snapshots")
	if err != nil {
		log.Fatalf("创建快照管理器失败: %v", err)
	}
	defer snapshotMgr.Close()

	nsMgr := isolation.NewNamespaceManager("test")

	// 获取数据库配置
	dbConfig, err := cfg.GetDatabaseConfig(template.Database.Type)
	if err != nil {
		log.Fatalf("获取数据库配置失败: %v", err)
	}

	// 创建数据库策略
	strategyFactory := strategies.NewDatabaseStrategyFactory()
	strategy, err := strategyFactory.CreateStrategy(dbConfig)
	if err != nil {
		log.Fatalf("创建数据库策略失败: %v", err)
	}

	// 创建测试执行器
	executor := testcases.NewTestExecutor(
		template,
		idaClient,
		dataClient,
		strategy,
		snapshotMgr,
		nsMgr,
	)

	// 执行测试
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	fmt.Printf("开始执行测试: %s\n", template.Name)
	fmt.Printf("命名空间: %s\n", executor.Namespace)
	
	result, err := executor.Execute(ctx)
	if err != nil {
		log.Fatalf("测试执行失败: %v", err)
	}

	// 输出结果
	printResults(result)

	if result.HasFailure {
		fmt.Println("\n❌ 存在测试失败")
		os.Exit(1)
	}

	fmt.Println("\n✅ 所有测试通过！")
}

func printResults(result *testcases.TestResult) {
	fmt.Printf("\n========== 测试结果 ==========\n")
	fmt.Printf("模板名称: %s\n", result.TemplateName)
	fmt.Printf("命名空间: %s\n", result.Namespace)
	fmt.Printf("执行时间: %v\n", result.Duration)
	fmt.Printf("开始时间: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("结束时间: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("\n测试详情:\n")
	
	for i, tr := range result.TestResults {
		fmt.Printf("\n  [%d] %s\n", i+1, tr.TestType)
		if tr.Error != "" {
			fmt.Printf("    错误: %s\n", tr.Error)
		} else {
			status := "✅ 通过"
			if !tr.Passed {
				status = "❌ 失败"
			}
			fmt.Printf("    状态: %s\n", status)
			fmt.Printf("    期望: %d 行\n", tr.Expected)
			fmt.Printf("    实际: %d 行\n", tr.Actual)
			if tr.Diff != 0 {
				fmt.Printf("    差异: %d 行 (%.2f%%)\n", tr.Diff, tr.DiffPercent)
			}
			fmt.Printf("    消息: %s\n", tr.Message)
			fmt.Printf("    耗时: %v\n", tr.Duration)
		}
	}
	fmt.Printf("\n==============================\n")
}

