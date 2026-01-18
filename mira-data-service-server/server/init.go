package main

import (
	"data-service/common"
	"data-service/config"
	"data-service/database"
	"data-service/database/gorm"
	log "data-service/log"
	"data-service/oss"
	"data-service/service"
	"errors"
	"fmt"
)

type Initializer struct {
	config           *config.DataServiceConf
	ossClient        oss.ClientInterface
	k8sService       service.K8sService
	tableInfoService service.TableInfoService
}

func (i *Initializer) Init() error {
	// 1. 首先初始化配置
	if err := i.initConfig(); err != nil {
		return fmt.Errorf("failed to init config: %v", err)
	}

	// 2. 然后初始化日志（依赖配置）
	if err := i.initLogger(); err != nil {
		return fmt.Errorf("failed to init logger: %v", err)
	}

	// 3. 初始化数据库（依赖配置和日志）
	if err := i.initDatabase(); err != nil {
		return fmt.Errorf("failed to init database: %v", err)
	}

	// 4. 初始化OSS（依赖配置、日志和数据库）
	if err := i.initOSS(); err != nil {
		return fmt.Errorf("failed to init OSS: %v", err)
	}

	// // 5. 初始化K8sService（依赖日志）
	// if err := i.initK8sService(); err != nil {
	// 	return fmt.Errorf("failed to init K8sService: %v", err)
	// }

	// 6. 初始化TableInfoService（依赖日志）
	if err := i.initTableInfoService(); err != nil {
		return fmt.Errorf("failed to init TableInfoService: %v", err)
	}

	// 7. 初始化 mira_task_tmp 数据库
	if err := i.initMiraTaskTmpDatabase(); err != nil {
		log.Logger.Errorf("failed to init doris mira_task_tmp database: %v", err)
		return fmt.Errorf("failed to init doris mira_task_tmp database: %v", err)
	}

	// 8. 初始化doris全局资源
	if err := i.initDorisResource(); err != nil {
		log.Logger.Errorf("failed to init Doris resource: %v", err)
		return fmt.Errorf("failed to init Doris resource: %v", err)
	}

	// 9. 初始化 Doris 查询超时
	if err := i.initQueryTimeout(); err != nil {
		return fmt.Errorf("failed to init Doris query timeout: %v", err)
	}

	// 10. 初始化 Doris 工作组内存负载
	if err := i.initWorkloadGroupMemory(); err != nil {
		return err
	}

	// 11. 初始化gorm
	if err := i.initGorm(); err != nil {
		return fmt.Errorf("failed to init gorm: %v", err)
	}

	return nil
}

// 每个初始化方法都返回错误，便于错误处理
func (i *Initializer) initConfig() error {
	i.config = config.GetConfigMap()
	common.LoadCommonConfig(i.config)
	return nil
}

func (i *Initializer) initLogger() error {
	if i.config == nil {
		return errors.New("config not initialized")
	}
	log.InitLogger()
	return nil
}

func (i *Initializer) initDatabase() error {
	if i.config == nil {
		return errors.New("config not initialized")
	}
	database.Init()
	return nil
}

func (i *Initializer) initOSS() error {
	if i.config == nil {
		return errors.New("config not initialized")
	}

	factory := oss.NewOSSFactory(i.config)
	client, err := factory.NewOSSClient()
	if err != nil {
		return fmt.Errorf("failed to create OSS client: %v", err)
	}

	// 初始化 bucket
	if err := oss.InitializeBucket(client, i.config); err != nil {
		return fmt.Errorf("failed to initialize bucket: %v", err)
	}

	i.ossClient = client
	return nil
}

func (i *Initializer) initK8sService() error {
	k8sService, err := service.NewK8sService(log.Logger)
	if err != nil {
		return fmt.Errorf("failed to create K8sService: %v", err)
	}
	i.k8sService = k8sService
	return nil
}

func (i *Initializer) initTableInfoService() error {
	tableInfoService := service.NewTableInfoService(log.Logger)
	i.tableInfoService = tableInfoService
	return nil
}

func (i *Initializer) initDorisResource() error {
	dorisService, err := service.NewDorisService(common.MIRA_TMP_TASK_DB)
	if err != nil {
		return fmt.Errorf("failed to create Doris service: %v", err)
	}
	if err := dorisService.InitGlobalResource(); err != nil {
		return fmt.Errorf("failed to init Doris resource: %v", err)
	}
	return nil
}

func (i *Initializer) initGorm() error {
	if err := gorm.InitGormConnection(); err != nil {
		return fmt.Errorf("failed to init gorm connection: %v", err)
	}

	return nil
}

func (i *Initializer) initMiraTaskTmpDatabase() error {
	dorisService, err := service.NewDorisService("")
	if err != nil {
		return fmt.Errorf("failed to create Doris service: %v", err)
	}
	if err := dorisService.InitMiraTaskTmpDatabase(); err != nil {
		return fmt.Errorf("failed to init mira_task_tmp database: %v", err)
	}
	return nil
}

// initQueryTimeout 初始化 Doris 全局查询超时时间（单位：秒）
func (i *Initializer) initQueryTimeout() error {
	// 从配置中获取查询超时时间
	conf := config.GetConfigMap()
	timeoutSeconds := conf.DorisConfig.QueryTimeout

	// 如果配置中没有设置或设置为0，使用默认值1800秒
	if timeoutSeconds <= 0 {
		timeoutSeconds = 1800
	}

	dorisService, err := service.NewDorisService("") // 连接 Doris（默认数据库即可）
	if err != nil {
		return fmt.Errorf("failed to create Doris service: %v", err)
	}

	// 设置全局查询超时
	if _, err := dorisService.ExecuteUpdate(fmt.Sprintf("SET GLOBAL query_timeout = %d", timeoutSeconds)); err != nil {
		// 兼容性考虑：某些版本/权限限制可能不支持该设置，降级为日志告警但不阻断启动
		log.Logger.Warnf("Failed to set Doris GLOBAL query_timeout: %v", err)
		return nil
	}

	log.Logger.Infof("Doris GLOBAL query_timeout set to %d seconds", timeoutSeconds)
	return nil
}

// initWorkloadGroupMemory 初始化 Doris 工作组内存负载
func (i *Initializer) initWorkloadGroupMemory() error {
	// 工作组配置
	workloadGroup := "normal"
	memoryLimit := "80%"

	dorisService, err := service.NewDorisService("") // 连接 Doris（默认数据库即可）
	if err != nil {
		return fmt.Errorf("failed to create Doris service: %v", err)
	}

	// 设置工作组内存限制
	sql := fmt.Sprintf("ALTER WORKLOAD GROUP `%s` PROPERTIES (\"memory_limit\" = \"%s\")", workloadGroup, memoryLimit)
	if _, err := dorisService.ExecuteUpdate(sql); err != nil {
		// 兼容性考虑：某些版本/权限限制可能不支持该设置，降级为日志告警但不阻断启动
		log.Logger.Warnf("Failed to set Doris workload group memory limit: %v", err)
		return nil
	}

	log.Logger.Infof("Doris workload group '%s' memory_limit set to %s", workloadGroup, memoryLimit)
	return nil
}
