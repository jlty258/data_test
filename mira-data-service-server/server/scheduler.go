package main

import (
	"data-service/config"
)

// Schedule 启动所有定时任务
func Schedule(conf *config.DataServiceConf) {
	// 启动临时文件清理任务
	tempManager := GetManager()
	go tempManager.StartCleanupTask()

	// 启动 Spark Pod 清理任务
	// go utils.StartPodCleanupTask(conf.SparkPodConfig.Namespace, time.Duration(conf.SparkPodConfig.CleanInterval)*time.Hour)

}
