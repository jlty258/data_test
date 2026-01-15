/*
*

	@author: shiliang
	@date: 2025/08/25
	@note: 运行时工具函数

*
*/
package common

import (
	"data-service/log"
	"runtime"
)

// GetDorisStreamloaderPath 根据操作系统架构返回正确的doris-streamloader路径
func GetDorisStreamloaderPath() string {
	log.Logger.Infof("Current architecture: %s", runtime.GOARCH)
	switch runtime.GOARCH {
	case "arm64":
		return "/home/workspace/bin/doris-streamloader-arm"
	case "arm":
		return "/home/workspace/bin/doris-streamloader-arm"
	default:
		// 对于其他架构，使用默认的通用版本
		return "/home/workspace/bin/doris-streamloader"
	}
}
