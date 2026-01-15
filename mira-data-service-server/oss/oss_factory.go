/*
*

	@author: shiliang
	@date: 2024/10/30
	@note:

*
*/
package oss

import (
	"data-service/config"
	"fmt"
)

type OSSFactory struct {
	conf *config.DataServiceConf
}

func NewOSSFactory(conf *config.DataServiceConf) *OSSFactory {
	return &OSSFactory{conf: conf}
}

// NewOSSClient 根据传入的类型返回相应的 OSS 客户端实例
func (f *OSSFactory) NewOSSClient() (ClientInterface, error) {
	endpoint := fmt.Sprintf("%s:%d", f.conf.OSSConfig.Host, f.conf.OSSConfig.Port)
	ossType := f.conf.OSSConfig.Type
	switch ossType {
	case "minio":
		return NewMinIOClient(endpoint, f.conf.OSSConfig.AccessKey, f.conf.OSSConfig.SecretKey, false)
	default:
		return nil, fmt.Errorf("unsupported OSS type: %s", ossType)
	}
}
