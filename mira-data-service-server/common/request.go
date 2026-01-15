/**
 * @Author: xueyanghan
 * @File: base.go
 * @Version: 1.0.0
 * @Description: desc.
 * @Date: 2023/5/23 17:59
 */

package common

type BaseRequest struct {
	RequestId   string `json:"RequestId"`
	UserId      string `json:"UserId"`
	ChainInfoId uint32 `json:"ChainInfoId"`
	ChainName   string `json:"ChainName"`
}

const (
	RequestIdKey   = "X-Request-Id"
	UserIdKey      = "X-UserId"
	ChainInfoIdKey = "X-Chain-Info-Id"
)

type Header struct {
	RequestId   string `json:"RequestId"`
	UserId      string `json:"UserId"`
	ChainInfoId uint32 `json:"ChainInfoId"`
}
