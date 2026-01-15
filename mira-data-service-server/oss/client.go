/*
*

	@author: shiliang
	@date: 2024/12/16
	@note:

*
*/
package oss

// GetOptions 定义了通用的对象下载选项
type GetOptions struct {
	ExtraHeaders map[string]string // 指定额外的 HTTP 请求头
	QueryParams  map[string]string // 指定额外的查询参数
}

// PutOptions 定义通用的对象上传选项
type PutOptions struct {
	ContentType  string            // 文件内容类型 (e.g., "application/octet-stream")
	ExtraHeaders map[string]string // 额外的 HTTP 请求头
	Metadata     map[string]string // 用户自定义的元数据
	StorageClass string            // 存储类型 (e.g., "STANDARD", "IA", "ARCHIVE")
	Tagging      map[string]string // 对象标签 (key-value 形式)
}
