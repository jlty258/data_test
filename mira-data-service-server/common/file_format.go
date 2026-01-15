/*
*

	@author: shiliang
	@date: 2025/08/10
	@note: 文件格式枚举及相关功能

*
*/
package common

// FileFormat 文件格式枚举
type FileFormat string

const (
	// 数据格式
	FILE_FORMAT_ARROW   FileFormat = "arrow"   // Apache Arrow 格式
	FILE_FORMAT_CSV     FileFormat = "csv"     // CSV 格式
	FILE_FORMAT_JSON    FileFormat = "json"    // JSON 格式
	FILE_FORMAT_PARQUET FileFormat = "parquet" // Parquet 格式
	FILE_FORMAT_ORC     FileFormat = "orc"     // ORC 格式
	FILE_FORMAT_AVRO    FileFormat = "avro"    // Avro 格式

	// 表格格式
	FILE_FORMAT_EXCEL FileFormat = "excel" // Excel 格式
	FILE_FORMAT_XLS   FileFormat = "xls"   // Excel 97-2003 格式
	FILE_FORMAT_XLSX  FileFormat = "xlsx"  // Excel 2007+ 格式

	// 标记语言格式
	FILE_FORMAT_XML  FileFormat = "xml"  // XML 格式
	FILE_FORMAT_YAML FileFormat = "yaml" // YAML 格式
	FILE_FORMAT_YML  FileFormat = "yml"  // YAML 格式（短扩展名）

	// 文本格式
	FILE_FORMAT_TXT FileFormat = "txt" // 纯文本格式
	FILE_FORMAT_TSV FileFormat = "tsv" // Tab分隔值格式

	// 压缩格式
	FILE_FORMAT_GZIP FileFormat = "gzip" // Gzip 压缩
	FILE_FORMAT_ZIP  FileFormat = "zip"  // ZIP 压缩
	FILE_FORMAT_TAR  FileFormat = "tar"  // TAR 归档

	// 其他格式
	FILE_FORMAT_BINARY  FileFormat = "binary"  // 二进制格式
	FILE_FORMAT_UNKNOWN FileFormat = "unknown" // 未知格式
)

// IsValidFileFormat 检查文件格式是否有效
func (ff FileFormat) IsValidFileFormat() bool {
	switch ff {
	case FILE_FORMAT_ARROW, FILE_FORMAT_CSV, FILE_FORMAT_JSON, FILE_FORMAT_PARQUET,
		FILE_FORMAT_ORC, FILE_FORMAT_AVRO, FILE_FORMAT_EXCEL, FILE_FORMAT_XLS, FILE_FORMAT_XLSX,
		FILE_FORMAT_XML, FILE_FORMAT_YAML, FILE_FORMAT_YML, FILE_FORMAT_TXT, FILE_FORMAT_TSV,
		FILE_FORMAT_GZIP, FILE_FORMAT_ZIP, FILE_FORMAT_TAR, FILE_FORMAT_BINARY:
		return true
	default:
		return false
	}
}
