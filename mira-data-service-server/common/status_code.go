package common

const (
	// ErrCodeOK -  Default error codes
	ErrCodeOK      = 200
	ErrCodeUnknown = 69999

	// ErrCodeInvalidParameter - Common error codes: 60000 - 60099
	ErrCodeInvalidParameter      = 60000
	ErrCodeInvalidParameterValue = 60001
	ErrCodeMissingParameter      = 60002
	ErrCodeUnknownParameter      = 60003
	ErrCodeInternalError         = 60004
	ErrCodeProtoToBean           = 60005
	ErrorJobNameExisted          = 60006
	ErrCodeNameIsEmpty           = 60007
	ErrCodeFileSizeLimit         = 60008
	ErrCodeNameExist             = 60009
	ErrCodeMethodNameExist       = 600010
	ErrCodeFileIsEmpty           = 600011

	// 数据源相关错误码 (60100-60199)
	ErrCodeDataSourceNotFound         = 60100
	ErrCodeDataSourceConnectionFailed = 60101
	ErrCodeDataSourceQueryFailed      = 60102
	ErrCodeDataSourceTypeNotSupport   = 60103

	// CSV处理相关错误码 (60200-60299)
	ErrCodeCSVParseFailed           = 60200
	ErrCodeCSVColumnMismatch        = 60201
	ErrCodeCSVDataTypeConvertFailed = 60202
	ErrCodeCSVFileNotFound          = 60203

	// Arrow相关错误码 (60300-60399)
	ErrCodeArrowSchemaInvalid     = 60300
	ErrCodeArrowRecordBuildFailed = 60301
	ErrCodeArrowTypeConvertFailed = 60302

	// Doris相关错误码 (60400-60499)
	ErrCodeDorisTableCreateFailed = 60400
	ErrCodeDorisDataImportFailed  = 60401
	ErrCodeDorisConnectionFailed  = 60402

	// OSS/MinIO相关错误码 (60500-60599)
	ErrCodeOSSFileNotFound     = 60500
	ErrCodeOSSFileUploadFailed = 60501
	ErrCodeOSSFileDeleteFailed = 60502
	ErrCodeOSSStreamReadFailed = 60503

	// 保留被使用的错误码
	ErrGetMinioResult = 63024
)

const (
	ErrNoSuchFile = "no such file"
)

var ErrCodeMessage = map[int]string{
	// 通用错误信息
	ErrCodeOK:                    "成功",
	ErrCodeUnknown:               "错误未识别",
	ErrCodeInvalidParameter:      "参数错误（包括参数格式、类型等错误）",
	ErrCodeInvalidParameterValue: "参数取值错误",
	ErrCodeMissingParameter:      "缺少参数错误，必传参数没填",
	ErrCodeUnknownParameter:      "未知参数错误，用户多传未定义的参数会导致错误",
	ErrCodeInternalError:         "网络错误",
	ErrCodeProtoToBean:           "Proto转Bean失败",
	ErrorJobNameExisted:          "任务名称已存在",
	ErrCodeNameIsEmpty:           "名称不能为空",
	ErrCodeFileSizeLimit:         "文件大小超过限制",
	ErrCodeNameExist:             "名称已存在",
	ErrCodeMethodNameExist:       "方法名称已存在",
	ErrCodeFileIsEmpty:           "文件为空",

	ErrCodeDataSourceNotFound:         "数据源未找到",
	ErrCodeDataSourceConnectionFailed: "数据源连接失败",
	ErrCodeDataSourceQueryFailed:      "数据源查询失败",
	ErrCodeDataSourceTypeNotSupport:   "不支持的数据源类型",

	ErrCodeCSVParseFailed:           "CSV解析失败",
	ErrCodeCSVColumnMismatch:        "CSV列数不匹配",
	ErrCodeCSVDataTypeConvertFailed: "CSV数据类型转换失败",
	ErrCodeCSVFileNotFound:          "CSV文件未找到",

	ErrCodeArrowSchemaInvalid:     "Arrow Schema无效",
	ErrCodeArrowRecordBuildFailed: "Arrow Record构建失败",
	ErrCodeArrowTypeConvertFailed: "Arrow类型转换失败",

	ErrCodeDorisTableCreateFailed: "Doris表创建失败",
	ErrCodeDorisDataImportFailed:  "Doris数据导入失败",
	ErrCodeDorisConnectionFailed:  "Doris连接失败",

	ErrCodeOSSFileNotFound:     "OSS文件未找到",
	ErrCodeOSSFileUploadFailed: "OSS文件上传失败",
	ErrCodeOSSFileDeleteFailed: "OSS文件删除失败",
	ErrCodeOSSStreamReadFailed: "OSS读取文件失败",

	// 保留被使用的错误信息
	ErrGetMinioResult: "获取结果失败",
}
