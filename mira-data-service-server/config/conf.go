/*
*

	@author: shiliang
	@date: 2024/9/11
	@note: 读取k8s configmap的配置

*
*/
package config

import (
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
)

// 全局配置管理器
var (
	configManager *ConfigManager
	once          sync.Once
)

// ConfigManager 配置管理器
type ConfigManager struct {
	config     *DataServiceConf
	configPath string
	mu         sync.RWMutex
}

// NewConfigManager 创建配置管理器
func NewConfigManager(configPath string) *ConfigManager {
	cm := &ConfigManager{
		configPath: configPath,
	}
	// 初始加载配置
	if err := cm.reloadConfig(); err != nil {
		log.Fatalf("Failed to load initial config: %v", err)
	}
	// 启动配置监听
	go cm.watchConfig()
	return cm
}

// watchConfig 监听配置文件变化
func (cm *ConfigManager) watchConfig() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		if err := cm.reloadConfig(); err != nil {
			log.Printf("Failed to reload config: %v", err)
		}
	}
}

// reloadConfig 重新加载配置
func (cm *ConfigManager) reloadConfig() error {
	var configData []byte
	var err error

	if viper.GetString("TestConfig") == "" {
		configData, err = ioutil.ReadFile("/home/workspace/config/config.yaml")
	} else {
		configData, err = ioutil.ReadFile("config.yaml")
	}

	if err != nil {
		return err
	}

	newConfig := &DataServiceConf{}
	if err := yaml.Unmarshal(configData, newConfig); err != nil {
		return err
	}

	cm.mu.Lock()
	cm.config = newConfig
	cm.mu.Unlock()

	return nil
}

// GetConfig 获取当前配置
func (cm *ConfigManager) GetConfig() *DataServiceConf {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.config
}

type DataServiceConf struct {
	OSSConfig         OSSConfig         `yaml:"oss"`
	Dbms              DbmsConfig        `yaml:"dbms"`
	HttpServiceConfig HttpServiceConfig `yaml:"http"`
	SparkPodConfig    SparkPodConfig    `yaml:"spark"`
	LoggerConfig      LoggerConfig      `yaml:"log"`
	CleanupConfig     CleanupConfig     `yaml:"clean_table"`
	CommonConfig      CommonConfig      `yaml:"common"`
	RedisConfig       RedisConfig       `yaml:"redis"`
	DorisConfig       DorisConfig       `yaml:"doris"`
	StreamConfig      StreamConfig      `yaml:"stream"`
}

type DbmsConfig struct {
	Type              string  `yaml:"type"`
	Params            string  `yaml:"params"`
	Host              string  `yaml:"host"`
	Port              int32   `yaml:"port"`
	User              string  `yaml:"user"`
	Password          string  `yaml:"password"`
	Database          string  `yaml:"db"`
	dsn               string  `yaml:"dsn"`
	MaxOpenConns      int     `yaml:"max_open_conns"`
	MaxIdleConns      int     `yaml:"max_idle_conns"`
	StreamDataSize    int     `yaml:"stream_data_size"`    // 流式接口一次返回的数据量
	BatchDataSize     int     `yaml:"batch_data_size"`     // 一次insert数据库的数据量
	UseEstimationOnly bool    `yaml:"use_estimation_only"` // 是否使用估算方式获取表大小
	TableSizeFactor   float64 `yaml:"table_size_factor"`   // 表大小估算系数
}

// RedisConfig Redis配置结构
type RedisConfig struct {
	Address            string `yaml:"address"`              // Redis服务地址 ip:port
	Password           string `yaml:"password"`             // Redis密码
	DB                 int    `yaml:"db"`                   // Redis数据库
	ClusterType        string `yaml:"cluster_type"`         // Redis部署模式：standalone/sentinel/cluster
	SentinelMasterName string `yaml:"sentinel_master_name"` // Redis哨兵master名称
}

type DorisConfig struct {
	Address         string `yaml:"address"`
	Port            int32  `yaml:"port"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Database        string `yaml:"db"`
	BatchInsertSize int32  `yaml:"batch_insert_size"`
	// 数据库连接池配置
	MaxOpenConns int `yaml:"max_open_conns"` // 数据库最大连接数
	MaxIdleConns int `yaml:"max_idle_conns"` // 数据库最大空闲连接数
	MaxLifeTime  int `yaml:"max_life_time"`  // 连接最大生存时间（分钟）
	MaxIdleTime  int `yaml:"max_idle_time"`  // 空闲连接最大空闲时间（分钟）
	QueryTimeout int `yaml:"query_timeout"`  // 查询超时时间（秒）
	// 导入相关配置
	ImportBatchSize int `yaml:"import_batch_size"` // 分批导入每批的条数
	ImportMaxRetry  int `yaml:"import_max_retry"`  // 导入失败重试次数
	// s3导出配置
	S3ExportMaxFileSize       string `yaml:"s3_export_max_file_size"`      // 单个导出文件大小
	S3ExportRequestTimeout    int    `yaml:"s3_export_request_timeout"`    // 请求超时时间（秒）
	S3ExportConnectionTimeout int    `yaml:"s3_export_connection_timeout"` // 连接超时时间（秒）
	S3ExportConnectionMaximum int    `yaml:"s3_export_connection_maximum"` // 最大并发连接数
}

type OSSConfig struct {
	Type      string `yaml:"type"`
	Host      string `yaml:"host"`
	Port      int32  `yaml:"port"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type HttpServiceConfig struct {
	Port       int32  `yaml:"port"`
	DataServer string `yaml:"data_server"`
}

type SparkPodConfig struct {
	AccountName           string                `yaml:"account_name"`
	Namespace             string                `yaml:"namespace"`
	UploadPath            string                `yaml:"upload_path"`
	ImageName             string                `yaml:"image_name"`
	ImageTag              string                `yaml:"image_tag"`
	Master                string                `yaml:"master"`
	EventLogEnable        bool                  `yaml:"event_log_enable"`
	EventLogDir           string                `yaml:"event_log_dir"`
	CleanInterval         int                   `yaml:"clean_interval"`
	BucketName            string                `yaml:"bucket_name"`
	RetentionDays         int                   `yaml:"retention_days"`
	LogRetentionDays      int                   `yaml:"log_retention_days"`
	DataRetentionDays     int                   `yaml:"data_retention_days"`
	NodeSelector          map[string]string     `yaml:"node_selector"`
	Tolerations           []corev1.Toleration   `yaml:"tolerations"`
	SparkExecutionConfigs SparkExecutionConfigs `yaml:"execution_configs"`
}

type SparkExecutionConfigs struct {
	SmallData  SparkExecutionConfig `yaml:"small_data"`
	MediumData SparkExecutionConfig `yaml:"medium_data"`
	LargeData  SparkExecutionConfig `yaml:"large_data"`
	XLargeData SparkExecutionConfig `yaml:"xlarge_data"`
}

type SparkExecutionConfig struct {
	RecordCountThreshold          int64 `yaml:"record_count_threshold"`
	DynamicAllocationEnabled      bool  `yaml:"dynamic_allocation_enabled"`
	DynamicAllocationMinExecutors int32 `yaml:"dynamic_allocation_min_executors"`
	DynamicAllocationMaxExecutors int32 `yaml:"dynamic_allocation_max_executors"`
	ExecutorMemoryMB              int32 `yaml:"executor_memory_mb"`
	ExecutorCores                 int32 `yaml:"executor_cores"`
	DriverMemoryMB                int32 `yaml:"driver_memory_mb"`
	DriverCores                   int32 `yaml:"driver_cores"`
	Parallelism                   int32 `yaml:"parallelism"`
	NumPartitions                 int32 `yaml:"num_partitions"`
}

type LoggerConfig struct {
	Level string `yaml:"level"`
}

type CleanupConfig struct {
	Enable        bool `yaml:"clean_enable"`
	RetentionDays int  `yaml:"retention_days"`
	Interval      int  `yaml:"interval"`
}

type StreamConfig struct {
	BatchLines       int `yaml:"batch_lines"`
	ParquetBatchSize int `yaml:"parquet_batch_size"`
}

type CommonConfig struct {
	Port           int   `yaml:"port"`
	MonitorPort    int32 `yaml:"monitorPort"`
	MonitorEnabled bool  `yaml:"monitorEnabled"`
}

// GetConfigMap 获取配置（保持原有函数名以兼容现有代码）
func GetConfigMap() *DataServiceConf {
	once.Do(func() {
		configPath := "/home/workspace/config/config.yaml"
		if viper.GetString("TestConfig") != "" {
			configPath = "config.yaml"
		}
		configManager = NewConfigManager(configPath)
	})
	return configManager.GetConfig()
}
