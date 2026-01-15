package config

import (
	"fmt"
	"os"
	"gopkg.in/yaml.v3"
)

type TestConfig struct {
	DataService struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"data_service"`
	
	MockServices struct {
		Gateway struct {
			Host string `yaml:"host"`
			Port int    `yaml:"port"`
		} `yaml:"gateway"`
		IDA struct {
			Host string `yaml:"host"`
			Port int    `yaml:"port"`
		} `yaml:"ida"`
	} `yaml:"mock_services"`
	
	Databases []DatabaseConfig `yaml:"databases"`
	MinIO    MinIOConfig      `yaml:"minio"`
}

type DatabaseConfig struct {
	Type     string `yaml:"type"` // mysql, kingbase, gbase, vastbase
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type MinIOConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Bucket    string `yaml:"bucket"`
}

func LoadConfig(path string) (*TestConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	
	var config TestConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	
	return &config, nil
}

func (c *TestConfig) GetDatabaseConfig(dbType string) (*DatabaseConfig, error) {
	for _, db := range c.Databases {
		if db.Type == dbType {
			return &db, nil
		}
	}
	return nil, fmt.Errorf("未找到数据库配置: %s", dbType)
}

