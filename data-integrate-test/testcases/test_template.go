package testcases

import (
	"gopkg.in/yaml.v3"
	"os"
)

// TestTemplate 测试模板
type TestTemplate struct {
	Name        string        `yaml:"name"`
	Description string        `yaml:"description"`
	Database    DatabaseConfig `yaml:"database"`
	Schema      SchemaConfig   `yaml:"schema"`
	Data        DataConfig     `yaml:"data"`
	Tests       []TestConfig   `yaml:"tests"`
}

type DatabaseConfig struct {
	Type string `yaml:"type"` // mysql, kingbase, gbase, vastbase
	Name string `yaml:"name"`
}

type SchemaConfig struct {
	FieldCount    int      `yaml:"field_count"`    // 1-16
	FieldTypes    []string `yaml:"field_types"`    // 可选
	MaxFieldSize  int      `yaml:"max_field_size"` // 最大1024
}

type DataConfig struct {
	RowCount    int64 `yaml:"row_count"`    // 1M, 50M, 100M
	UseSnapshot bool  `yaml:"use_snapshot"` // 是否使用快照
}

type TestConfig struct {
	Type      string                 `yaml:"type"`      // read, write, read_write
	Expected  int64                  `yaml:"expected"` // 期望的行数
	Tolerance float64                `yaml:"tolerance"` // 允许误差（百分比，默认0.1%）
	Params    map[string]interface{} `yaml:"params"`
}

func LoadTemplate(path string) (*TestTemplate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	
	var template TestTemplate
	if err := yaml.Unmarshal(data, &template); err != nil {
		return nil, err
	}
	
	// 设置默认值
	for i := range template.Tests {
		if template.Tests[i].Tolerance == 0 {
			template.Tests[i].Tolerance = 0.1 // 默认0.1%误差
		}
	}
	
	return &template, nil
}

