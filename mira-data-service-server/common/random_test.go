package common

import (
	"testing"
)

func TestGenerateRandomString(t *testing.T) {
	tests := []struct {
		name     string
		length   int
		validate func(string) bool
	}{
		{
			name:   "Generate random string with length 10",
			length: 10,
			validate: func(s string) bool {
				return len(s) == 10 && isHexString(s)
			},
		},
		{
			name:   "Generate random string with length 16",
			length: 16,
			validate: func(s string) bool {
				return len(s) == 16 && isHexString(s)
			},
		},
		{
			name:   "Generate random string with length 32",
			length: 32,
			validate: func(s string) bool {
				return len(s) == 32 && isHexString(s)
			},
		},
		{
			name:   "Generate random string with length 1",
			length: 1,
			validate: func(s string) bool {
				return len(s) == 1 && isHexString(s)
			},
		},
		{
			name:   "Generate random string with length 0",
			length: 0,
			validate: func(s string) bool {
				return len(s) == 0
			},
		},
		{
			name:   "Generate random string with odd length",
			length: 7,
			validate: func(s string) bool {
				return len(s) == 7 && isHexString(s)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GenerateRandomString(tt.length)
			if err != nil {
				t.Errorf("GenerateRandomString() error = %v", err)
				return
			}

			// 打印生成的字符串
			t.Logf("Generated string: '%s' (length: %d)", result, len(result))

			if !tt.validate(result) {
				t.Errorf("GenerateRandomString() = %v, validation failed", result)
			}
		})
	}
}

func TestGenerateRandomStringHexFormat(t *testing.T) {
	// 测试生成的字符串是否都是有效的十六进制字符
	length := 24
	result, err := GenerateRandomString(length)
	if err != nil {
		t.Errorf("GenerateRandomString() error = %v", err)
		return
	}

	if !isHexString(result) {
		t.Errorf("Generated string is not valid hex: %s", result)
	}
}

// 辅助函数：检查字符串是否为有效的十六进制字符串
func isHexString(s string) bool {
	if s == "" {
		return true
	}

	// 检查是否只包含十六进制字符 (0-9, a-f, A-F)
	for _, char := range s {
		if !((char >= '0' && char <= '9') ||
			(char >= 'a' && char <= 'f') ||
			(char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}
