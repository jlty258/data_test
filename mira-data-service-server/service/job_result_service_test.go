package service

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransformHardwareFormat(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []string
		expected string
		wantErr  bool
	}{
		{
			name:     "空输入",
			inputs:   []string{""},
			expected: "[]",
			wantErr:  false,
		},
		{
			name:     "单个对象带JSON数组字符串",
			inputs:   []string{`{"t":[{"k":20,"v":400},{"k":19,"v":361},{"k":2,"v":4},{"k":1,"v":1}]}`},
			expected: `[{"t":{"k":20,"v":400}},{"t":{"k":19,"v":361}},{"t":{"k":2,"v":4}},{"t":{"k":1,"v":1}}]`,
			wantErr:  false,
		},
		{
			name:     "多个字段的对象",
			inputs:   []string{`{"id": ["1", "2", "3"], "username": ["Admin", "test1", "test3"], "values": [{"k":20,"v":400},{"k":19,"v":361},{"k":2,"v":4}]}`},
			expected: `[{"id":"1","username":"Admin","values":{"k":20,"v":400}},{"id":"2","username":"test1","values":{"k":19,"v":361}},{"id":"3","username":"test3","values":{"k":2,"v":4}}]`,
			wantErr:  false,
		},
		{
			name:     "原始数组格式",
			inputs:   []string{`[{"id":"1","val":"test"},{"id":"2","val":"test2"}]`},
			expected: `[{"id":"1","val":"test"},{"id":"2","val":"test2"}]`,
			wantErr:  false,
		},
		{
			name:     "无效JSON",
			inputs:   []string{`{invalid json}`},
			expected: "",
			wantErr:  true,
		},
		{
			name:     "对象中的非JSON字符串",
			inputs:   []string{`{"t":"not a json array"}`},
			expected: "[]",
			wantErr:  false,
		},
		{
			name:     "多个输入",
			inputs:   []string{`{"t":[{"k":1,"v":1}]}`, `{"t":[{"k":2,"v":4}]}`},
			expected: `[{"t":{"k":1,"v":1}},{"t":{"k":2,"v":4}}]`,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TransformHardwareFormat(tt.inputs)

			// 检查错误
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// 为了比较JSON，我们需要标准化格式（去除空格差异等）
			var expectedObj interface{}
			var resultObj interface{}

			err = json.Unmarshal([]byte(tt.expected), &expectedObj)
			assert.NoError(t, err, "预期结果应该是有效的JSON")

			err = json.Unmarshal([]byte(result), &resultObj)
			assert.NoError(t, err, "函数返回结果应该是有效的JSON")

			// 比较标准化后的JSON
			assert.Equal(t, expectedObj, resultObj, "转换结果与预期不符")
		})
	}
}
