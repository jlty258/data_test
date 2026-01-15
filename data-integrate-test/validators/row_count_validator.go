package validators

import (
	"context"
	"database/sql"
	"fmt"
	"math"
)

// RowCountValidator 行数验证器（核心验证功能）
type RowCountValidator struct {
	expected  int64
	tolerance float64 // 允许的误差范围（百分比）
}

func NewRowCountValidator(expected int64, tolerance float64) *RowCountValidator {
	return &RowCountValidator{
		expected:  expected,
		tolerance: tolerance,
	}
}

// ValidationResult 验证结果
type ValidationResult struct {
	Operation   string
	Expected    int64
	Actual      int64
	Diff        int64
	DiffPercent float64
	Passed      bool
	Message     string
}

// ValidateReadResult 验证读取结果的行数
func (v *RowCountValidator) ValidateReadResult(
	ctx context.Context,
	actualCount int64,
) (*ValidationResult, error) {
	return v.validate(actualCount, "读取")
}

// ValidateWriteResult 验证写入结果的行数
func (v *RowCountValidator) ValidateWriteResult(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) (*ValidationResult, error) {
	// 查询数据库实际行数
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var actualCount int64
	err := db.QueryRowContext(ctx, query).Scan(&actualCount)
	if err != nil {
		return nil, fmt.Errorf("查询行数失败: %v", err)
	}
	
	return v.validate(actualCount, "写入")
}

// validate 执行验证
func (v *RowCountValidator) validate(actual int64, operation string) (*ValidationResult, error) {
	diff := actual - v.expected
	var diffPercent float64
	if v.expected > 0 {
		diffPercent = float64(diff) / float64(v.expected) * 100
	}
	
	// 检查是否在允许的误差范围内
	passed := math.Abs(diffPercent) <= v.tolerance
	
	result := &ValidationResult{
		Operation:   operation,
		Expected:    v.expected,
		Actual:      actual,
		Diff:        diff,
		DiffPercent: diffPercent,
		Passed:      passed,
	}
	
	if !passed {
		result.Message = fmt.Sprintf(
			"%s行数不匹配: 期望 %d, 实际 %d, 差异 %d (%.2f%%)",
			operation, v.expected, actual, diff, diffPercent,
		)
	} else {
		result.Message = fmt.Sprintf(
			"%s行数验证通过: 期望 %d, 实际 %d",
			operation, v.expected, actual,
		)
	}
	
	return result, nil
}

