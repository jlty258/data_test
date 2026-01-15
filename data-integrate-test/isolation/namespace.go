package isolation

import (
	"fmt"
	"time"
)

// NamespaceManager 命名空间管理器（借鉴SQLMesh环境隔离思想）
type NamespaceManager struct {
	prefix string
}

func NewNamespaceManager(prefix string) *NamespaceManager {
	return &NamespaceManager{prefix: prefix}
}

// GenerateNamespace 生成唯一命名空间
func (nm *NamespaceManager) GenerateNamespace(testName string) string {
	timestamp := time.Now().Unix()
	random := fmt.Sprintf("%08x", time.Now().UnixNano()%0xFFFFFFFF)
	return fmt.Sprintf("%s_%s_%d_%s", nm.prefix, testName, timestamp, random)
}

// GenerateTableName 生成表名（带命名空间）
func (nm *NamespaceManager) GenerateTableName(namespace, baseName string) string {
	return fmt.Sprintf("%s_%s", namespace, baseName)
}

// GenerateAssetName 生成资产名（带命名空间）
func (nm *NamespaceManager) GenerateAssetName(namespace, baseName string) string {
	return fmt.Sprintf("%s_%s", namespace, baseName)
}

