package common

import (
	"crypto/rand"
	"encoding/hex"
)

// GenerateRandomString 生成一个指定长度的随机字符串
func GenerateRandomString(length int) (string, error) {
	byteLength := (length + 1) / 2 // 每个字节可以生成两个十六进制字符

	// 创建一个随机字节数组
	bytes := make([]byte, byteLength)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	// 将字节数组编码为十六进制字符串
	randomString := hex.EncodeToString(bytes)

	// 截取到指定长度
	if len(randomString) > length {
		randomString = randomString[:length]
	}

	return randomString, nil
}
