/*
*

	@author: shiliang
	@date: 2025/08/21
	@note: 证书格式转换工具

*
*/
package service

import (
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"data-service/common"
	"data-service/log"
)

// CertificateConverter 证书转换器
type CertificateConverter struct {
	tempDir string
}

// NewCertificateConverter 创建新的证书转换器
func NewCertificateConverter(requestId string) (*CertificateConverter, error) {
	// 使用Doris的small_files目录
	tempDir := filepath.Join("/opt/apache-doris/small_files", "tls_certs", requestId)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %v", err)
	}

	return &CertificateConverter{
		tempDir: tempDir,
	}, nil
}

// ConvertToPKCS12 将PEM证书转换为PKCS12格式
func (c *CertificateConverter) ConvertToPKCS12(caCert, clientCert, clientKey string) (string, string, error) {
	var caP12Path, clientP12Path string
	var err error

	// 转换CA证书
	if caCert != "" {
		caP12Path, err = c.convertCACertToPKCS12(caCert)
		if err != nil {
			return "", "", fmt.Errorf("failed to convert CA certificate: %v", err)
		}
	}

	// 转换客户端证书和私钥
	if clientCert != "" && clientKey != "" {
		clientP12Path, err = c.convertClientCertToPKCS12(clientCert, clientKey)
		if err != nil {
			return "", "", fmt.Errorf("failed to convert client certificate: %v", err)
		}
	}

	return caP12Path, clientP12Path, nil
}

// convertCACertToPKCS12 使用 keytool 将 PEM 证书链导入为 PKCS12 truststore（含 trustedCertEntry）
func (c *CertificateConverter) convertCACertToPKCS12(base64Cert string) (string, error) {
	// 解码 base64 -> PEM 文本
	certData, err := base64.StdEncoding.DecodeString(base64Cert)
	if err != nil {
		return "", fmt.Errorf("failed to decode CA certificate: %v", err)
	}

	// 解析出所有 CERTIFICATE block（支持根/中间链）
	var blocks []*pem.Block
	rest := certData
	for {
		var b *pem.Block
		b, rest = pem.Decode(rest)
		if b == nil {
			break
		}
		if b.Type == "CERTIFICATE" {
			blocks = append(blocks, b)
		}
	}
	if len(blocks) == 0 {
		// 回退：直接按单 PEM 文件导入
		tmpPem := filepath.Join(c.tempDir, "ca.pem")
		if err := os.WriteFile(tmpPem, certData, 0644); err != nil {
			return "", fmt.Errorf("failed to write CA PEM file: %v", err)
		}
		caP12Path := filepath.Join(c.tempDir, "ca.p12")
		cmd := exec.Command("keytool", "-importcert",
			"-storetype", "PKCS12",
			"-keystore", caP12Path,
			"-storepass", common.TLS_KEYSTORE_PASSWORD,
			"-alias", "root",
			"-file", tmpPem,
			"-noprompt")
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("failed to import CA to PKCS12 via keytool: %v", err)
		}
		log.Logger.Infof("Successfully converted CA certificate to PKCS12: %s", caP12Path)
		return caP12Path, nil
	}

	// 多段证书链逐段导入（keytool 会在首次导入时自动创建 keystore）
	caP12Path := filepath.Join(c.tempDir, "ca.p12")
	for i, b := range blocks {
		tmp := filepath.Join(c.tempDir, fmt.Sprintf("ca_%d.pem", i))
		if err := os.WriteFile(tmp, pem.EncodeToMemory(b), 0644); err != nil {
			return "", fmt.Errorf("failed to write chain PEM %d: %v", i, err)
		}
		alias := fmt.Sprintf("ca%d", i)
		cmd := exec.Command("keytool", "-importcert",
			"-storetype", "PKCS12",
			"-keystore", caP12Path,
			"-storepass", common.TLS_KEYSTORE_PASSWORD,
			"-alias", alias,
			"-file", tmp,
			"-noprompt")
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("failed to import chain cert %d to PKCS12: %v", i, err)
		}
	}

	log.Logger.Infof("Successfully converted CA certificate chain to PKCS12: %s (entries=%d)", caP12Path, len(blocks))
	return caP12Path, nil
}

// convertClientCertToPKCS12 转换客户端证书和私钥为PKCS12
func (c *CertificateConverter) convertClientCertToPKCS12(base64Cert, base64Key string) (string, error) {
	// 解码证书
	certData, err := base64.StdEncoding.DecodeString(base64Cert)
	if err != nil {
		return "", fmt.Errorf("failed to decode client certificate: %v", err)
	}

	// 解码私钥
	keyData, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return "", fmt.Errorf("failed to decode client key: %v", err)
	}

	// 写入临时文件
	clientCertPemPath := filepath.Join(c.tempDir, "client.pem")
	clientKeyPemPath := filepath.Join(c.tempDir, "client_key.pem")

	if err := os.WriteFile(clientCertPemPath, certData, 0644); err != nil {
		return "", fmt.Errorf("failed to write client cert PEM file: %v", err)
	}
	if err := os.WriteFile(clientKeyPemPath, keyData, 0600); err != nil {
		return "", fmt.Errorf("failed to write client key PEM file: %v", err)
	}

	// 转换为PKCS12
	clientP12Path := filepath.Join(c.tempDir, "client.p12")
	cmd := exec.Command("openssl", "pkcs12", "-export",
		"-in", clientCertPemPath,
		"-inkey", clientKeyPemPath,
		"-out", clientP12Path,
		"-passout", "pass:"+common.TLS_KEYSTORE_PASSWORD)

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to convert client cert to PKCS12: %v", err)
	}

	log.Logger.Infof("Successfully converted client certificate to PKCS12: %s", clientP12Path)
	return clientP12Path, nil
}

// ConvertToJKS 将PEM证书转换为JKS格式（用于Java应用）
func (c *CertificateConverter) ConvertToJKS(caCert, clientCert, clientKey string) (string, string, error) {
	// 先转换为PKCS12
	caP12Path, clientP12Path, err := c.ConvertToPKCS12(caCert, clientCert, clientKey)
	if err != nil {
		return "", "", err
	}

	var caJksPath, clientJksPath string

	// 转换CA证书为JKS
	if caP12Path != "" {
		caJksPath = filepath.Join(c.tempDir, "ca.jks")
		cmd := exec.Command("keytool", "-importkeystore",
			"-srckeystore", caP12Path,
			"-srcstoretype", "PKCS12",
			"-srcstorepass", common.TLS_KEYSTORE_PASSWORD,
			"-destkeystore", caJksPath,
			"-deststoretype", "JKS",
			"-deststorepass", common.TLS_KEYSTORE_PASSWORD)

		if err := cmd.Run(); err != nil {
			return "", "", fmt.Errorf("failed to convert CA cert to JKS: %v", err)
		}
	}

	// 转换客户端证书为JKS
	if clientP12Path != "" {
		clientJksPath = filepath.Join(c.tempDir, "client.jks")
		cmd := exec.Command("keytool", "-importkeystore",
			"-srckeystore", clientP12Path,
			"-srcstoretype", "PKCS12",
			"-srcstorepass", common.TLS_KEYSTORE_PASSWORD,
			"-destkeystore", clientJksPath,
			"-deststoretype", "JKS",
			"-deststorepass", common.TLS_KEYSTORE_PASSWORD)

		if err := cmd.Run(); err != nil {
			return "", "", fmt.Errorf("failed to convert client cert to JKS: %v", err)
		}
	}

	return caJksPath, clientJksPath, nil
}

// Cleanup 清理临时文件
func (c *CertificateConverter) Cleanup() error {
	return os.RemoveAll(c.tempDir)
}

// GetTempDir 获取临时目录路径
func (c *CertificateConverter) GetTempDir() string {
	return c.tempDir
}
