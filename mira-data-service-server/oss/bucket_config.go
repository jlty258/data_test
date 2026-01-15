/*
*

	@author: shiliang
	@date: 2024/11/25
	@note: bucket创建以及生命周期管理

*
*/
package oss

import (
	"context"
	"data-service/common"
	"data-service/config"
	"data-service/log"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

func InitializeBucket(client ClientInterface, conf *config.DataServiceConf) error {
	// 检查存储桶是否存在，如果不存在则创建
	bucketName := conf.SparkPodConfig.BucketName
	err := createBucketIfNotExists(client, bucketName)
	if err != nil {
		return err
	}

	// 创建 tls 证书 bucket
	err = createTlsCertBucket(client)
	if err != nil {
		log.Logger.Errorf("Failed to create tls cert bucket: %v", err)
		return err
	}

	// 创建 logs 和 data 文件夹
	directories := []string{common.SPARK_EVENT_LOG_PATH_PREFIX, common.SPARK_DATA_PATH_PREFIX}
	for _, dir := range directories {
		exists, errDir := client.DirectoryExists(bucketName, dir)
		if errDir != nil {
			return errDir
		}
		if exists {
			log.Logger.Infof("Folder %s already exists in bucket %s", dir, bucketName)
			continue
		}
		fileContent := uuid.New().String()
		fileName := fmt.Sprintf("%s/%s", dir, uuid.New().String())
		// 在 OSS 上创建文件夹可以通过上传一个空的对象来实现
		_, err := client.PutObject(context.Background(), bucketName, fileName, strings.NewReader(fileContent), int64(len(fileContent)), &PutOptions{ContentType: "application/octet-stream"})
		if err != nil {
			log.Logger.Errorf("Failed to create folder %s in bucket %s: %v", dir, bucketName, err)
			return err
		}
		log.Logger.Infof("Folder %s created in bucket %s", dir, bucketName)

	}

	// Set lifecycle rules for logs, data, and upload paths using the helper function
	lifecycleConfigs := []struct {
		pathPrefix    string
		ruleName      string
		retentionDays int
	}{
		{common.SPARK_EVENT_LOG_PATH_PREFIX, "spark-event-log-lifecycle-rule", conf.SparkPodConfig.LogRetentionDays},
		{common.SPARK_DATA_PATH_PREFIX, "spark-data-lifecycle-rule", conf.SparkPodConfig.DataRetentionDays},
		{common.SPARK_UPLOAD_PATH_PREFIX, "spark-file-lifecycle-rule", conf.SparkPodConfig.RetentionDays},
	}

	// 将所有规则一次性设置到桶上
	if err := setBucketLifecycle(client, bucketName, lifecycleConfigs); err != nil {
		log.Logger.Errorf("Failed to set lifecycle for bucket %s: %v", bucketName, err)
		return err
	}

	return nil
}

// createBucketIfNotExists 检查存储桶是否存在，如果不存在则创建
func createBucketIfNotExists(client ClientInterface, bucketName string) error {
	exists, err := client.BucketExists(bucketName)
	if err != nil {
		return err
	}
	if !exists {
		log.Logger.Infof("Bucket %s does not exist. Creating it now...", bucketName)
		err := client.MakeBucket(bucketName, "")
		if err != nil {
			return err
		}
		log.Logger.Infof("Bucket %s created successfully.", bucketName)
	} else {
		log.Logger.Infof("Bucket %s already exists.", bucketName)
	}
	return nil
}

func setBucketLifecycle(client ClientInterface, bucketName string, lifecycleConfigs []struct {
	pathPrefix    string
	ruleName      string
	retentionDays int
}) error {
	// 创建生命周期配置
	lifecycleConfig := lifecycle.NewConfiguration()

	for _, config := range lifecycleConfigs {
		expiration := lifecycle.Expiration{Days: lifecycle.ExpirationDays(config.retentionDays)}
		delMarkerExpiration := lifecycle.DelMarkerExpiration{Days: 1}

		lifecycleRule := lifecycle.Rule{
			ID:     config.ruleName,
			Status: "Enabled",
			RuleFilter: lifecycle.Filter{
				Prefix: config.pathPrefix, // 仅对指定前缀的对象生效
			},
			Expiration:          expiration,
			DelMarkerExpiration: delMarkerExpiration, // 设置删除标记过期
		}

		lifecycleConfig.Rules = append(lifecycleConfig.Rules, lifecycleRule)
	}

	// 应用生命周期配置到存储桶
	err := client.SetBucketLifecycle(bucketName, lifecycleConfig)
	if err != nil {
		return err
	}

	log.Logger.Infof("Lifecycle rules applied to bucket %s", bucketName)
	return nil
}

// 创建存放tls证书的bucket，并将桶设置为匿名可读mc anonymous set download
func createTlsCertBucket(client ClientInterface) error {
	err := createBucketIfNotExists(client, common.TLS_CERT_BUCKET_NAME)
	if err != nil {
		return err
	}
	return setBucketPublicReadWritePolicy(client, common.TLS_CERT_BUCKET_NAME)
}

// 设置匿名可读策略（等价于 mc anonymous set download）
func setBucketPublicReadWritePolicy(client ClientInterface, bucketName string) error {
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:GetBucketLocation","s3:ListBucket"],
      "Resource": ["arn:aws:s3:::%s"]
    },
    {
      "Effect": "Allow", 
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:GetObject","s3:PutObject"],
      "Resource": ["arn:aws:s3:::%s/*"]
    }
  ]
}`, bucketName, bucketName)

	return client.SetBucketPolicy(bucketName, policy)
}
