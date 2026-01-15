/*
*

	@author: shiliang
	@date: 2024/10/30
	@note:

*
*/
package oss

import (
	"context"
	"data-service/log"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

type MinIOClient struct {
	client *minio.Client
}

// NewMinIOClient 创建一个新的 MinIO 客户端
func NewMinIOClient(endpoint, accessKey, secretKey string, secure bool) (*MinIOClient, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %v", err)
	}
	return &MinIOClient{client: client}, nil
}

func (c *MinIOClient) GetObject(ctx context.Context, bucketName, objectName string, opts *GetOptions) (io.ReadCloser, error) {
	// 初始化 MinIO 的 GetObjectOptions
	minioOpts := minio.GetObjectOptions{}

	if opts != nil {
		if opts.ExtraHeaders != nil {
			for key, value := range opts.ExtraHeaders {
				minioOpts.Set(key, value)
			}
		}

		// 设置查询参数
		if opts.QueryParams != nil {
			for key, value := range opts.QueryParams {
				minioOpts.AddReqParam(key, value)
			}
		}
	}

	return c.client.GetObject(ctx, bucketName, objectName, minioOpts)
}

func (c *MinIOClient) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts *PutOptions) (info interface{}, err error) {
	minioOpts := minio.PutObjectOptions{
		ContentType: opts.ContentType,
	}
	return c.client.PutObject(ctx, bucketName, objectName, reader, objectSize, minioOpts)
}

func (c *MinIOClient) BucketExists(bucketName string) (bool, error) {
	exists, err := c.client.BucketExists(context.Background(), bucketName)
	return exists, err
}

func (c *MinIOClient) MakeBucket(bucketName, location string) error {
	err := c.client.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{
		Region: location,
	})
	return err
}

func (c *MinIOClient) SetBucketLifecycle(bucketName string, lifecycleConfig *lifecycle.Configuration) error {
	return c.client.SetBucketLifecycle(context.Background(), bucketName, lifecycleConfig)
}

func (c *MinIOClient) DirectoryExists(bucketName string, dir string) (bool, error) {
	// 使用 ListObjects 检查是否有对象以该目录为前缀
	objectCh := c.client.ListObjects(context.Background(), bucketName, minio.ListObjectsOptions{Prefix: dir, Recursive: false})
	for object := range objectCh {
		if object.Err != nil {
			log.Logger.Errorf("Failed to list objects in bucket %s: %v", bucketName, object.Err)
			return false, object.Err
		}

		// 打印对象的详细信息
		log.Logger.Infof("Object: %+v", object) // %+v 会打印出 object 的所有字段

		// 或者打印 object 的部分字段
		log.Logger.Infof("Object Key: %s, Size: %d", object.Key, object.Size)

		// 找到以 dir 为前缀的对象，返回 true
		if object.Key != "" {
			log.Logger.Infof("Directory %s exists in bucket %s", dir, bucketName)
			return true, nil
		}
	}
	log.Logger.Infof("Directory %s does not exist in bucket %s", dir, bucketName)
	return false, nil
}

func (c *MinIOClient) PresignedGetObject(bucketName string, mergedFileName string) (string, error) {
	// 将过期时间设置为 24 小时 (24 * 60 * 60 秒)
	expiration := time.Duration(24 * time.Hour)
	// 生成 presigned URL
	presignedURL, err := c.client.PresignedGetObject(context.Background(), bucketName, mergedFileName, expiration, nil)
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %v", err)
	}
	return presignedURL.String(), nil
}

// ListObjects 列出对象 key
func (c *MinIOClient) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) ([]string, error) {
	ch := c.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
	})
	var keys []string
	for obj := range ch {
		if obj.Err != nil {
			log.Logger.Errorf("failed to list objects in bucket %s with prefix %s: %v", bucketName, prefix, obj.Err)
			return nil, fmt.Errorf("failed to list objects in bucket %s with prefix %s: %v", bucketName, prefix, obj.Err)
		}
		if obj.Key != "" {
			keys = append(keys, obj.Key)
		}
	}
	return keys, nil
}

// DeleteObject 删除指定的对象
func (c *MinIOClient) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	return c.client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
}

func (c *MinIOClient) DeleteObjectsByJobInstanceId(ctx context.Context, bucketName, jobInstanceId string) error {
	prefix := fmt.Sprintf("%s/", jobInstanceId)

	log.Logger.Infof("Starting cleanup MinIO export files for job %s in bucket %s", jobInstanceId, bucketName)

	// 列出目录下的所有对象
	keys, err := c.ListObjects(ctx, bucketName, prefix, true)
	if err != nil {
		return fmt.Errorf("failed to list objects for cleanup: %v", err)
	}

	if len(keys) == 0 {
		log.Logger.Infof("No files found to cleanup for job %s", jobInstanceId)
		return nil
	}

	log.Logger.Infof("Found %d files to cleanup for job %s", len(keys), jobInstanceId)

	// 删除所有对象
	var deletedCount int
	var failedKeys []string

	for _, key := range keys {
		log.Logger.Debugf("Deleting object: %s", key)

		err := c.DeleteObject(ctx, bucketName, key)
		if err != nil {
			log.Logger.Errorf("Failed to delete object %s: %v", key, err)
			failedKeys = append(failedKeys, key)
			continue
		}

		deletedCount++
		log.Logger.Debugf("Successfully deleted object: %s", key)
	}

	// 检查是否有删除失败的对象
	if len(failedKeys) > 0 {
		log.Logger.Warnf("Failed to delete %d objects: %v", len(failedKeys), failedKeys)
		return fmt.Errorf("failed to delete %d objects: %v", len(failedKeys), failedKeys)
	}

	log.Logger.Infof("Cleanup completed for job %s, successfully deleted %d files", jobInstanceId, deletedCount)
	return nil
}

func (c *MinIOClient) SetBucketPolicy(bucketName string, policy string) error {
	return c.client.SetBucketPolicy(context.Background(), bucketName, policy)
}
