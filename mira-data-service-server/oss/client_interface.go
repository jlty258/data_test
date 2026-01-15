//go:generate mockgen -source=client_interface.go -destination=../mocks/mock_client_interface.go -package=mocks
/*
*

	@author: shiliang
	@date: 2024/10/30
	@note: OSS 客户端的基本操作接口

*
*/
package oss

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

type ClientInterface interface {
	GetObject(ctx context.Context, bucketName, objectName string, opts *GetOptions) (io.ReadCloser, error)

	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts *PutOptions) (info interface{}, err error)

	BucketExists(bucketName string) (bool, error)

	MakeBucket(bucketName, location string) error

	SetBucketLifecycle(bucketName string, lifecycleConfig *lifecycle.Configuration) error

	DirectoryExists(bucketName string, dir string) (bool, error)

	PresignedGetObject(bucketName string, mergedFileName string) (string, error)

	ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) ([]string, error)

	DeleteObject(ctx context.Context, bucketName, objectName string) error

	DeleteObjectsByJobInstanceId(ctx context.Context, bucketName, jobInstanceId string) error

	// 设置桶策略
	SetBucketPolicy(bucketName string, policy string) error
}
