package service

import (
	"context"
	"data-service/config"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	client redis.Cmdable // 使用接口类型替代具体类型
}

func NewRedisClient() (*RedisClient, error) {
	conf := config.GetConfigMap()
	redisConf := conf.RedisConfig

	var client redis.Cmdable

	switch redisConf.ClusterType {
	case "sentinel":
		// 哨兵模式
		sentinelAddrs := strings.Split(redisConf.Address, ",")
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    redisConf.SentinelMasterName,
			SentinelAddrs: sentinelAddrs,
			Password:      redisConf.Password,
			DB:            redisConf.DB,
			// 连接池配置
			PoolSize:           10,
			MinIdleConns:       5,
			MaxConnAge:         time.Hour,
			IdleTimeout:        5 * time.Minute,
			IdleCheckFrequency: time.Minute,
		})
	case "cluster":
		// 集群模式
		clusterAddrs := strings.Split(redisConf.Address, ",")
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    clusterAddrs,
			Password: redisConf.Password,
			// 连接池配置
			PoolSize:           10,
			MinIdleConns:       5,
			MaxConnAge:         time.Hour,
			IdleTimeout:        5 * time.Minute,
			IdleCheckFrequency: time.Minute,
		})
	default:
		// 单机模式
		client = redis.NewClient(&redis.Options{
			Addr:     redisConf.Address,
			Password: redisConf.Password,
			DB:       redisConf.DB,
			// 连接池配置
			PoolSize:           10,
			MinIdleConns:       5,
			MaxConnAge:         time.Hour,
			IdleTimeout:        5 * time.Minute,
			IdleCheckFrequency: time.Minute,
		})
	}

	return &RedisClient{client: client}, nil
}

func (c *RedisClient) HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd {
	return c.client.HGetAll(ctx, key)
}

func (c *RedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return c.client.Get(ctx, "job_result:"+key)
}

// 添加关闭连接的方法
func (c *RedisClient) Close() error {
	if closer, ok := c.client.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
