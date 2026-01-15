package utils

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	log "data-service/log"
)

// IsRetryableNetErr 判断是否为可重试的网络类错误
func IsRetryableNetErr(err error) bool {
	if err == nil {
		return false
	}
	e := strings.ToLower(err.Error())
	return strings.Contains(e, "broken pipe") ||
		strings.Contains(e, "connection reset") ||
		strings.Contains(e, "connection refused") ||
		strings.Contains(e, "timeout") ||
		strings.Contains(e, "eof") ||
		strings.Contains(e, "closed network connection")
}

// 线程安全随机数
var (
	rngMu sync.Mutex
	rng   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func randBetween(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	rngMu.Lock()
	n := rng.Int63n(int64(max - min))
	rngMu.Unlock()
	return min + time.Duration(n)
}

// 带 context、最大退避与抖动的重试（Decorrelated Jitter）
// 参考公式：sleep = min(maxDelay, rand(baseDelay, prevDelay*3))
func WithRetryCtx(ctx context.Context, maxRetries int, baseDelay, maxDelay time.Duration, fn func() error, retryable func(error) bool) error {
	var last error
	// 下一次睡眠的上界，初始为 baseDelay
	next := baseDelay

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			upper := next * 3
			if upper > maxDelay {
				upper = maxDelay
			}
			sleep := randBetween(baseDelay, upper)
			log.Logger.Warnf("Retrying (attempt=%d) after %s: %v", attempt, sleep, last)

			// 可取消等待
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				return ctx.Err()
			}
			// 更新下一次的上界
			next = upper
		}

		if err := fn(); err != nil {
			last = err
			if retryable != nil && !retryable(err) {
				return err
			}
			continue
		}
		return nil
	}
	return last
}

// 兼容旧签名：默认最大退避 2s，无 context
func WithRetry(maxRetries int, baseDelay time.Duration, fn func() error, retryable func(error) bool) error {
	return WithRetryCtx(context.Background(), maxRetries, baseDelay, 2*time.Second, fn, retryable)
}
