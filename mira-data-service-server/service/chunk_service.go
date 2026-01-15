package service

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"data-service/common"
	"data-service/oss"
)

// ChunkService 分块服务
type ChunkService struct {
	ossClient  oss.ClientInterface
	chunkCache map[string]*ChunkSession // 文件ID -> 分块会话
	mutex      sync.RWMutex
}

// ChunkSession 分块会话
type ChunkSession struct {
	FileID     string
	FilePath   string
	FileSize   int64
	Chunks     []*common.ChunkInfo
	Completed  map[int64]bool
	LastAccess time.Time
	Mutex      sync.RWMutex
}

// NewChunkService 创建分块服务
func NewChunkService(ossClient oss.ClientInterface) *ChunkService {
	cs := &ChunkService{
		ossClient:  ossClient,
		chunkCache: make(map[string]*ChunkSession),
	}

	// 启动清理过期会话的goroutine
	go cs.cleanupExpiredSessions()

	return cs
}

// GetChunk 获取指定分块
func (cs *ChunkService) GetChunk(ctx context.Context, req *common.ChunkRequest) (*common.ChunkResponse, error) {
	// 获取或创建会话
	session, err := cs.getOrCreateSession(ctx, req.FileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get session: %v", err)
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	// 检查分块ID是否有效
	if req.ChunkID < 0 || req.ChunkID >= int64(len(session.Chunks)) {
		return nil, fmt.Errorf("invalid chunk ID: %d", req.ChunkID)
	}

	chunk := session.Chunks[req.ChunkID]

	// 读取分块数据
	data, err := cs.readChunkData(ctx, session.FilePath, chunk, req.ResumeOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk data: %v", err)
	}

	// 计算校验和
	chunk.Checksum = cs.calculateChecksum(data)

	// 标记为完成
	session.Completed[req.ChunkID] = true
	session.LastAccess = time.Now()

	// 判断是否为最后一个分块
	isLastChunk := req.ChunkID == int64(len(session.Chunks))-1

	// 确定下一个分块ID
	nextChunkID := req.ChunkID + 1
	if nextChunkID >= int64(len(session.Chunks)) {
		nextChunkID = -1 // 表示没有下一个分块
	}

	return &common.ChunkResponse{
		ChunkInfo:   chunk,
		Data:        data,
		NextChunkID: nextChunkID,
		IsLastChunk: isLastChunk,
	}, nil
}

// readChunkData 读取分块数据
func (cs *ChunkService) readChunkData(ctx context.Context, filePath string, chunk *common.ChunkInfo, resumeOffset int64) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 计算实际读取的起始位置
	startPos := chunk.StartOffset + resumeOffset
	if startPos >= chunk.EndOffset {
		return nil, fmt.Errorf("resume offset exceeds chunk end")
	}

	// 定位到指定位置
	_, err = file.Seek(startPos, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// 计算需要读取的数据大小
	readSize := chunk.EndOffset - startPos

	// 读取数据
	data := make([]byte, readSize)
	n, err := io.ReadFull(file, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	return data[:n], nil
}

// getOrCreateSession 获取或创建会话
func (cs *ChunkService) getOrCreateSession(ctx context.Context, fileID string) (*ChunkSession, error) {
	cs.mutex.RLock()
	if session, exists := cs.chunkCache[fileID]; exists {
		cs.mutex.RUnlock()
		session.LastAccess = time.Now()
		return session, nil
	}
	cs.mutex.RUnlock()

	// 创建新会话
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	// 双重检查
	if session, exists := cs.chunkCache[fileID]; exists {
		return session, nil
	}

	// 这里需要根据fileID获取文件信息
	// 实际实现中可能需要从数据库或配置中获取
	session := &ChunkSession{
		FileID:     fileID,
		FilePath:   "", // 需要根据fileID获取
		FileSize:   0,  // 需要根据fileID获取
		Chunks:     []*common.ChunkInfo{},
		Completed:  make(map[int64]bool),
		LastAccess: time.Now(),
	}

	cs.chunkCache[fileID] = session
	return session, nil
}

// calculateChecksum 计算数据校验和
func (cs *ChunkService) calculateChecksum(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// cleanupExpiredSessions 清理过期会话
func (cs *ChunkService) cleanupExpiredSessions() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cs.mutex.Lock()
		now := time.Now()
		for fileID, session := range cs.chunkCache {
			if now.Sub(session.LastAccess) > 30*time.Minute {
				delete(cs.chunkCache, fileID)
			}
		}
		cs.mutex.Unlock()
	}
}
