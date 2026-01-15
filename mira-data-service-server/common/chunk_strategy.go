package common

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
)

// ChunkInfo 分块信息
type ChunkInfo struct {
	ChunkID     int64  `json:"chunk_id"`     // 分块ID
	StartOffset int64  `json:"start_offset"` // 起始偏移量
	EndOffset   int64  `json:"end_offset"`   // 结束偏移量
	ChunkSize   int64  `json:"chunk_size"`   // 分块大小
	TotalChunks int64  `json:"total_chunks"` // 总分块数
	Checksum    string `json:"checksum"`     // 分块校验和
	IsComplete  bool   `json:"is_complete"`  // 是否完整
	RetryCount  int    `json:"retry_count"`  // 重试次数
}

// ChunkRequest 分块请求
type ChunkRequest struct {
	FileID       string `json:"file_id"`        // 文件唯一标识
	ChunkID      int64  `json:"chunk_id"`       // 请求的分块ID
	ResumeOffset int64  `json:"resume_offset"`  // 断点续传偏移量
	MaxChunkSize int64  `json:"max_chunk_size"` // 最大分块大小
}

// ChunkResponse 分块响应
type ChunkResponse struct {
	ChunkInfo   *ChunkInfo `json:"chunk_info"`    // 分块信息
	Data        []byte     `json:"data"`          // 分块数据
	NextChunkID int64      `json:"next_chunk_id"` // 下一个分块ID
	IsLastChunk bool       `json:"is_last_chunk"` // 是否为最后一个分块
}

// CalculateChunks 计算文件分块信息
func CalculateChunks(fileSize, chunkSize int) []*ChunkInfo {
	return calculateChunksByLines(fileSize, chunkSize)
}

// GenerateFileID 生成文件唯一标识
func GenerateFileID(filePath string, fileSize, lastModified int64) string {
	data := fmt.Sprintf("%s_%d_%d", filePath, fileSize, lastModified)
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}

// 流式读取 CSV，按行分块
func StreamCSVByChunks(reader io.Reader, linesPerChunk int) (<-chan []string, error) {
	chunkChan := make(chan []string, 10) // 缓冲通道

	go func() {
		defer close(chunkChan)

		scanner := bufio.NewScanner(reader)
		var currentChunk []string

		for scanner.Scan() {
			currentChunk = append(currentChunk, scanner.Text())

			if len(currentChunk) >= linesPerChunk {
				chunkChan <- currentChunk
				currentChunk = nil
			}
		}

		// 发送最后一个不完整的 chunk
		if len(currentChunk) > 0 {
			chunkChan <- currentChunk
		}
	}()

	return chunkChan, nil
}

// 按行数切分
func calculateChunksByLines(fileSize, linesPerChunk int) []*ChunkInfo {
	// 估算总行数（假设平均每行100字节）
	estimatedLines := int(fileSize / 100)
	totalChunks := (estimatedLines + linesPerChunk - 1) / linesPerChunk

	var chunks []*ChunkInfo
	for i := 0; i < totalChunks; i++ {
		chunk := &ChunkInfo{
			ChunkID:     int64(i),
			StartOffset: int64(i * linesPerChunk), // 行号偏移
			EndOffset:   int64((i + 1) * linesPerChunk),
			ChunkSize:   int64(linesPerChunk),
			TotalChunks: int64(totalChunks),
		}
		chunks = append(chunks, chunk)
	}
	return chunks
}
