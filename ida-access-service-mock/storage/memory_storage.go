package storage

import (
	"context"
	"fmt"
	"ida-access-service-mock/mirapb"
	"sync"
	"time"
)

// MemoryStorage 内存存储实现
type MemoryStorage struct {
	mu          sync.RWMutex
	dataSources map[int32]*mirapb.GetPrivateDBConnInfoResp
	assets      map[int32]*mirapb.AssetInfo
	assetsByEnName map[string]int32 // assetEnName -> assetId
	nextDSId    int32
	nextAssetId int32
}

// NewMemoryStorage 创建内存存储实例
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		dataSources:    make(map[int32]*mirapb.GetPrivateDBConnInfoResp),
		assets:         make(map[int32]*mirapb.AssetInfo),
		assetsByEnName: make(map[string]int32),
		nextDSId:       1000,
		nextAssetId:    2000,
	}
}

// CreateDataSource 创建数据源
func (s *MemoryStorage) CreateDataSource(ctx context.Context, req *mirapb.CreateDataSourceRequest) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextDSId
	s.nextDSId++

	now := time.Now().Format(time.RFC3339)
	dataSource := &mirapb.GetPrivateDBConnInfoResp{
		DbConnId:   id,
		ConnName:   req.Name,
		Host:       req.Host,
		Port:       req.Port,
		Type:       req.DbType,
		Username:   req.Username,
		Password:   req.Password,
		DbName:     req.InstanceName,
		CreatedAt:  now,
		LlmHubToken: req.LlmHubToken,
	}

	s.dataSources[id] = dataSource
	return id, nil
}

// GetDataSource 获取数据源
func (s *MemoryStorage) GetDataSource(ctx context.Context, dbConnId int32) (*mirapb.GetPrivateDBConnInfoResp, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ds, ok := s.dataSources[dbConnId]
	if !ok {
		return nil, fmt.Errorf("数据源不存在: %d", dbConnId)
	}

	// 返回副本，避免并发修改
	return &mirapb.GetPrivateDBConnInfoResp{
		DbConnId:    ds.DbConnId,
		ConnName:    ds.ConnName,
		Host:        ds.Host,
		Port:        ds.Port,
		Type:        ds.Type,
		Username:    ds.Username,
		Password:    ds.Password,
		DbName:      ds.DbName,
		CreatedAt:   ds.CreatedAt,
		LlmHubToken: ds.LlmHubToken,
	}, nil
}

// ListDataSources 列出所有数据源
func (s *MemoryStorage) ListDataSources(ctx context.Context) ([]*mirapb.GetPrivateDBConnInfoResp, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*mirapb.GetPrivateDBConnInfoResp, 0, len(s.dataSources))
	for _, ds := range s.dataSources {
		result = append(result, &mirapb.GetPrivateDBConnInfoResp{
			DbConnId:    ds.DbConnId,
			ConnName:    ds.ConnName,
			Host:        ds.Host,
			Port:        ds.Port,
			Type:        ds.Type,
			Username:    ds.Username,
			Password:    ds.Password,
			DbName:      ds.DbName,
			CreatedAt:   ds.CreatedAt,
			LlmHubToken: ds.LlmHubToken,
		})
	}

	return result, nil
}

// CreateAsset 创建资产
func (s *MemoryStorage) CreateAsset(ctx context.Context, req *mirapb.CreateAssetRequest) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextAssetId
	s.nextAssetId++

	now := time.Now().Format(time.RFC3339)
	
	// 构建 DataInfo
	var dataInfo *mirapb.DataInfo
	if req.Table != nil {
		itemList := make([]*mirapb.SaveTableColumnItem, 0, len(req.Table.Columns))
		for _, col := range req.Table.Columns {
			itemList = append(itemList, &mirapb.SaveTableColumnItem{
				Name:        col.Name,
				DataType:    col.DataType,
				DataLength:  col.DataLength,
				Description: col.Description,
				IsPrimaryKey: col.PrimaryKey,
				PrivacyQuery: 0, // 默认值
			})
		}
		dataInfo = &mirapb.DataInfo{
			DbName:       "",
			TableName:    req.Table.TableName,
			ItemList:     itemList,
			DataSourceId: req.Table.DataSourceId,
		}
	}

	asset := &mirapb.AssetInfo{
		AssetId:         fmt.Sprintf("%d", id),
		AssetNumber:     req.ResourceBasic.ResourceNumber,
		AssetName:       req.ResourceBasic.ZhName,
		AssetEnName:     req.ResourceBasic.EnName,
		AssetType:       req.ResourceBasic.Type,
		Scale:           fmt.Sprintf("%d", req.ResourceBasic.ScaleValue),
		Cycle:           "",
		TimeSpan:        "",
		HolderCompany:   "",
		Intro:           req.ResourceBasic.Description,
		TxId:            "",
		UploadedAt:      now,
		DataInfo:        dataInfo,
		VisibleType:     1,
		ParticipantId:   "",
		ParticipantName: "",
		AccountAlias:    req.ChainInfo.Address,
		DataProductType: mirapb.DataProductType_DATA_PRODUCT_TYPE_DATASET, // 默认为数据集
	}

	s.assets[id] = asset
	if req.ResourceBasic.EnName != "" {
		s.assetsByEnName[req.ResourceBasic.EnName] = id
	}

	return id, nil
}

// GetAssetByEnName 通过英文名获取资产
func (s *MemoryStorage) GetAssetByEnName(ctx context.Context, assetEnName string) (*mirapb.AssetInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	assetId, ok := s.assetsByEnName[assetEnName]
	if !ok {
		return nil, fmt.Errorf("资产不存在: %s", assetEnName)
	}

	asset, ok := s.assets[assetId]
	if !ok {
		return nil, fmt.Errorf("资产不存在: %d", assetId)
	}

	// 返回副本
	return copyAssetInfo(asset), nil
}

// GetAssetById 通过ID获取资产
func (s *MemoryStorage) GetAssetById(ctx context.Context, assetId int32) (*mirapb.AssetInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	asset, ok := s.assets[assetId]
	if !ok {
		return nil, fmt.Errorf("资产不存在: %d", assetId)
	}

	return copyAssetInfo(asset), nil
}

// ListAssets 列出资产
func (s *MemoryStorage) ListAssets(ctx context.Context, pageNumber, pageSize int32, filters []*mirapb.Filter) ([]*mirapb.AssetItem, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 简单的过滤和分页实现
	allAssets := make([]*mirapb.AssetItem, 0, len(s.assets))
		for _, asset := range s.assets {
			// 应用过滤器（简化实现）
			if matchFilters(asset, filters) {
				allAssets = append(allAssets, &mirapb.AssetItem{
					AssetId:         asset.AssetId,
					AssetNumber:     asset.AssetNumber,
					AssetName:       asset.AssetName,
					HolderCompany:   asset.HolderCompany,
					Intro:           asset.Intro,
					TxId:           asset.TxId,
					UploadedAt:      asset.UploadedAt,
					ChainName:       "",
					ParticipantId:   asset.ParticipantId,
					ParticipantName: asset.ParticipantName,
					Alias:          asset.AccountAlias,
					DataProductType: asset.DataProductType,
				})
			}
		}

	total := int64(len(allAssets))

	// 分页
	start := (pageNumber - 1) * pageSize
	end := start + pageSize
	if start >= int32(len(allAssets)) {
		return []*mirapb.AssetItem{}, total, nil
	}
	if end > int32(len(allAssets)) {
		end = int32(len(allAssets))
	}

	return allAssets[start:end], total, nil
}

// UpdateAsset 更新资产
func (s *MemoryStorage) UpdateAsset(ctx context.Context, req *mirapb.UpdateAssetRequest) (*mirapb.AssetInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	asset, ok := s.assets[req.AssetId]
	if !ok {
		return nil, fmt.Errorf("资产不存在: %d", req.AssetId)
	}

	// 更新资源基本信息（如果提供）
	if req.ResourceBasic != nil {
		if req.ResourceBasic.ResourceNumber != "" {
			asset.AssetNumber = req.ResourceBasic.ResourceNumber
		}
		if req.ResourceBasic.ZhName != "" {
			asset.AssetName = req.ResourceBasic.ZhName
		}
		if req.ResourceBasic.EnName != "" {
			// 如果英文名改变，需要更新索引
			oldEnName := asset.AssetEnName
			if oldEnName != "" && oldEnName != req.ResourceBasic.EnName {
				delete(s.assetsByEnName, oldEnName)
			}
			asset.AssetEnName = req.ResourceBasic.EnName
			if req.ResourceBasic.EnName != "" {
				s.assetsByEnName[req.ResourceBasic.EnName] = req.AssetId
			}
		}
		if req.ResourceBasic.Description != "" {
			asset.Intro = req.ResourceBasic.Description
		}
		if req.ResourceBasic.Type > 0 {
			asset.AssetType = req.ResourceBasic.Type
		}
		if req.ResourceBasic.ScaleValue > 0 {
			asset.Scale = fmt.Sprintf("%d", req.ResourceBasic.ScaleValue)
		}
	}

	// 更新表信息（如果提供）
	if req.Table != nil {
		itemList := make([]*mirapb.SaveTableColumnItem, 0, len(req.Table.Columns))
		for _, col := range req.Table.Columns {
			itemList = append(itemList, &mirapb.SaveTableColumnItem{
				Name:        col.Name,
				DataType:    col.DataType,
				DataLength:  col.DataLength,
				Description: col.Description,
				IsPrimaryKey: col.PrimaryKey,
				PrivacyQuery: 0, // 默认值
			})
		}
		asset.DataInfo = &mirapb.DataInfo{
			DbName:       "",
			TableName:    req.Table.TableName,
			ItemList:     itemList,
			DataSourceId: req.Table.DataSourceId,
		}
	}

	return copyAssetInfo(asset), nil
}

// DeleteAsset 删除资产
func (s *MemoryStorage) DeleteAsset(ctx context.Context, assetId int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	asset, ok := s.assets[assetId]
	if !ok {
		return fmt.Errorf("资产不存在: %d", assetId)
	}

	// 从英文名索引中删除
	if asset.AssetEnName != "" {
		delete(s.assetsByEnName, asset.AssetEnName)
	}

	// 从资产映射中删除
	delete(s.assets, assetId)

	return nil
}

// Close 关闭存储
func (s *MemoryStorage) Close() error {
	return nil
}

// copyAssetInfo 复制资产信息
func copyAssetInfo(src *mirapb.AssetInfo) *mirapb.AssetInfo {
	if src == nil {
		return nil
	}

	dst := &mirapb.AssetInfo{
		AssetId:         src.AssetId,
		AssetNumber:     src.AssetNumber,
		AssetName:       src.AssetName,
		AssetEnName:     src.AssetEnName,
		AssetType:       src.AssetType,
		Scale:           src.Scale,
		Cycle:           src.Cycle,
		TimeSpan:        src.TimeSpan,
		HolderCompany:   src.HolderCompany,
		Intro:           src.Intro,
		TxId:           src.TxId,
		UploadedAt:      src.UploadedAt,
		VisibleType:    src.VisibleType,
		ParticipantId:   src.ParticipantId,
		ParticipantName: src.ParticipantName,
		AccountAlias:    src.AccountAlias,
		DataProductType: src.DataProductType,
	}

	if src.DataInfo != nil {
		itemList := make([]*mirapb.SaveTableColumnItem, len(src.DataInfo.ItemList))
		for i, item := range src.DataInfo.ItemList {
			itemList[i] = &mirapb.SaveTableColumnItem{
				Name:        item.Name,
				DataType:    item.DataType,
				DataLength:  item.DataLength,
				Description: item.Description,
				IsPrimaryKey: item.IsPrimaryKey,
				PrivacyQuery: item.PrivacyQuery,
			}
		}
		dst.DataInfo = &mirapb.DataInfo{
			DbName:       src.DataInfo.DbName,
			TableName:    src.DataInfo.TableName,
			ItemList:     itemList,
			DataSourceId: src.DataInfo.DataSourceId,
		}
	}

	return dst
}

// matchFilters 匹配过滤器（简化实现）
func matchFilters(asset *mirapb.AssetInfo, filters []*mirapb.Filter) bool {
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if filter == nil {
			continue
		}
		// 简化实现：只检查资产名称
		if filter.Key == "assetName" {
			matched := false
			for _, value := range filter.Values {
				if asset.AssetName == value {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}

	return true
}

