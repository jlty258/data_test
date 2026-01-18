package clients

import (
	"context"
	"fmt"
	pb "data-integrate-test/generated/proto/proto"
)

// UpdateAssetRequest 更新资产请求
type UpdateAssetRequest struct {
	RequestId     string
	AssetId       int32
	ResourceBasic *ResourceBasic // 可选，只更新提供的字段
	Table         *Table         // 可选
}

// ResourceBasic 资源基本信息
type ResourceBasic struct {
	ResourceNumber string
	ZhName         string
	EnName         string
	Description    string
	Type           int32
	ScaleValue     int32
}

// Table 表信息
type Table struct {
	DataSourceId int32
	TableName    string
	Columns      []TableColumn
}

// TableColumn 表字段
type TableColumn struct {
	OriginName  string
	Name        string
	DataType    string
	DataLength  int32
	Description string
	PrimaryKey  int32
	NotNull     int32
	Level       int32
}

// UpdateAssetResponse 更新资产响应
type UpdateAssetResponse struct {
	Code int32
	Msg  string
	Data *AssetInfo
}

// UpdateAsset 更新资产
func (c *IDAServiceClient) UpdateAsset(ctx context.Context, req *UpdateAssetRequest) (*UpdateAssetResponse, error) {
	protoReq := &pb.UpdateAssetRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: req.RequestId,
		},
		AssetId: req.AssetId,
	}

	// 转换资源基本信息
	if req.ResourceBasic != nil {
		protoReq.ResourceBasic = &pb.ResourceBasic{
			ResourceNumber: req.ResourceBasic.ResourceNumber,
			ZhName:         req.ResourceBasic.ZhName,
			EnName:         req.ResourceBasic.EnName,
			Description:    req.ResourceBasic.Description,
			Type:           req.ResourceBasic.Type,
			ScaleValue:     req.ResourceBasic.ScaleValue,
		}
	}

	// 转换表信息
	if req.Table != nil {
		columns := make([]*pb.TableColumn, len(req.Table.Columns))
		for i, col := range req.Table.Columns {
			columns[i] = &pb.TableColumn{
				OriginName:  col.OriginName,
				Name:        col.Name,
				DataType:    col.DataType,
				DataLength:  col.DataLength,
				Description: col.Description,
				PrimaryKey:  col.PrimaryKey,
				NotNull:     col.NotNull,
				Level:       col.Level,
			}
		}
		protoReq.Table = &pb.Table{
			DataSourceId: req.Table.DataSourceId,
			TableName:    req.Table.TableName,
			Columns:      columns,
		}
	}

	resp, err := c.client.UpdateAsset(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}

	result := &UpdateAssetResponse{
		Code: resp.BaseResponse.Code,
		Msg:  resp.BaseResponse.Msg,
	}

	if resp.Data != nil {
		result.Data = convertAssetInfo(resp.Data)
	}

	return result, nil
}

// DeleteAssetRequest 删除资产请求
type DeleteAssetRequest struct {
	RequestId string
	AssetId   int32
}

// DeleteAssetResponse 删除资产响应
type DeleteAssetResponse struct {
	Code int32
	Msg  string
}

// DeleteAsset 删除资产
func (c *IDAServiceClient) DeleteAsset(ctx context.Context, req *DeleteAssetRequest) (*DeleteAssetResponse, error) {
	protoReq := &pb.DeleteAssetRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: req.RequestId,
		},
		AssetId: req.AssetId,
	}

	resp, err := c.client.DeleteAsset(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}

	return &DeleteAssetResponse{
		Code: resp.BaseResponse.Code,
		Msg:  resp.BaseResponse.Msg,
	}, nil
}

// convertAssetInfo 转换资产信息
func convertAssetInfo(src *pb.AssetInfo) *AssetInfo {
	if src == nil {
		return nil
	}

	result := &AssetInfo{
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
		TxId:            src.TxId,
		UploadedAt:      src.UploadedAt,
		VisibleType:     src.VisibleType,
		ParticipantId:   src.ParticipantId,
		ParticipantName: src.ParticipantName,
		AccountAlias:    src.AccountAlias,
		DataProductType: int32(src.DataProductType),
	}

	if src.DataInfo != nil {
		itemList := make([]SaveTableColumnItem, len(src.DataInfo.ItemList))
		for i, item := range src.DataInfo.ItemList {
			itemList[i] = SaveTableColumnItem{
				Name:        item.Name,
				DataType:    item.DataType,
				DataLength:  item.DataLength,
				Description: item.Description,
				IsPrimaryKey: item.IsPrimaryKey,
				PrivacyQuery: item.PrivacyQuery,
			}
		}
		result.DataInfo = &DataInfo{
			DbName:       src.DataInfo.DbName,
			TableName:    src.DataInfo.TableName,
			ItemList:     itemList,
			DataSourceId: src.DataInfo.DataSourceId,
		}
	}

	return result
}

