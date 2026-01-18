package clients

import (
	"context"
	"fmt"
	"time"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	
	// 使用生成的proto代码
	pb "data-integrate-test/generated/proto/proto"
)

// IDAServiceClient IDA服务客户端
type IDAServiceClient struct {
	conn   *grpc.ClientConn
	client pb.MiraIdaAccessClient
}

func NewIDAServiceClient(host string, port int) (*IDAServiceClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	
	return &IDAServiceClient{
		conn:   conn,
		client: pb.NewMiraIdaAccessClient(conn),
	}, nil
}

func (c *IDAServiceClient) Close() error {
	return c.conn.Close()
}

// CreateDataSourceRequest 创建数据源请求（简化版本，用于测试）
type CreateDataSourceRequest struct {
	Name         string
	Host         string
	Port         int32
	DBType       int32 // 1.mysql, 2.kingbase, 3.gbase, 4.vastbase
	Username     string
	Password     string
	DatabaseName string
}

// CreateDataSourceResponse 创建数据源响应
type CreateDataSourceResponse struct {
	DataSourceId int32
	Success      bool
	Message      string
}

// CreateAssetRequest 创建资产请求（简化版本，用于测试）
type CreateAssetRequest struct {
	AssetName    string
	AssetEnName  string
	DataSourceId int32
	DBName       string
	TableName    string
}

// CreateAssetResponse 创建资产响应
type CreateAssetResponse struct {
	AssetId  int32
	Success  bool
	Message  string
}

// CreateDataSource 创建数据源
func (c *IDAServiceClient) CreateDataSource(ctx context.Context, req *CreateDataSourceRequest) (*CreateDataSourceResponse, error) {
	// 使用真实的 gRPC 调用
	protoReq := &pb.CreateDataSourceRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: fmt.Sprintf("ds_%d", time.Now().Unix()),
		},
		DbPattern:  1, // 1-关系型数据库
		DbType:     req.DBType,
		Name:       req.Name,
		Host:       req.Host,
		Port:       req.Port,
		Username:   req.Username,
		Password:   req.Password,
		InstanceName: req.DatabaseName,
	}
	
	resp, err := c.client.CreateDataSource(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}
	
	// 转换响应
	result := &CreateDataSourceResponse{
		Success: resp.BaseResponse.Code == 0,
		Message: resp.BaseResponse.Msg,
	}
	
	if resp.Data != nil {
		result.DataSourceId = resp.Data.Id
	}
	
	return result, nil
}

// CreateAsset 创建资产
func (c *IDAServiceClient) CreateAsset(ctx context.Context, req *CreateAssetRequest) (*CreateAssetResponse, error) {
	// 使用真实的 gRPC 调用
	protoReq := &pb.CreateAssetRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: fmt.Sprintf("asset_%d", time.Now().Unix()),
		},
		ChainInfo: &pb.ChainInfo{
			ChainInfoId: "test_chain_001",
			Address:     "test_address_001",
			Cid:         "test_cid_001",
		},
		ResourceBasic: &pb.ResourceBasic{
			ResourceNumber: fmt.Sprintf("ASSET_%d", time.Now().Unix()),
			ZhName:        req.AssetName,
			EnName:        req.AssetEnName,
			Description:   fmt.Sprintf("测试资产: %s", req.AssetName),
			ScaleValue:    1000,
			ScaleUnit:     1, // MB
			UseLimit:      "测试使用",
			Type:          1, // 1-库表
			DataType:      2, // 2-企业数据
			Authorized:    1, // 1-不需要授权
			MachineLearning: 1, // 1-不支持
		},
		Table: &pb.Table{
			DataSourceId: req.DataSourceId,
			TableName:    req.TableName,
			Columns:      []*pb.TableColumn{}, // 可以为空
		},
	}
	
	resp, err := c.client.CreateAsset(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}
	
	// 转换响应
	result := &CreateAssetResponse{
		Success: resp.BaseResponse.Code == 0,
		Message: resp.BaseResponse.Msg,
	}
	
	if resp.Data != nil {
		result.AssetId = resp.Data.Id
	}
	
	return result, nil
}

// GetPrivateAssetInfoByEnName 获取资产信息
func (c *IDAServiceClient) GetPrivateAssetInfoByEnName(ctx context.Context, assetEnName string) (interface{}, error) {
	// TODO: 实现GetPrivateAssetInfoByEnName调用
	// req := &pb.GetPrivateAssetInfoByEnNameRequest{
	//     BaseRequest: &pb.BaseRequest{
	//         RequestId: fmt.Sprintf("test_%d", time.Now().Unix()),
	//     },
	//     AssetEnName: assetEnName,
	// }
	// return c.client.GetPrivateAssetInfoByEnName(ctx, req)
	return nil, fmt.Errorf("not implemented: need proto compilation")
}

// GetPrivateDBConnInfoRequest 查询数据源请求
type GetPrivateDBConnInfoRequest struct {
	RequestId string
	DbConnId  int32
}

// GetPrivateDBConnInfoResp 数据源连接信息
type GetPrivateDBConnInfoResp struct {
	DbConnId   int32
	ConnName   string
	Host       string
	Port       int32
	Type       int32 // 数据库类型：1.mysql, 2.kingbase, 3.gbase, 4.vastbase
	Username   string
	Password   string
	DbName     string
	CreatedAt  string
	LlmHubToken string
}

// GetPrivateDBConnInfoResponse 查询数据源响应
type GetPrivateDBConnInfoResponse struct {
	Code int32
	Msg  string
	Data *GetPrivateDBConnInfoResp
}

// GetPrivateDBConnInfo 查询数据源连接信息
func (c *IDAServiceClient) GetPrivateDBConnInfo(ctx context.Context, req *GetPrivateDBConnInfoRequest) (*GetPrivateDBConnInfoResponse, error) {
	// 使用真实的 gRPC 调用
	protoReq := &pb.GetPrivateDBConnInfoRequest{
		RequestId: req.RequestId,
		DbConnId:  req.DbConnId,
	}
	
	resp, err := c.client.GetPrivateDBConnInfo(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}
	
	// 转换响应
	result := &GetPrivateDBConnInfoResponse{
		Code: resp.BaseResponse.Code,
		Msg:  resp.BaseResponse.Msg,
	}
	
	if resp.Data != nil {
		result.Data = &GetPrivateDBConnInfoResp{
			DbConnId:    resp.Data.DbConnId,
			ConnName:    resp.Data.ConnName,
			Host:        resp.Data.Host,
			Port:        resp.Data.Port,
			Type:        resp.Data.Type,
			Username:    resp.Data.Username,
			Password:    resp.Data.Password,
			DbName:      resp.Data.DbName,
			CreatedAt:   resp.Data.CreatedAt,
			LlmHubToken: resp.Data.LlmHubToken,
		}
	}
	
	return result, nil
}

// AssetItem 资产条目
type AssetItem struct {
	AssetId         string
	AssetNumber     string
	AssetName       string
	HolderCompany   string
	Intro           string
	TxId            string
	UploadedAt      string
	ChainName       string
	ParticipantId   string
	ParticipantName string
	Alias           string
	DataProductType int32
}

// Pagination 分页信息
type Pagination struct {
	PageNumber int32
	PageSize   int32
	Total      int64
}

// GetPrivateAssetListRequest 查询资产列表请求
type GetPrivateAssetListRequest struct {
	RequestId  string
	PageNumber int32
	PageSize   int32
	Filters    []Filter // 搜索条件（可选）
}

// Filter 过滤器
type Filter struct {
	Key    string
	Values []string
}

// GetPrivateAssetListResponse 查询资产列表响应
type GetPrivateAssetListResponse struct {
	Code int32
	Msg  string
	Data struct {
		Pagination Pagination
		List       []AssetItem
	}
}

// GetPrivateAssetList 查询资产列表
func (c *IDAServiceClient) GetPrivateAssetList(ctx context.Context, req *GetPrivateAssetListRequest) (*GetPrivateAssetListResponse, error) {
	// 使用真实的 gRPC 调用
	protoReq := &pb.GetPrivateAssetListRequest{
		BaseRequest: &pb.BaseRequest{
			RequestId: req.RequestId,
		},
		PageNumber: req.PageNumber,
		PageSize:   req.PageSize,
	}
	
	// 转换过滤器
	if len(req.Filters) > 0 {
		protoReq.Filters = make([]*pb.Filter, len(req.Filters))
		for i, f := range req.Filters {
			protoReq.Filters[i] = &pb.Filter{
				Key:    f.Key,
				Values: f.Values,
			}
		}
	}
	
	resp, err := c.client.GetPrivateAssetList(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}
	
	// 转换响应
	result := &GetPrivateAssetListResponse{
		Code: resp.BaseResponse.Code,
		Msg:  resp.BaseResponse.Msg,
	}
	
	if resp.Data != nil {
		result.Data.Pagination = Pagination{
			PageNumber: resp.Data.Pagination.PageNumber,
			PageSize:   resp.Data.Pagination.PageSize,
			Total:      resp.Data.Pagination.Total,
		}
		
		// 转换资产列表
		if resp.Data.List != nil {
			result.Data.List = make([]AssetItem, len(resp.Data.List))
			for i, item := range resp.Data.List {
				result.Data.List[i] = AssetItem{
					AssetId:         item.AssetId,
					AssetNumber:     item.AssetNumber,
					AssetName:       item.AssetName,
					HolderCompany:   item.HolderCompany,
					Intro:           item.Intro,
					TxId:            item.TxId,
					UploadedAt:      item.UploadedAt,
					ChainName:       item.ChainName,
					ParticipantId:   item.ParticipantId,
					ParticipantName: item.ParticipantName,
					Alias:           item.Alias,
					DataProductType: int32(item.DataProductType),
				}
			}
		}
	}
	
	return result, nil
}

// GetPrivateAssetInfoRequest 查询资产详情请求
type GetPrivateAssetInfoRequest struct {
	RequestId string
	AssetId   int32
}

// AssetInfo 资产信息
type AssetInfo struct {
	AssetId         string
	AssetNumber     string
	AssetName       string
	AssetEnName     string
	AssetType       int32
	Scale           string
	Cycle           string
	TimeSpan        string
	HolderCompany   string
	Intro           string
	TxId            string
	UploadedAt      string
	DataInfo        *DataInfo
	VisibleType     int32
	ParticipantId   string
	ParticipantName string
	AccountAlias    string
	DataProductType int32
}

// DataInfo 数据库信息
type DataInfo struct {
	DbName       string
	TableName    string
	ItemList     []SaveTableColumnItem
	DataSourceId int32
}

// SaveTableColumnItem 数据表字段信息
type SaveTableColumnItem struct {
	Name        string
	DataType    string
	DataLength  int32
	Description string
	IsPrimaryKey int32
	PrivacyQuery int32
}

// GetPrivateAssetInfoResponse 查询资产详情响应
type GetPrivateAssetInfoResponse struct {
	Code int32
	Msg  string
	Data *AssetInfo
}

// GetPrivateAssetInfo 查询资产详情
func (c *IDAServiceClient) GetPrivateAssetInfo(ctx context.Context, req *GetPrivateAssetInfoRequest) (*GetPrivateAssetInfoResponse, error) {
	// 使用真实的 gRPC 调用
	protoReq := &pb.GetPrivateAssetInfoRequest{
		RequestId: req.RequestId,
		AssetId:   req.AssetId,
	}
	
	resp, err := c.client.GetPrivateAssetInfo(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC调用失败: %v", err)
	}
	
	// 转换响应
	result := &GetPrivateAssetInfoResponse{
		Code: resp.BaseResponse.Code,
		Msg:  resp.BaseResponse.Msg,
	}
	
	if resp.Data != nil {
		result.Data = &AssetInfo{
			AssetId:         resp.Data.AssetId,
			AssetNumber:     resp.Data.AssetNumber,
			AssetName:       resp.Data.AssetName,
			AssetEnName:     resp.Data.AssetEnName,
			AssetType:       resp.Data.AssetType,
			Scale:           resp.Data.Scale,
			Cycle:           resp.Data.Cycle,
			TimeSpan:        resp.Data.TimeSpan,
			HolderCompany:   resp.Data.HolderCompany,
			Intro:           resp.Data.Intro,
			TxId:            resp.Data.TxId,
			UploadedAt:      resp.Data.UploadedAt,
			VisibleType:     resp.Data.VisibleType,
			ParticipantId:   resp.Data.ParticipantId,
			ParticipantName: resp.Data.ParticipantName,
			AccountAlias:    resp.Data.AccountAlias,
			DataProductType: int32(resp.Data.DataProductType),
		}
		
		// 转换数据信息
		if resp.Data.DataInfo != nil {
			result.Data.DataInfo = &DataInfo{
				DbName:       resp.Data.DataInfo.DbName,
				TableName:    resp.Data.DataInfo.TableName,
				DataSourceId: resp.Data.DataInfo.DataSourceId,
			}
			
			// 转换字段列表
			if resp.Data.DataInfo.ItemList != nil {
				result.Data.DataInfo.ItemList = make([]SaveTableColumnItem, len(resp.Data.DataInfo.ItemList))
				for i, item := range resp.Data.DataInfo.ItemList {
					result.Data.DataInfo.ItemList[i] = SaveTableColumnItem{
						Name:         item.Name,
						DataType:     item.DataType,
						DataLength:   item.DataLength,
						Description:  item.Description,
						IsPrimaryKey: item.IsPrimaryKey,
						PrivacyQuery: item.PrivacyQuery,
					}
				}
			}
		}
	}
	
	return result, nil
}

