package routes

import (
	"context"
	log "data-service/log"
	"encoding/base64"
	"encoding/json"
	"os"

	status "data-service/common"
	"data-service/oss"
	"data-service/utils"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"

	"chainweaver.org.cn/chainweaver/mira/mira-common/minio_access"
	mirapb "chainweaver.org.cn/chainweaver/mira/mira-ida-access-service/pb/mirapb"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	FileFormatCsv = "csv"
)

type GetResultHandler struct {
	req       GetResultReq
	resp      status.Response
	ossClient oss.ClientInterface
}

type GetMinioResultUrlReq struct {
	ObjectName string `json:"object_name"`
}

type GetResultReq struct {
	status.BaseRequest
	JobInstanceId string `json:"jobInstanceId" binding:"required"`
	PartyId       string `json:"partyId" binding:"required"`
	FileType      string `json:"fileType"`
	AsynRequestId string `json:"asynRequestId"`
	DataId        string `json:"dataId"`
	IsEncrypted   bool   `json:"isEncrypted"`
	PubKey        string `json:"pubKey"`
}

type GetResultResp struct {
	JobID      string `json:"jobID"`
	ResultData string `json:"resultData"`
}

// ResultDetail 结构体
type ResultDetail struct {
	PublicURL   string `json:"pubUrl"`
	DataID      string `json:"dataId"`
	PartyId     string `json:"partyId"`
	InternalURL string `json:"intUrl"`
	ResultData  string `json:"resultData"`
}

func NewGetResultHandler(req GetResultReq) GetResultHandler {
	return GetResultHandler{
		req: req,
	}
}

func (h *GetResultHandler) BindReq(c *gin.Context) error {
	if err := c.ShouldBindJSON(&h.req); err != nil {
		log.Logger.Errorf("BindReq error: %v", err)
		h.resp.SetError(status.ErrCodeInvalidParameter, err.Error())
		return err
	}
	return nil
}

func (h *GetResultHandler) Process() {

	// 获取json格式任务结果
	result, errCode, err := h.processTaskResults(h.req.IsEncrypted, h.req.PubKey)
	if err != nil {
		log.Logger.Errorf("GetResultHandler | process task results failed. err: %v", err)
		h.resp.SetErrCode(errCode)
		return
	}

	// 处理文件格式
	result.ResultData, err = h.ProcessFileFormat(result.ResultData)
	if err != nil {
		log.Logger.Errorf("GetResultHandler | process result format failed. err: %v", err)
		h.resp.SetErrCode(status.ErrGetMinioResult)
		return
	}

	h.resp.SetData(result)
}

func (h *GetResultHandler) processTaskResults(isEncrypted bool, pubKey string) (*GetResultResp, int, error) {
	result := &GetResultResp{JobID: h.req.JobInstanceId}

	objectNameJson := fmt.Sprintf("%s/job_results.json", h.req.JobInstanceId)
	allResults, errCode, err := h.collectTaskResults(isEncrypted, pubKey)
	if err != nil {
		return nil, errCode, err
	}

	if len(allResults) == 0 {
		return nil, status.ErrGetMinioResult, errors.New("processTaskResults | no results")
	}

	resultData, err := h.mergeResults(allResults, objectNameJson)
	if err != nil {
		return nil, status.ErrGetMinioResult, errors.WithMessage(err, "processTaskResults | merge results failed")
	}
	result.ResultData = resultData

	return result, status.ErrCodeOK, nil
}

func (h *GetResultHandler) collectTaskResults(isEncrypted bool, pubKey string) ([]string, int, error) {
	var allResults []string

	objectName := h.req.JobInstanceId + "/" + h.req.DataId + ".csv"
	resultContent, err := h.getResultContent(isEncrypted, pubKey, objectName)
	if err != nil {
		return nil, status.ErrGetMinioResult, err
	}

	allResults = append(allResults, resultContent)

	return allResults, status.ErrCodeOK, nil
}

// 从minio获取结果内容（字符串）
func (h *GetResultHandler) getResultContent(isEncrypted bool, pubKey string, objectName string) (string, error) {
	reader, err := h.ossClient.GetObject(context.Background(), status.RESULT_BUCKET_NAME, objectName, &oss.GetOptions{})
	if err != nil {
		log.Logger.Errorf("Failed to get object: %v", err)
		return "", err
	}
	defer reader.Close()

	// 转换为字符串
	content, err := io.ReadAll(reader)
	if err != nil {
		log.Logger.Errorf("Failed to read object content: %v", err)
		return "", err
	}

	fileContent := string(content)

	if isEncrypted {
		return h.decryptContent(fileContent, pubKey)
	}
	return fileContent, nil
}

// 解密内容
func (h *GetResultHandler) decryptContent(content string, key string) (string, error) {
	// 使用流式解密接口
	stream, err := utils.GetIDAService().Client.StreamDecrypt(context.Background(), &mirapb.StreamDecryptRequest{
		CipherText: content,
		AlgoType:   mirapb.AlgoType_SM2,
		PubKey:     key,
		RequestId:  uuid.New().String(),
		Segment:    1024 * 127,
	})
	if err != nil {
		return "", errors.WithMessage(err, "decryptContent | stream decrypt failed")
	}

	// 读取流式响应并拼接解密结果
	var plainTextBuilder strings.Builder
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", errors.WithMessage(err, "decryptContent | receive stream failed")
		}

		// 检查响应状态
		if resp.GetCode() != 0 {
			return "", errors.WithMessage(errors.New(resp.GetMsg()), "decryptContent | decrypt failed")
		}

		plainTextBuilder.WriteString(resp.GetPlainText())
	}

	return plainTextBuilder.String(), nil
}

func (h *GetResultHandler) mergeResults(
	allResults []string, objectNameJson string) (
	string, error) {

	if len(allResults) == 0 {
		return "", nil
	}

	// 默认按软件格式解析
	mergedContent, err := TransformSoftwareFormat(allResults)
	if err != nil {
		// 尝试按硬件格式解析
		mergedContent, err = TransformHardwareFormat(allResults)
		if err != nil {
			return "", errors.WithMessage(err, "mergeResults | transform json arrays failed")
		}
		log.Logger.Infof("mergeResults | transform hardware format suc, objectNameJson: %s", objectNameJson)
	}

	mergedFilePath := "./data/" + objectNameJson

	if err := h.req.saveContentToFile(mergedContent, mergedFilePath); err != nil {
		return "", errors.WithMessage(err, "mergeResults | save merged content to file error")
	}
	defer h.req.deleteFile(mergedFilePath)

	_, err = minio_access.MinioAccessService.UploadFile(objectNameJson, mergedFilePath)
	if err != nil {
		return "", errors.WithMessage(err, "mergeResults | upload merged file to minio error")
	}

	return mergedContent, nil
}

// saveContentToFile 将内容保存到指定文件
func (r *GetResultReq) saveContentToFile(content, filePath string) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.WithMessage(err, "saveContentToFile | failed to create directory")
	}
	err := ioutil.WriteFile(filePath, []byte(content), 0644)
	if err != nil {
		return errors.WithMessage(err, "saveContentToFile | failed to write file")
	}
	return nil
}

func (h *GetResultHandler) GetResponse() *status.Response {
	return &h.resp
}

func (h *GetResultHandler) ProcessFileFormat(result string) (string, error) {
	// 兼容空结果
	if strings.TrimSpace(result) == "" {
		return encodeContent("[]"), nil
	}

	if h.req.FileType == FileFormatCsv {
		csvContent, err := utils.JsonToCSV([]byte(result))
		if err != nil {
			return "", fmt.Errorf("processResultFormat | transform to csv failed. err: %v", err)
		}
		return encodeContent(string(csvContent)), nil
	}

	return encodeContent(result), nil
}

// TransformSoftwareFormat 将多个JSON字符串合并并转换格式
// 输入格式: 详见https://www.tapd.cn/51081496/markdown_wikis/show/#1151081496001001856
// 输出格式: [{"column1":1,"column2":2},{"column1":1,"column2":2}]
func TransformSoftwareFormat(allResults []string) (string, error) {
	r, columnsIdx, err := dealRows(allResults)
	if err != nil {
		return "", errors.WithMessage(err, "TransformSoftwareFormat | deal row failed")
	}

	result, err := json.Marshal(r)
	if err != nil {
		return "", errors.WithMessage(err, "TransformSoftwareFormat | marshal result failed")
	}

	// 解析所有输入的JSON字符串
	var columnMap = make(map[string][]interface{})
	maxLen := 0
	// 兼容空结果
	if checkResultIsEmpty(string(result)) {
		return "", fmt.Errorf("TransformSoftwareFormat | result is empty")
	}

	var resultMap map[string][]interface{}
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		return "", errors.WithMessage(err, "TransformSoftwareFormat | unmarshal input JSON failed")
	}

	// 合并所有列数据
	for col, values := range resultMap {
		columnMap[col] = values
		if len(values) > maxLen {
			maxLen = len(values)
		}
	}

	// 兼容空结果
	if len(columnMap) == 0 {
		return "", nil
	}

	// 构建转换后的结果
	// [{"column1":1,"column2":2},{"column1":1,"column2":2}]
	var transformedResults []string
	for i := 0; i < maxLen; i++ {
		row := "{"
		// 按排序后的列名顺序构建 row
		for _, col := range columnsIdx {
			values := columnMap[col]
			if i < len(values) {
				value, _ := json.Marshal(values[i])
				row += fmt.Sprintf("\"%s\":%s,", col, string(value))
			}
		}
		row = strings.TrimSuffix(row, ",")
		row += "}"
		transformedResults = append(transformedResults, row)
	}

	// 将结果转换回JSON字符串
	return "[" + strings.Join(transformedResults, ",") + "]", nil
}

// 背景：目前硬件常用的板卡执行结果是json格式，每个mpc的结果格式可能不尽相同
// TransformHardwareFormat 将多个JSON字符串合并并转换格式
// 输入格式：[[{"id":"1","val":"这里是分包1板卡执行结果"},{"id":"2","val":"这里是分包2板卡执行结果"}],[...]]
// 输出格式：[{"id":"1","val":"这里是分包1板卡执行结果"},{"id":"2","val":"这里是分包2板卡执行结果"}...]
func TransformHardwareFormat(allResults []string) (string, error) {
	var transformedResult []map[string]interface{}

	for _, result := range allResults {
		// 兼容空结果
		if result == "" {
			continue
		}
		var arrayResult []map[string]interface{}
		if err := json.Unmarshal([]byte(result), &arrayResult); err != nil {
			return "", errors.WithMessage(err, "TransformHardware | unmarshal input JSON failed")
		}
		transformedResult = append(transformedResult, arrayResult...)
	}

	if len(transformedResult) == 0 {
		return "[]", nil
	}

	resultJSON, err := json.Marshal(transformedResult)
	if err != nil {
		return "", errors.WithMessage(err, "TransformHardware | marshal output JSON failed")
	}

	return string(resultJSON), nil
}

// 2025.02.24 变更：软件执行引擎将参与方信息、表信息写入minIO, 解析后展示给用户以区分同名属性
//
//	输入：{"id": {
//		"value": [1, 2, 3, 4],
//		"party_id": "party_id_1",
//		"asset_name": "asset_name_1"
//		}], "id2"...}
//
// 输出：{"party_id_1.asset_name_1.id":[1, 2, 3, 4], "party_id_2.asset_name_2.id2":[1, 2, 3, 4]}
func dealRows(rows []string) (map[string][]interface{}, []string, error) {
	type rowInfo struct {
		Value      []interface{} `json:"value"`
		PartyId    string        `json:"party_id"`
		AssetName  string        `json:"asset_name"`
		PartyName  string        `json:"party_name"`
		ItemIdx    int           `json:"item_idx"`
		ColumnName string        `json:"column"`
	}
	res := make(map[string][]interface{})
	indexArray := make([]rowInfo, 0, len(rows))
	for _, row := range rows {
		rowInfoMap := make(map[string]rowInfo)
		err := json.Unmarshal([]byte(row), &rowInfoMap)
		if err != nil {
			return nil, nil, fmt.Errorf("error unmarshalling err: %v, row: %s", err, row)
		}

		for k, v := range rowInfoMap {
			if v.PartyName == "" || v.AssetName == "" {
				res[k] = v.Value
				indexArray = append(indexArray, rowInfo{ColumnName: k, ItemIdx: v.ItemIdx})
			} else {
				res[v.PartyName+"."+v.AssetName+"."+k] = v.Value
				indexArray = append(indexArray, rowInfo{
					ColumnName: v.PartyName + "." + v.AssetName + "." + k,
					ItemIdx:    v.ItemIdx,
				})
			}
		}
	}

	sort.Slice(indexArray, func(i, j int) bool {
		return indexArray[i].ItemIdx < indexArray[j].ItemIdx
	})

	columns := make([]string, 0, len(indexArray))
	for _, v := range indexArray {
		columns = append(columns, v.ColumnName)
	}

	return res, columns, nil
}

// deleteFile 删除已经保存的文件，防止占用太多存储空间
func (r *GetResultReq) deleteFile(filePath string) error {
	err := os.Remove(filePath)
	if err != nil {
		return errors.WithMessage(err, "deleteFile | failed to delete file")
	}
	return nil
}

// 结果为空时也应该有表头之类的数据， 但各个执行引擎实现可能不一致， 这里对空结果做兼容
func checkResultIsEmpty(result string) bool {
	return strings.TrimSpace(result) == "" ||
		strings.TrimSpace(result) == "[]" ||
		strings.TrimSpace(result) == "{}"
}

func encodeContent(content string) string {
	return base64.StdEncoding.EncodeToString([]byte(content))
}
