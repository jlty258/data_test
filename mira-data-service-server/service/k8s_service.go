package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"data-service/common"
	"data-service/config"
	pb "data-service/generated/datasource"

	"data-service/utils"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type K8sService interface {
	SubmitBatchJob(ctx context.Context, request *pb.BatchReadRequest) (*pb.BatchResponse, error)
	GetJobStatus(ctx context.Context, request *pb.JobStatusRequest) (*pb.BatchResponse, error)
}

type k8sService struct {
	logger      *zap.SugaredLogger
	k8sClient   *kubernetes.Clientset
	redisClient *RedisClient
}

func NewK8sService(logger *zap.SugaredLogger) (K8sService, error) {
	// 初始化 k8s 客户端
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %v", err)
	}

	// 初始化 Redis 客户端
	redisClient, err := NewRedisClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %v", err)
	}

	return &k8sService{
		logger:      logger,
		k8sClient:   clientset,
		redisClient: redisClient,
	}, nil
}

func (s *k8sService) logPodInfo(pod *corev1.Pod) {
	s.logger.Infof("Created Spark Pod: name=%s, namespace=%s, uid=%s",
		pod.Name, pod.Namespace, pod.UID)
	s.logger.Infof("Pod Status: phase=%s, hostIP=%s, podIP=%s",
		pod.Status.Phase, pod.Status.HostIP, pod.Status.PodIP)
}

// 提交批处理作业
func (s *k8sService) SubmitBatchJob(ctx context.Context, request *pb.BatchReadRequest) (*pb.BatchResponse, error) {
	s.logger.Infof("Received batch job request: RequestId=%s, OperationType=%v",
		request.RequestId,
		request.Operation)
	var jobInstanceId string

	// 根据数据源类型获取连接信息
	var connInfo *pb.ConnectionInfo
	var err error
	conf := config.GetConfigMap()
	// 获取表信息以调整Spark配置
	var tableInfo *pb.TableInfoResponse
	tableInfoService := NewTableInfoService(s.logger)

	switch ds := request.DataSource.(type) {
	case *pb.BatchReadRequest_External:
		// 外部数据源：使用 assetName, chainInfoId, platformId 获取连接信息
		s.logger.Infof("GetDatasourceByAssetName参数: AssetName=%s, ChainInfoId=%s, PlatformId=%s, RequestId=%s",
			ds.External.AssetName,
			ds.External.ChainInfoId,
			request.GetRequestId())
		connInfo, err = utils.GetDatasourceByAssetName(
			request.GetRequestId(),
			ds.External.AssetName,
			ds.External.ChainInfoId,
			ds.External.Alias,
		)
		if err != nil {
			s.logger.Errorf("Failed to get connection info: %v, RequestId=%s", err, request.RequestId)
			return nil, err
		}
		tableInfo, err = tableInfoService.GetTableInfo(request.GetRequestId(), ds.External.AssetName, ds.External.ChainInfoId, false, ds.External.Alias)
		if err != nil {
			s.logger.Errorf("Failed to get table info: %v, RequestId=%s", err, request.RequestId)
			return nil, err
		}

	case *pb.BatchReadRequest_Internal:
		// 内部数据源：创建默认的 ConnectionInfo 实例
		var targetTable string
		// 操作的中间表名
		var tempTable string
		// 根据操作类型确定目标表名
		switch op := request.Operation.(type) {
		case *pb.BatchReadRequest_Write:
			targetTable = op.Write.TargetTable
		case *pb.BatchReadRequest_Sort:
			targetTable = op.Sort.TargetTable
		case *pb.BatchReadRequest_PsiJoin:
			targetTable = op.PsiJoin.TargetTable
			tempTable = op.PsiJoin.TempTable
			jobInstanceId = op.PsiJoin.JobInstanceId
		case *pb.BatchReadRequest_Query:
			jobInstanceId = op.Query.JobInstanceId
			if jobInstanceId != "" {
				targetTable = jobInstanceId + "_" + ds.Internal.TableName
			} else {
				targetTable = ds.Internal.TableName
			}
			tempTable = targetTable

		case *pb.BatchReadRequest_Join:
			jobInstanceId = op.Join.JobInstanceId
			targetTable = ds.Internal.TableName
		case *pb.BatchReadRequest_AddHashColumn:
			jobInstanceId = op.AddHashColumn.JobInstanceId
			if jobInstanceId != "" {
				targetTable = jobInstanceId + "_" + ds.Internal.TableName
			} else {
				targetTable = ds.Internal.TableName
			}
			tempTable = targetTable
		default:
			// 对于查询、统计等操作，使用内部数据源的表名
			targetTable = ds.Internal.TableName

		}

		connInfo = &pb.ConnectionInfo{
			Dbtype:    utils.GetDbTypeFromName(conf.Dbms.Type),
			Host:      conf.Dbms.Host,
			Port:      conf.Dbms.Port,
			User:      conf.Dbms.User,
			Password:  conf.Dbms.Password,
			DbName:    ds.Internal.DbName,
			TableName: targetTable,
			Columns:   []*pb.ColumnItem{},
		}
		tableInfo, err = tableInfoService.GetTableInfoByDataSource(connInfo, tempTable, false)
		if err != nil {
			s.logger.Errorf("Failed to get table info: %v, RequestId=%s", err, request.RequestId)
			return nil, err
		}

	default:
		return nil, fmt.Errorf("未指定数据源类型")
	}

	// 在获取tableInfo后添加日志
	if tableInfo != nil {
		s.logger.Infof("Table info retrieved: TableName=%s, RecordCount=%d, RecordSize=%d bytes (%.2f MB)",
			tableInfo.GetTableName(),
			tableInfo.GetRecordCount(),
			tableInfo.GetRecordSize(),
			float64(tableInfo.GetRecordSize())/(1024*1024))
	} else {
		s.logger.Warnf("Table info is nil, will use default Spark config")
	}

	// 根据表信息调整Spark配置
	adjustedSparkConfig := AdjustSparkConfigByTableInfo(tableInfo, request.SparkConfig)
	s.logger.Infof("Adjusted Spark config based on table info: RecordCount=%d, RecordSize=%d, MaxExecutors=%d, ExecutorMemoryMB=%d",
		tableInfo.GetRecordCount(), tableInfo.GetRecordSize(),
		adjustedSparkConfig.GetDynamicAllocationMaxExecutors(), adjustedSparkConfig.GetExecutorMemoryMB())

	// 根据操作类型构建 SparkDBConnInfo
	sparkConnInfo := &pb.SparkDBConnInfo{}
	sparkConnInfo.StorageType = pb.StorageType_STORAGE_TYPE_DB
	sparkConnInfo.BucketName = common.BATCH_DATA_BUCKET_NAME

	// 设置数据库连接信息（如果有的话）
	if connInfo != nil {
		sparkConnInfo.DbType = getDbTypeString(connInfo.Dbtype)
		sparkConnInfo.Host = connInfo.Host
		sparkConnInfo.Port = connInfo.Port
		sparkConnInfo.Database = connInfo.DbName
		sparkConnInfo.Username = connInfo.User
		sparkConnInfo.Password = connInfo.Password
		// 将 ColumnItem 切片转换为字符串切片
		var dbFields []string
		for _, column := range connInfo.Columns {
			dbFields = append(dbFields, column.Name)
		}
		sparkConnInfo.DbFields = dbFields
	}

	// 根据具体操作类型设置参数
	switch op := request.Operation.(type) {
	case *pb.BatchReadRequest_Query:
		sparkConnInfo.DbFields = op.Query.DbFields   // 查询字段
		sparkConnInfo.SortRules = op.Query.SortRules // 排序规则
		sparkConnInfo.Mode = pb.OperationMode_OPERATION_MODE_QUERY

	case *pb.BatchReadRequest_Write:
		sparkConnInfo.DataObject = op.Write.DataObject
		sparkConnInfo.TargetTable = op.Write.TargetTable
		sparkConnInfo.Mode = pb.OperationMode_OPERATION_MODE_WRITE

	case *pb.BatchReadRequest_Sort:
		sparkConnInfo.DataObject = op.Sort.DataObject
		sparkConnInfo.OrderByColumn = op.Sort.OrderByColumn
		sparkConnInfo.TargetTable = op.Sort.TargetTable
		sparkConnInfo.Mode = pb.OperationMode_OPERATION_MODE_SORT

	case *pb.BatchReadRequest_Count:
		sparkConnInfo.Mode = pb.OperationMode_OPERATION_MODE_COUNT
		// 对于统计操作，通常针对内部数据源
		if internal := request.GetInternal(); internal != nil {
			sparkConnInfo.Query = fmt.Sprintf("SELECT COUNT(*) FROM %s", internal.TableName)
		}

	case *pb.BatchReadRequest_GroupbyCount:
		sparkConnInfo.Mode = pb.OperationMode_OPERATION_MODE_GROUPBY_COUNT
		// 构建分组统计查询
		if internal := request.GetInternal(); internal != nil {
			groupByFields := strings.Join(op.GroupbyCount.GroupByFields, ", ")
			sparkConnInfo.Query = fmt.Sprintf("SELECT %s, COUNT(*) FROM %s GROUP BY %s",
				groupByFields, internal.TableName, groupByFields)
		}

	case *pb.BatchReadRequest_Join:
		sparkConnInfo.JoinColumns = op.Join.JoinColumns
		sparkConnInfo.JoinType = op.Join.JoinType
		sparkConnInfo.DataObject = op.Join.DataObject
		if jobInstanceId != "" {
			sparkConnInfo.TempTable = op.Join.JobInstanceId + "_" + op.Join.TempTable
		} else {
			sparkConnInfo.TempTable = op.Join.TempTable
		}
		sparkConnInfo.Mode = pb.OperationMode_OPERATION_MODE_JOIN

	case *pb.BatchReadRequest_AddHashColumn:
		sparkConnInfo.TempTable = op.AddHashColumn.TempTable
		sparkConnInfo.JoinColumns = op.AddHashColumn.JoinColumns // 这里复用 JoinColumns 存储哈希列
		sparkConnInfo.Mode = pb.OperationMode_OPERATION_MODE_ADD_HASH_COLUMN
		sparkConnInfo.DbFields = op.AddHashColumn.Columns

	case *pb.BatchReadRequest_PsiJoin:
		sparkConnInfo.InObjects = op.PsiJoin.InObjects
		sparkConnInfo.JoinColumns = op.PsiJoin.JoinColumns
		sparkConnInfo.TargetTable = op.PsiJoin.TargetTable
		if jobInstanceId != "" {
			sparkConnInfo.TargetTable = jobInstanceId + "_" + op.PsiJoin.TargetTable
		} else {
			sparkConnInfo.TargetTable = op.PsiJoin.TargetTable
		}
		sparkConnInfo.TempTable = op.PsiJoin.TempTable
		sparkConnInfo.OrderByColumn = op.PsiJoin.OrderByColumn
		sparkConnInfo.DataObject = op.PsiJoin.DataObject
		sparkConnInfo.Mode = pb.OperationMode_OPERATION_MODE_PSI_JOIN

	default:
		return nil, fmt.Errorf("不支持的操作类型")
	}

	// 创建 Spark Pod
	podName := "spark-job-" + request.RequestId
	pod, err := utils.CreateSparkPod(s.k8sClient, podName, sparkConnInfo, adjustedSparkConfig, connInfo, tableInfo.GetRecordCount())
	if err != nil {
		s.logger.Errorf("Failed to create Pod: %v", err)
		return nil, err
	}

	// 打印 Pod 信息
	s.logPodInfo(pod)

	cleanPod := conf.CleanupConfig.Enable

	if cleanPod {
		s.logger.Infof("Starting Pod cleanup for %s", podName)
		go s.monitorJob(podName)
	}

	// 返回初始状态
	return &pb.BatchResponse{
		Status: pb.JobStatus_JOB_STATUS_PENDING,
		JobId:  podName,
	}, nil
}

// 修改GetJobStatus函数，尝试从Redis获取信息
func (s *k8sService) GetJobStatus(ctx context.Context, request *pb.JobStatusRequest) (*pb.BatchResponse, error) {
	podName := request.GetJobId() // podName
	s.logger.Debugf("查询作业状态: %s", podName)

	// 先检查Pod状态
	pod, err := s.findJobPod(ctx, podName)
	if err != nil {
		// 如果Pod不存在，检查Redis中是否有结果
		resultStr, err := s.redisClient.Get(ctx, podName).Result()
		if err == nil && resultStr != "" {
			// Redis中有数据，说明任务已经成功完成
			return &pb.BatchResponse{
				Status: pb.JobStatus_JOB_STATUS_SUCCEEDED,
				Data:   resultStr,
			}, nil
		}

		// Redis中也没有数据，返回错误
		s.logger.Errorf("获取Pod失败且Redis中无数据: %v", err)
		return nil, err
	}

	// 根据Pod状态处理
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		// Pod成功，从Redis获取详细数据
		resultStr, err := s.redisClient.Get(ctx, podName).Result()
		if err == nil && resultStr != "" {
			return &pb.BatchResponse{
				Status: pb.JobStatus_JOB_STATUS_SUCCEEDED,
				Data:   resultStr,
			}, nil
		}

		// Redis没有数据，返回成功但无数据
		return &pb.BatchResponse{
			Status: pb.JobStatus_JOB_STATUS_SUCCEEDED,
		}, nil

	case corev1.PodFailed:
		// Pod失败，返回失败状态
		errorMsg := getPodFailureReason(pod)
		return &pb.BatchResponse{
			Status: pb.JobStatus_JOB_STATUS_FAILED,
			Error:  errorMsg,
		}, nil

	case corev1.PodRunning:
		// Pod运行中，返回运行状态
		return &pb.BatchResponse{
			Status: pb.JobStatus_JOB_STATUS_RUNNING,
		}, nil

	default:
		// 其他状态，返回待处理状态
		return &pb.BatchResponse{
			Status: pb.JobStatus_JOB_STATUS_PENDING,
		}, nil
	}
}

// 获取 Pod 失败原因
func getPodFailureReason(pod *corev1.Pod) string {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionFalse {
			return condition.Message
		}
	}
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
			return fmt.Sprintf("Container %s failed with exit code %d: %s",
				containerStatus.Name,
				containerStatus.State.Terminated.ExitCode,
				containerStatus.State.Terminated.Message)
		}
	}
	return "Unknown failure reason"
}

func (s *k8sService) findJobPod(ctx context.Context, podName string) (*corev1.Pod, error) {
	return s.k8sClient.CoreV1().Pods(config.GetConfigMap().SparkPodConfig.Namespace).Get(ctx, podName, metav1.GetOptions{})
}

// 后台监控作业状态，清理已完成的Pod
func (s *k8sService) monitorJob(podName string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Get Pod status
		pod, err := s.k8sClient.CoreV1().Pods(config.GetConfigMap().SparkPodConfig.Namespace).Get(context.Background(), podName, metav1.GetOptions{})
		if err != nil {
			s.logger.Errorf("Failed to get Pod status: %v", err)
			return
		}

		switch pod.Status.Phase {
		case corev1.PodRunning:
			s.logger.Debugf("Pod %s is running", podName)
		case corev1.PodSucceeded:
			s.logger.Infof("Pod %s completed successfully", podName)
			time.Sleep(10 * time.Second)
			// Delete successfully completed Pod
			s.deletePod(podName)
			return
		case corev1.PodFailed:
			s.logger.Errorf("Pod %s failed: %s", podName, getPodFailureReason(pod))
			s.deletePod(podName)
			return
		}
	}
}

// Delete Pod
func (s *k8sService) deletePod(podName string) {
	s.logger.Infof("Deleting Pods starting with %s", podName)

	// Get all Pods
	pods, err := s.k8sClient.CoreV1().Pods(config.GetConfigMap().SparkPodConfig.Namespace).List(
		context.Background(),
		metav1.ListOptions{},
	)
	if err != nil {
		s.logger.Errorf("Failed to get Pod list: %v", err)
		return
	}

	// Delete Pods starting with podName
	for _, pod := range pods.Items {
		if strings.HasPrefix(pod.Name, podName) {
			err := s.k8sClient.CoreV1().Pods(config.GetConfigMap().SparkPodConfig.Namespace).Delete(
				context.Background(),
				pod.Name,
				metav1.DeleteOptions{},
			)
			if err != nil {
				s.logger.Warnf("Failed to delete Pod %s: %v", pod.Name, err)
			} else {
				s.logger.Infof("Successfully deleted Pod: %s", pod.Name)
			}
		}
	}
}

// 辅助函数：将数据库类型转换为字符串
func getDbTypeString(dbtype int32) string {
	switch dbtype {
	case 1:
		return "mysql"
	case 2:
		return "kingbase"
	case 3:
		return "hive"
	case 4:
		return "tidb"
	case 5:
		return "tdsql"
	default:
		return "unknown"
	}
}
