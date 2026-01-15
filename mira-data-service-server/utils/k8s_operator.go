/*
*

	@author: shiliang
	@date: 2024/9/23
	@note: k8s的相关操作

*
*/
package utils

import (
	"context"
	"data-service/config"
	pb "data-service/generated/datasource"
	"data-service/log"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func CreateSparkPod(clientset *kubernetes.Clientset, podName string, info *pb.SparkDBConnInfo, sparkConfig *pb.SparkConfig, connInfo *pb.ConnectionInfo, recordCount int32) (*v1.Pod, error) {
	conf := config.GetConfigMap()
	imageFullName := conf.SparkPodConfig.ImageName + ":" + conf.SparkPodConfig.ImageTag
	minioEndpoint := fmt.Sprintf("%s:%d", conf.OSSConfig.Host, conf.OSSConfig.Port)
	minioAccessKey := conf.OSSConfig.AccessKey
	minioSecretKey := conf.OSSConfig.SecretKey
	hostPort := strings.Split(conf.RedisConfig.Address, ":")
	if len(hostPort) != 2 {
		return nil, fmt.Errorf("invalid redis address format: %s", conf.RedisConfig.Address)
	}
	redisHost := hostPort[0]
	redisPort, err := strconv.Atoi(hostPort[1])
	if err != nil {
		return nil, fmt.Errorf("invalid redis port: %s", hostPort[1])
	}

	// Generate dynamic configuration arguments
	sparkConfArgs := GenerateSparkConfArgs(sparkConfig, recordCount)
	log.Logger.Infof("Generated Spark configuration arguments: %v", sparkConfArgs)
	// Spark-submit arguments
	sparkSubmitArgs := []string{
		"/opt/spark/bin/spark-submit",
		"--master", conf.SparkPodConfig.Master,
		"--deploy-mode", "cluster",
		"--conf", "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3",
		"--conf", fmt.Sprintf("spark.kubernetes.namespace=%s", conf.SparkPodConfig.Namespace),
		"--conf", fmt.Sprintf("spark.kubernetes.driver.container.image=%s", imageFullName),
		"--conf", fmt.Sprintf("spark.kubernetes.executor.container.image=%s", imageFullName),
		"--conf", fmt.Sprintf("spark.kubernetes.authenticate.driver.serviceAccountName=%s", conf.SparkPodConfig.AccountName), // Driver的ServiceAccount
		"--conf", fmt.Sprintf("spark.kubernetes.authenticate.executor.serviceAccountName=%s", conf.SparkPodConfig.AccountName), // Executor的ServiceAccount
		"--conf", fmt.Sprintf("spark.kubernetes.file.upload.path=%s", conf.SparkPodConfig.UploadPath),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.access.key=%s", minioAccessKey),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.secret.key=%s", minioSecretKey),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.endpoint=http://%s", minioEndpoint),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"),
		"--conf", fmt.Sprintf("spark.io.compression.codec=snappy"),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.path.style.access=true"),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.connection.ssl.enabled=false"),
		"--conf", fmt.Sprintf("spark.kubernetes.app.name=%s", podName), // 设置应用名称
		"--conf", fmt.Sprintf("spark.app.name=%s", podName),
		"--conf", fmt.Sprintf("spark.stage.retryWait=30"), // 修改失败重试等待时间为30秒，不设置则为1秒
	}

	// Main script arguments
	mainScriptArgs := []string{
		"--py-files", "/opt/spark/jars/spark-job.zip",
		"/opt/spark/jars/unzipped/__main__.py",
		"--bucket", info.BucketName,
		"--query", info.Query,
		"--dataobject", info.DataObject,
		"--serverip", conf.HttpServiceConfig.DataServer,
		"--serverport", fmt.Sprintf("%d", conf.HttpServiceConfig.Port),
		"--endpoint", minioEndpoint,
		"--accesskey", minioAccessKey,
		"--secretkey", minioSecretKey,
		"--dbType", strconv.Itoa(int(connInfo.Dbtype)),
		"--host", connInfo.Host,
		"--port", strconv.Itoa(int(connInfo.Port)),
		"--username", connInfo.User,
		"--password", connInfo.Password,
		"--dbName", connInfo.DbName,
		"--join_columns", strings.Join(info.JoinColumns, ","),
		"--orderby_column", info.OrderByColumn,
		"--mode", OperationModeToString(info.Mode),
		"--db_table", connInfo.TableName,
		"--join_type", JoinTypeToString(info.JoinType),
		"--partitions", strconv.Itoa(int(sparkConfig.NumPartitions)),
		"--embedded_dbType", strconv.Itoa(int(GetDbTypeFromName(conf.Dbms.Type))),
		"--embedded_host", conf.Dbms.Host,
		"--embedded_port", strconv.Itoa(int(conf.Dbms.Port)),
		"--embedded_user", conf.Dbms.User,
		"--embedded_password", conf.Dbms.Password,
		"--embedded_dbName", "mira_task_tmp",
		"--embedded_table", info.TempTable,
		"--redis_host", redisHost,
		"--redis_port", strconv.Itoa(redisPort),
		"--redis_password", conf.RedisConfig.Password,
		"--redis_db", strconv.Itoa(int(conf.RedisConfig.DB)),
		"--redis_cluster_type", conf.RedisConfig.ClusterType,
		"--redis_sentinel_master_name", conf.RedisConfig.SentinelMasterName,
		"--podName", podName,
		"--target_table", info.TargetTable,
		"--storage_type", StorageTypeToString(info.StorageType),
	}

	if len(info.InColumns) > 0 {
		mainScriptArgs = append(mainScriptArgs, "--incolumns")
		mainScriptArgs = append(mainScriptArgs, strings.Join(info.InColumns, ","))
	}

	if len(info.InObjects) > 0 {
		mainScriptArgs = append(mainScriptArgs, "--inobjects")
		mainScriptArgs = append(mainScriptArgs, strings.Join(info.InObjects, ","))
	}

	if len(info.DbFields) > 0 {
		mainScriptArgs = append(mainScriptArgs, "--columns")
		mainScriptArgs = append(mainScriptArgs, strings.Join(info.DbFields, ","))
	}

	if len(info.SortRules) > 0 {
		sortColumns := make([]string, 0)
		for _, sortRule := range info.SortRules {
			sortColumns = append(sortColumns, sortRule.FieldName)
		}
		mainScriptArgs = append(mainScriptArgs, "--sort_columns")
		mainScriptArgs = append(mainScriptArgs, strings.Join(sortColumns, ","))
		mainScriptArgs = append(mainScriptArgs, "--sort_types")
		sortOrders := make([]string, 0)
		for _, sortRule := range info.SortRules {
			sortOrders = append(sortOrders, SortOrderToString(sortRule.SortOrder))
		}
		mainScriptArgs = append(mainScriptArgs, strings.Join(sortOrders, ","))
	}

	// Combine all arguments
	allArgs := append(append(sparkSubmitArgs, sparkConfArgs...), mainScriptArgs...)
	log.Logger.Infof("Generated all arguments: %v", allArgs)

	// 打印标签
	log.Logger.Infof("NodeSelector: %v", conf.SparkPodConfig.NodeSelector)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: conf.SparkPodConfig.Namespace,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: conf.SparkPodConfig.AccountName,
			Containers: []corev1.Container{
				{
					Name:            "spark-container",
					Image:           imageFullName,           // 替换为实际的 Spark 镜像
					ImagePullPolicy: corev1.PullIfNotPresent, // 镜像拉取策略
					Args:            allArgs,
					Ports: []corev1.ContainerPort{
						{
							Name:          "spark-port",
							ContainerPort: 7077,
						},
					},
				},
			},
			Volumes:       []corev1.Volume{},
			RestartPolicy: corev1.RestartPolicyNever,
			DNSPolicy:     corev1.DNSClusterFirst,
			DNSConfig: &corev1.PodDNSConfig{
				Searches: []string{},
			},
		},
	}

	return clientset.CoreV1().Pods(conf.SparkPodConfig.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
}

// GenerateSparkConfArgs generates the --conf arguments for Spark based on the provided SparkConfig.
func GenerateSparkConfArgs(sparkConfig *pb.SparkConfig, recordCount int32) []string {
	if sparkConfig == nil {
		return []string{}
	}

	var args []string
	conf := config.GetConfigMap()

	// 动态分配配置
	if sparkConfig.DynamicAllocationEnabled {
		args = append(args, "--conf", "spark.dynamicAllocation.enabled=true")
		if sparkConfig.DynamicAllocationMinExecutors > 0 {
			args = append(args, "--conf", fmt.Sprintf("spark.dynamicAllocation.minExecutors=%d", sparkConfig.DynamicAllocationMinExecutors))
		}
		if sparkConfig.DynamicAllocationMaxExecutors > 0 {
			args = append(args, "--conf", fmt.Sprintf("spark.dynamicAllocation.maxExecutors=%d", sparkConfig.DynamicAllocationMaxExecutors))
		}
	}

	// 执行器配置
	if sparkConfig.ExecutorMemoryMB > 0 {
		args = append(args, "--conf", fmt.Sprintf("spark.executor.memory=%dm", sparkConfig.ExecutorMemoryMB))
	}
	if sparkConfig.ExecutorCores > 0 {
		args = append(args, "--conf", fmt.Sprintf("spark.executor.cores=%d", sparkConfig.ExecutorCores))
	}

	// 驱动程序配置
	if sparkConfig.DriverMemoryMB > 0 {
		args = append(args, "--conf", fmt.Sprintf("spark.driver.memory=%dm", sparkConfig.DriverMemoryMB))
	}
	if sparkConfig.DriverCores > 0 {
		args = append(args, "--conf", fmt.Sprintf("spark.driver.cores=%d", sparkConfig.DriverCores))
	}

	// 并行度
	if sparkConfig.Parallelism > 0 {
		args = append(args, "--conf", fmt.Sprintf("spark.default.parallelism=%d", sparkConfig.Parallelism))
	}

	log.Logger.Infof("Event log enabled: %t, dir: %s", conf.SparkPodConfig.EventLogEnable, conf.SparkPodConfig.EventLogDir)
	// 事件日志配置
	if conf.SparkPodConfig.EventLogEnable {
		args = append(args, "--conf", fmt.Sprintf("spark.eventLog.enabled=%t", conf.SparkPodConfig.EventLogEnable))
		args = append(args, "--conf", fmt.Sprintf("spark.eventLog.dir=%s", conf.SparkPodConfig.EventLogDir))
	}

	// NodeSelector，大于1000W条数据，放到内存大的机器上
	if recordCount > 10000000 {
		for k, v := range conf.SparkPodConfig.NodeSelector {
			args = append(args, "--conf", fmt.Sprintf("spark.kubernetes.driver.node.selector.%s=%s", k, v))
			args = append(args, "--conf", fmt.Sprintf("spark.kubernetes.executor.node.selector.%s=%s", k, v))
		}
	}

	// Tolerations
	args = append(args, "--conf", fmt.Sprintf("spark.kubernetes.driver.podTemplateFile=%s", "driver.yml"))
	args = append(args, "--conf", fmt.Sprintf("spark.kubernetes.executor.podTemplateFile=%s", "driver.yml"))

	log.Logger.Infof("Spark conf args: %v", args)
	return args
}

// 启动定时清理spark pod任务
func StartPodCleanupTask(namespace string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		<-ticker.C
		log.Logger.Infof("Starting cleanup task for namespace: %s\n", namespace)
		err := cleanSparkPods(namespace)
		if err != nil {
			log.Logger.Errorf("Error cleaning Spark pods: %v\n", err)
		} else {
			log.Logger.Infof("Successfully cleaned up Spark pods.")
		}
	}
}

// cleanSparkPods 清理指定 Namespace 中名称以 "spark" 开头的 Pod，不论状态
func cleanSparkPods(namespace string) error {
	// 创建与 Kubernetes 集群的配置
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to create in-cluster config: %v", err)
	}

	// 使用配置创建 Kubernetes 客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clientset: %v", err)
	}

	// 获取指定 Namespace 中所有 Pod
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %v", err)
	}

	// 删除名称以 "spark-job" 开头的 Pod
	for _, pod := range pods.Items {
		if strings.HasPrefix(pod.Name, "spark-job") && (pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded) {
			err := clientset.CoreV1().Pods(namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
			if err != nil {
				log.Logger.Errorf("Failed to delete pod %s: %v\n", pod.Name, err)
			} else {
				log.Logger.Infof("Deleted pod %s successfully\n", pod.Name)
			}
		}
	}
	return nil
}

func SortOrderToString(order pb.SortOrder) string {
	switch order {
	case pb.SortOrder_DESC:
		return "DESC"
	default:
		return "ASC"
	}
}
