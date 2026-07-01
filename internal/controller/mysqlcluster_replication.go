package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1 "mysql-cluster-operator/api/v1"
)

// setupReplication 建立主从复制：主库创建 repl 账号，从库 CHANGE MASTER TO + START SLAVE
func (r *MysqlClusterReconciler) setupReplication(ctx context.Context, m *mysqlv1.MysqlCluster) error {
	log := logf.FromContext(ctx)
	log.Info("开始建立主从复制")

	// 通过 role=primary 标签找到主库 Pod
	primaryPod, err := r.getPodByRole(ctx, m.Namespace, m.Spec.ClusterName, "primary")
	if err != nil {
		return fmt.Errorf("获取主库 Pod 失败: %w", err)
	}

	// 1. 主库创建复制用户
	log.Info("主库创建复制用户", "pod", primaryPod.Name)
	if _, err := r.execSQL(ctx, m.Namespace, primaryPod.Name,
		"CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl_password'; GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'; FLUSH PRIVILEGES;"); err != nil {
		return fmt.Errorf("主库创建复制用户失败: %w", err)
	}

	// 2. 遍历从库，配置复制
	replicaPods, err := r.getPodsByRole(ctx, m.Namespace, m.Spec.ClusterName, "replica")
	if err != nil {
		return fmt.Errorf("获取从库 Pod 失败: %w", err)
	}

	for _, pod := range replicaPods {
		log.Info("从库配置复制", "pod", pod.Name)
		sql := fmt.Sprintf(
			"STOP SLAVE; CHANGE MASTER TO MASTER_HOST='%s.%s.svc.cluster.local', MASTER_USER='repl', MASTER_PASSWORD='repl_password', MASTER_AUTO_POSITION=1; START SLAVE;",
			m.Spec.PrimaryServiceName, m.Namespace,
		)
		if _, err := r.execSQL(ctx, m.Namespace, pod.Name, sql); err != nil {
			log.Error(err, "从库配置复制失败", "pod", pod.Name)
			continue
		}
	}

	return nil
}

// reconcileMasterSlave 主从健康检查与故障切换
func (r *MysqlClusterReconciler) reconcileMasterSlave(ctx context.Context, m *mysqlv1.MysqlCluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	ready, err := r.isPrimaryReady(ctx, m.Namespace, m.Spec.ClusterName)
	if err != nil {
		log.Error(err, "检测主库状态失败")
		return ctrl.Result{}, err
	}

	if !ready {
		log.Info("主库未就绪，触发故障切换")
		if err := r.failover(ctx, m); err != nil {
			log.Error(err, "故障切换失败")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	log.Info("主库就绪，检查从库复制状态")
	if err := r.checkReplication(ctx, m); err != nil {
		log.Error(err, "检查复制状态失败")
	}
	return ctrl.Result{}, nil
}

// isPrimaryReady 检查主库 Pod 是否就绪
func (r *MysqlClusterReconciler) isPrimaryReady(ctx context.Context, namespace, clusterName string) (bool, error) {
	primaryPod, err := r.getPodByRole(ctx, namespace, clusterName, "primary")
	if err != nil {
		return false, err
	}
	for _, cond := range primaryPod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true, nil
		}
	}
	return false, nil
}

// failover 故障切换：选举 GTID 最大的从库提升为主库
func (r *MysqlClusterReconciler) failover(ctx context.Context, m *mysqlv1.MysqlCluster) error {
	log := logf.FromContext(ctx)
	log.Info("开始故障切换")

	// 1. 找到所有 Ready 的从库
	replicaPods, err := r.getPodsByRole(ctx, m.Namespace, m.Spec.ClusterName, "replica")
	if err != nil {
		return fmt.Errorf("获取从库列表失败: %w", err)
	}
	if len(replicaPods) == 0 {
		return fmt.Errorf("没有从库可用于故障切换")
	}

	// 在线从库必须过半数才允许自动故障切换
	healthyCount := 0
	for i := range replicaPods {
		if r.isPodReady(&replicaPods[i]) {
			healthyCount++
		}
	}
	if healthyCount <= int(m.Spec.Replicas)/2 {
		return fmt.Errorf("在线从库未过半数（%d/%d），拒绝自动故障切换", healthyCount, m.Spec.Replicas)
	}

	// 2. 选举：GTID 最大的从库
	newPrimary, err := r.electNewPrimary(ctx, m, replicaPods)
	if err != nil {
		return fmt.Errorf("选举新主库失败: %w", err)
	}
	log.Info("选举新主库", "pod", newPrimary.Name)

	// 3. 新主库：停止复制，关闭只读
	if _, err := r.execSQL(ctx, m.Namespace, newPrimary.Name,
		"STOP SLAVE; RESET SLAVE ALL; SET GLOBAL read_only=OFF;"); err != nil {
		return fmt.Errorf("新主库切换失败: %w", err)
	}

	// 4. 改标签
	if err := r.updatePodLabel(ctx, m.Namespace, newPrimary.Name, "role", "primary"); err != nil {
		return fmt.Errorf("更新新主库标签失败: %w", err)
	}

	// 5. 删除旧主库 Pod（PVC 保留）
	if err := r.deletePodByName(ctx, m.Namespace, fmt.Sprintf("%s-0", m.Spec.ClusterName)); err != nil {
		log.Error(err, "删除旧主库 Pod 失败")
	}

	// 6. 其余从库重连新主
	for _, pod := range replicaPods {
		if pod.Name == newPrimary.Name {
			continue
		}
		sql := fmt.Sprintf(
			"STOP SLAVE; CHANGE MASTER TO MASTER_HOST='%s.%s.svc.cluster.local', MASTER_USER='repl', MASTER_PASSWORD='repl_password', MASTER_AUTO_POSITION=1; START SLAVE;",
			m.Spec.PrimaryServiceName, m.Namespace,
		)
		if _, err := r.execSQL(ctx, m.Namespace, pod.Name, sql); err != nil {
			log.Error(err, "从库重连新主失败", "pod", pod.Name)
		}
	}

	// 7. 更新状态
	m.Status.PrimaryPod = newPrimary.Name
	m.Status.Phase = "Failover"
	m.Status.Ready = false
	return r.Status().Update(ctx, m)
}

// electNewPrimary 评分选举新主库：GTID 优先 → 延迟最低 → Pod 最稳定
func (r *MysqlClusterReconciler) electNewPrimary(ctx context.Context, m *mysqlv1.MysqlCluster, replicas []corev1.Pod) (*corev1.Pod, error) {
	type candidate struct {
		pod    *corev1.Pod
		gtid   string
		lag    int // Seconds_Behind_Master，-1 表示不可用
	}

	var candidates []candidate
	for i := range replicas {
		pod := &replicas[i]
		if !r.isPodReady(pod) {
			continue
		}
		gtid, err := r.getGTID(ctx, m.Namespace, pod.Name)
		if err != nil {
			continue
		}
		lag := r.getReplicaLag(ctx, m.Namespace, pod.Name)
		candidates = append(candidates, candidate{pod: pod, gtid: gtid, lag: lag})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("没有可用的从库")
	}

	// 评分：GTID 最大 → 延迟最低 → Pod 创建最早
	best := candidates[0]
	for _, c := range candidates[1:] {
		if compareGTID(c.gtid, best.gtid) > 0 {
			best = c
		} else if c.gtid == best.gtid {
			if c.lag >= 0 && (best.lag < 0 || c.lag < best.lag) {
				best = c
			} else if c.lag == best.lag && c.pod.CreationTimestamp.Before(&best.pod.CreationTimestamp) {
				best = c
			}
		}
	}
	return best.pod, nil
}

// getReplicaLag 获取复制延迟（Seconds_Behind_Master），失败返回 -1
func (r *MysqlClusterReconciler) getReplicaLag(ctx context.Context, namespace, podName string) int {
	result, err := r.execSQL(ctx, namespace, podName,
		"SELECT COALESCE(Seconds_Behind_Master, -1) FROM performance_schema.replication_applier_status LIMIT 1")
	if err != nil {
		return -1
	}
	result = strings.TrimSpace(result)
	var lag int
	if _, e := fmt.Sscanf(result, "%d", &lag); e != nil {
		return -1
	}
	return lag
}

// compareGTID 比较两段 GTID 集，返回 1=新>x, -1=x>新, 0=持平
func compareGTID(a, b string) int {
	if len(a) > len(b) {
		return 1
	}
	if len(a) < len(b) {
		return -1
	}
	if a > b {
		return 1
	}
	if a < b {
		return -1
	}
	return 0
}

// getGTID 获取 Pod 的 GTID 值
func (r *MysqlClusterReconciler) getGTID(ctx context.Context, namespace, podName string) (string, error) {
	return r.execSQL(ctx, namespace, podName, "SELECT @@global.gtid_executed")
}

// isPodReady 检查 Pod 是否就绪
func (r *MysqlClusterReconciler) isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// updatePodLabel 更新 Pod 标签
func (r *MysqlClusterReconciler) updatePodLabel(ctx context.Context, namespace, podName, key, value string) error {
	var pod corev1.Pod
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podName}, &pod); err != nil {
		return err
	}
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels["role"] = value
	return r.Update(ctx, &pod)
}

// deletePodByName 删除 Pod
func (r *MysqlClusterReconciler) deletePodByName(ctx context.Context, namespace, podName string) error {
	pod := &corev1.Pod{}
	pod.SetName(podName)
	pod.SetNamespace(namespace)
	return client.IgnoreNotFound(r.Delete(ctx, pod))
}

// checkReplication 检查从库复制状态，异常则尝试修复
func (r *MysqlClusterReconciler) checkReplication(ctx context.Context, m *mysqlv1.MysqlCluster) error {
	log := logf.FromContext(ctx)
	replicaPods, err := r.getPodsByRole(ctx, m.Namespace, m.Spec.ClusterName, "replica")
	if err != nil {
		return err
	}

	allHealthy := true
	for _, pod := range replicaPods {
		if !r.isPodReady(&pod) {
			log.Info("从库 Pod 未就绪，跳过检查", "pod", pod.Name)
			allHealthy = false
			continue
		}

		result, err := r.execSQL(ctx, m.Namespace, pod.Name, "SHOW SLAVE STATUS\\G")
		if err != nil || result == "" {
			log.Info("从库复制异常，尝试修复", "pod", pod.Name)
			r.repairReplica(ctx, m, pod.Name)
			allHealthy = false
			continue
		}

		// IO/SQL 线程异常 → 修复
		if !strings.Contains(result, "Slave_IO_Running: Yes") || !strings.Contains(result, "Slave_SQL_Running: Yes") {
			log.Info("从库 IO/SQL 线程异常，尝试修复", "pod", pod.Name)
			r.repairReplica(ctx, m, pod.Name)
			allHealthy = false
			continue
		}
	}

	if allHealthy {
		m.Status.Phase = "Running"
		m.Status.Ready = true
	} else {
		m.Status.Phase = "Degraded"
	}
	return r.Status().Update(ctx, m)
}

// repairReplica 修复从库复制：停止旧连接，重新 CHANGE MASTER TO + START SLAVE
func (r *MysqlClusterReconciler) repairReplica(ctx context.Context, m *mysqlv1.MysqlCluster, podName string) {
	log := logf.FromContext(ctx)
	sql := fmt.Sprintf(
		"STOP SLAVE; CHANGE MASTER TO MASTER_HOST='%s.%s.svc.cluster.local', MASTER_USER='repl', MASTER_PASSWORD='repl_password', MASTER_AUTO_POSITION=1; START SLAVE;",
		m.Spec.PrimaryServiceName, m.Namespace,
	)
	if _, err := r.execSQL(ctx, m.Namespace, podName, sql); err != nil {
		log.Error(err, "修复从库复制失败", "pod", podName)
	}
}

// syncGTID 采集所有节点 GTID，写入 Status
func (r *MysqlClusterReconciler) syncGTID(ctx context.Context, m *mysqlv1.MysqlCluster) error {
	allPods, err := r.getPodsByLabel(ctx, m.Namespace, m.Spec.ClusterName)
	if err != nil {
		return err
	}

	m.Status.Nodes = nil
	for _, pod := range allPods {
		gtid, err := r.execSQL(ctx, m.Namespace, pod.Name, "SELECT @@global.gtid_executed AS gtid")
		if err != nil {
			continue
		}
		m.Status.Nodes = append(m.Status.Nodes, mysqlv1.NodeStatus{
			Name: pod.Name,
			GTID: gtid,
		})
	}

	m.Status.LastGTIDSync = metav1.Now()
	return r.Status().Update(ctx, m)
}

// getPodByRole 通过 role 标签获取单个 Pod
func (r *MysqlClusterReconciler) getPodByRole(ctx context.Context, namespace, clusterName, role string) (*corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{
		"app":  clusterName,
		"role": role,
	}); err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("未找到 role=%s 的 Pod", role)
	}
	return &list.Items[0], nil
}

// getPodsByRole 通过 role 标签获取多个 Pod
func (r *MysqlClusterReconciler) getPodsByRole(ctx context.Context, namespace, clusterName, role string) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{
		"app":  clusterName,
		"role": role,
	}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// getPodsByLabel 通过 app 标签获取所有 Pod
func (r *MysqlClusterReconciler) getPodsByLabel(ctx context.Context, namespace, clusterName string) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{
		"app": clusterName,
	}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// execSQL 在指定 Pod 里执行 MySQL 命令，返回结果字符串
func (r *MysqlClusterReconciler) execSQL(ctx context.Context, namespace, podName, sqlCmd string) (string, error) {
	clientset, err := kubernetes.NewForConfig(r.RestConfig)
	if err != nil {
		return "", fmt.Errorf("创建 clientset 失败: %w", err)
	}

	cmd := []string{
		"mysql",
		"-u", "root",
		"-p${MYSQL_ROOT_PASSWORD}",
		"-e", sqlCmd,
	}

	var stdout, stderr bytes.Buffer
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "mysql",
			Command:   cmd,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(r.RestConfig, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("创建 executor 失败: %w", err)
	}

	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return "", fmt.Errorf("exec 执行失败: %w, stderr: %s", err, stderr.String())
	}

	return stdout.String(), nil
}
