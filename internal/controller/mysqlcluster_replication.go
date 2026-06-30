package controller

import (
	"bytes"
	"context"
	"fmt"

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

// checkReplication 检查主从复制状态，SHOW SLAVE STATUS 只看 IO/SQL 线程
func (r *MysqlClusterReconciler) checkReplication(ctx context.Context, m *mysqlv1.MysqlCluster) error {
	log := logf.FromContext(ctx)
	replicaPods, err := r.getPodsByRole(ctx, m.Namespace, m.Spec.ClusterName, "replica")
	if err != nil {
		return err
	}

	allHealthy := true
	for _, pod := range replicaPods {
		result, err := r.execSQL(ctx, m.Namespace, pod.Name,
			"SHOW SLAVE STATUS\\G")
		if err != nil {
			log.Error(err, "查询复制状态失败", "pod", pod.Name)
			allHealthy = false
			continue
		}
		if result == "" {
			log.Info("从库未配置复制，跳过检查", "pod", pod.Name)
			allHealthy = false
			continue
		}
		// 简单检查：有结果说明复制已配置，具体 IO/SQL 状态后续完善
	}

	if allHealthy {
		m.Status.Phase = "Running"
		m.Status.Ready = true
	} else {
		m.Status.Phase = "Degraded"
	}
	return r.Status().Update(ctx, m)
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
