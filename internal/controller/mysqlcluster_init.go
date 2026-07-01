package controller

import (
	"context"
	"fmt"
	mysqlv1 "mysql-cluster-operator/api/v1"
	"mysql-cluster-operator/internal/utils"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// initialize 初始化 MySQL 集群：创建 Service、PVC、Pod、ConfigMap，执行 MySQL 主从初始化
func (r *MysqlClusterReconciler) initialize(ctx context.Context, m *mysqlv1.MysqlCluster) error {
	log := log.FromContext(ctx)
	log.Info("开始初始化 MySQL 集群", "cluster", m.Spec.ClusterName)

	// 1. 创建两个 Service（mysql-primary、mysql-replica）
	if _, err := utils.GetOrCreateService(ctx, r.Client, r.Scheme, m, m.Namespace, m.Spec.ClusterName, m.Spec.PrimaryServiceName, "primary", false); err != nil {
		return err
	}
	if _, err := utils.GetOrCreateService(ctx, r.Client, r.Scheme, m, m.Namespace, m.Spec.ClusterName, m.Spec.ReplicaServiceName, "replica", false); err != nil {
		return err
	}

	// 2.读取副本数
	replicas := m.Spec.Replicas
	if replicas < 1 {
		return fmt.Errorf("副本数不能小于 1，当前副本数：%d", replicas)
	}
	if m.Spec.RootPasswordSecretRef == nil {
		return fmt.Errorf("rootPasswordSecretRef 不能为空")
	}

	// 3. 创建 ConfigMap（my.cnf），每个 Pod 一份
	for i := int32(0); i < replicas+1; i++ {
		cmName := fmt.Sprintf("%s-%d-config", m.Spec.ClusterName, i)
		config := strings.ReplaceAll(m.Spec.MysqlConfig, "{server-id}", fmt.Sprintf("%d", 100+i))
		if _, err := utils.GetOrCreateConfigMap(ctx, r.Client, r.Scheme, m, m.Namespace, m.Spec.ClusterName,
			cmName, config); err != nil {
			return err
		}
	}

	// 4. 根据副本数，创建 PVC 和 Pod
	for i := int32(0); i < replicas+1; i++ {
		podName := fmt.Sprintf("%s-%d", m.Spec.ClusterName, i)
		pvcName := fmt.Sprintf("data-%s-%d", m.Spec.ClusterName, i)

		if _, err := utils.GetOrCreatePVC(ctx, r.Client, r.Scheme, m, m.Namespace, m.Spec.ClusterName,
			pvcName, m.Spec.Storage.Size, m.Spec.Storage.StorageClassName); err != nil {
			return err
		}

		role := "replica"
		if i == 0 {
			role = "primary"
		}
		if _, err := utils.GetOrCreatePod(ctx, r.Client, r.Scheme, m, utils.PodConfig{
			Name:               podName,
			Namespace:          m.Namespace,
			ClusterName:        m.Spec.ClusterName,
			Index:              i,
			Role:               role,
			Image:              m.Spec.Image,
			PVCName:            pvcName,
			ConfigMapName:      fmt.Sprintf("%s-%d-config", m.Spec.ClusterName, i),
			RootPasswordSecret: m.Spec.RootPasswordSecretRef,
			Resources:          m.Spec.Resources,
			ReadinessProbe:     m.Spec.ReadinessProbe,
		}); err != nil {
			log.Error(err, "创建 Pod 失败，将在后续调谐中重试", "pod", podName)
		}
	}

	// 5. 等待 Pod Ready 并建立主从关系
	if err := r.waitForPodsReady(ctx, m.Namespace, m.Spec.ClusterName, replicas+1, 2*time.Minute); err != nil {
		return fmt.Errorf("等待 Pod 就绪超时: %w", err)
	}
	if err := r.setupReplication(ctx, m); err != nil {
		return fmt.Errorf("建立主从失败: %w", err)
	}

	return nil
}

// waitForPodsReady 等待所有 Pod 就绪
func (r *MysqlClusterReconciler) waitForPodsReady(ctx context.Context, namespace, clusterName string, expected int32, timeout time.Duration) error {
	log := log.FromContext(ctx)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		readyCount := int32(0)
		for i := int32(0); i < expected; i++ {
			podName := fmt.Sprintf("%s-%d", clusterName, i)
			var pod corev1.Pod
			if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podName}, &pod); err != nil {
				continue
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					readyCount++
					break
				}
			}
		}
		if readyCount >= expected {
			log.Info("所有 Pod 已就绪", "count", readyCount)
			return nil
		}
		log.Info("等待 Pod 就绪", "ready", readyCount, "expected", expected)
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("超时：期待 %d 个 Pod 就绪", expected)
}
