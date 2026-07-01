package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1 "mysql-cluster-operator/api/v1"
	"mysql-cluster-operator/internal/utils"
)

func (r *MysqlClusterReconciler) reconcileReplicas(ctx context.Context, m *mysqlv1.MysqlCluster) (ctrl.Result, error) {
	expect := m.Spec.Replicas + 1

	pods, err := r.getPodsByLabel(ctx, m.Namespace, m.Spec.ClusterName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("获取 Pod 列表失败: %w", err)
	}

	// 期望 Pod 名集合
	expectNames := make(map[string]int32, expect)
	for i := int32(0); i < expect; i++ {
		expectNames[fmt.Sprintf("%s-%d", m.Spec.ClusterName, i)] = i
	}

	// 实际 Pod 名集合
	actualNames := make(map[string]struct{}, len(pods))
	for _, p := range pods {
		actualNames[p.Name] = struct{}{}
	}

	// 缺失的 Pod → 创建
	var missing []int32
	for name, idx := range expectNames {
		if _, ok := actualNames[name]; !ok {
			missing = append(missing, idx)
		}
	}

	// 多余的 Pod → 删除
	var extra []string
	for _, p := range pods {
		if _, ok := expectNames[p.Name]; !ok {
			extra = append(extra, p.Name)
		}
	}

	if len(missing) > 0 {
		r.scaleOut(ctx, m, missing)
	}
	if len(extra) > 0 {
		r.scaleIn(ctx, m, extra)
	}

	return ctrl.Result{}, nil
}

// scaleOut 扩容：为缺失的索引创建 CM + PVC + Pod
func (r *MysqlClusterReconciler) scaleOut(ctx context.Context, m *mysqlv1.MysqlCluster, missing []int32) {
	log := logf.FromContext(ctx)

	for _, i := range missing {
		podName := fmt.Sprintf("%s-%d", m.Spec.ClusterName, i)
		pvcName := fmt.Sprintf("data-%s-%d", m.Spec.ClusterName, i)
		cmName := fmt.Sprintf("%s-%d-config", m.Spec.ClusterName, i)
		config := strings.ReplaceAll(m.Spec.MysqlConfig, "{server-id}", fmt.Sprintf("%d", 100+i))

		log.Info("扩容创建资源", "index", i, "pod", podName)

		if _, err := utils.GetOrCreateConfigMap(ctx, r.Client, r.Scheme, m, m.Namespace, m.Spec.ClusterName,
			cmName, config); err != nil {
			log.Error(err, "扩容创建 ConfigMap 失败", "configmap", cmName)
			continue
		}

		if _, err := utils.GetOrCreatePVC(ctx, r.Client, r.Scheme, m, m.Namespace, m.Spec.ClusterName,
			pvcName, m.Spec.Storage.Size, m.Spec.Storage.StorageClassName); err != nil {
			log.Error(err, "扩容创建 PVC 失败", "pvc", pvcName)
			continue
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
			ConfigMapName:      cmName,
			RootPasswordSecret: m.Spec.RootPasswordSecretRef,
			Resources:          m.Spec.Resources,
			ReadinessProbe:     m.Spec.ReadinessProbe,
		}); err != nil {
			log.Error(err, "扩容创建 Pod 失败", "pod", podName)
		}
	}
}

// scaleIn 缩容：删除多余的 Pod + PVC，跳过 Pod-0
func (r *MysqlClusterReconciler) scaleIn(ctx context.Context, m *mysqlv1.MysqlCluster, extra []string) {
	log := logf.FromContext(ctx)

	for _, podName := range extra {
		log.Info("缩容删除资源", "pod", podName)

		// Pod-0 不允许删除
		if podName == fmt.Sprintf("%s-0", m.Spec.ClusterName) {
			log.Info("跳过主库 Pod-0，不可缩容")
			continue
		}

		pod := &corev1.Pod{}
		pod.SetName(podName)
		pod.SetNamespace(m.Namespace)
		if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
			log.Error(err, "缩容删除 Pod 失败", "pod", podName)
			continue
		}

		// 根据 Pod 名推 PVC 名: xxx-1 → data-xxx-1
		pvcName := "data-" + podName
		pvc := &corev1.PersistentVolumeClaim{}
		pvc.SetName("data-" + podName)
		pvc.SetNamespace(m.Namespace)
		if err := r.Delete(ctx, pvc); client.IgnoreNotFound(err) != nil {
			log.Error(err, "缩容删除 PVC 失败", "pvc", pvcName)
		}
	}
}
