package controller

import (
	"context"
	"fmt"
	"strings"

	mysqlv1 "mysql-cluster-operator/api/v1"
	"mysql-cluster-operator/internal/utils"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// initialize 初始化 MySQL 集群：创建 StatefulSet、两个 Service，执行 MySQL 主从初始化
func (r *MysqlClusterReconciler) initialize(ctx context.Context, m *mysqlv1.MysqlCluster) error {
	log := log.FromContext(ctx)
	log.Info("开始初始化 MySQL 集群", "cluster", m.Spec.ClusterName)

	// 1. 创建两个 Service（mysql-primary、mysql-replica）
	if _, err := utils.GetOrCreateService(ctx, r.Client, r.Scheme, m, m.Namespace, m.Spec.ClusterName, m.Spec.PrimaryServiceName, "primary"); err != nil {
		return err
	}
	if _, err := utils.GetOrCreateService(ctx, r.Client, r.Scheme, m, m.Namespace, m.Spec.ClusterName, m.Spec.ReplicaServiceName, "replica"); err != nil {
		return err
	}

	// 2.读取副本数
	replicas := m.Spec.Replicas
	if replicas < 1 {
		return fmt.Errorf("副本数不能小于 1，当前副本数：%d", replicas)
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

	// 4. 根据副本数，创建 StatefulSet（含 PVC）

	// 5. 建立主从关系

	return nil
}
