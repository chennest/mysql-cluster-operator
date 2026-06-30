/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1 "mysql-cluster-operator/api/v1"
)

// MysqlClusterReconciler reconciles a MysqlCluster object
type MysqlClusterReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	RestConfig *rest.Config
}

// +kubebuilder:rbac:groups=mysql.chennest.io,resources=mysqlclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mysql.chennest.io,resources=mysqlclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.chennest.io,resources=mysqlclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MysqlCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *MysqlClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster mysqlv1.MysqlCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. 判断是新集群还是已有集群
	if _, ok := cluster.Annotations["initialized"]; !ok {
		if err := r.initialize(ctx, &cluster); err != nil {
			log.Error(err, "初始化集群失败")
			return ctrl.Result{}, err
		} else {
			log.Info("初始化集群成功")
		}

		// 设置annotations 标识 已成功
		if cluster.Annotations == nil {
			cluster.Annotations = make(map[string]string)
		}
		cluster.Annotations["initialized"] = "true"
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// 已经初始化完成，进入检测逻辑
		// 1.副本调谐
		result, err := r.reconcileReplicas(ctx, &cluster)
		if err != nil {
			return result, err
		}
		// 2.主从检测逻辑和调谐
		result, err = r.reconcileMasterSlave(ctx, &cluster)
		if err != nil {
			return result, err
		}
	}

	// 3.定期记录 GTID 快照，用于选举
	if err := r.syncGTID(ctx, &cluster); err != nil {
		log.Error(err, "GTID 采集失败")
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MysqlClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1.MysqlCluster{}).
		Owns(&corev1.Pod{}).
		Named("mysqlcluster").
		Complete(r)
}

func (r *MysqlClusterReconciler) reconcileReplicas(ctx context.Context, m *mysqlv1.MysqlCluster) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

func (r *MysqlClusterReconciler) reconcileMasterSlave(ctx context.Context, m *mysqlv1.MysqlCluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 等待所有 Pod Ready
	replicas := m.Spec.Replicas + 1
	readyCount := 0
	for i := int32(0); i < replicas; i++ {
		podName := fmt.Sprintf("%s-%d", m.Spec.ClusterName, i)
		var pod corev1.Pod
		if err := r.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: podName}, &pod); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				readyCount++
				break
			}
		}
	}
	if readyCount < int(replicas) {
		log.Info("等待所有 Pod 就绪", "ready", readyCount, "expected", replicas)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 检查从库复制状态，未配置则建主从
	if m.Status.Phase != "Running" {
		if err := r.setupReplication(ctx, m); err != nil {
			log.Error(err, "建立主从失败")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		m.Status.Phase = "Running"
		m.Status.Ready = true
		if err := r.Status().Update(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 主从已建立，检查健康状态
	if err := r.checkReplication(ctx, m); err != nil {
		log.Error(err, "检查复制状态失败")
	}
	return ctrl.Result{}, nil
}
