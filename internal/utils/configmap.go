package utils

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// defaultMysqlConfig 默认 my.cnf 模板
const defaultMysqlConfig = `[mysqld]
server-id={server-id}
log-bin=mysql-bin
binlog-format=row
gtid-mode=ON
enforce-gtid-consistency=ON
log-slave-updates=ON
read-only=OFF
skip-name-resolve=ON
character-set-server=utf8mb4
collation-server=utf8mb4_unicode_ci

[client]
default-character-set=utf8mb4
`

// GetOrCreateConfigMap 构造并创建 my.cnf 的 ConfigMap，已存在则直接返回。
func GetOrCreateConfigMap(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, namespace, clusterName, name, mysqlConfig string) (*corev1.ConfigMap, error) {
	config := mysqlConfig
	if config == "" {
		config = defaultMysqlConfig
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": clusterName},
		},
		Data: map[string]string{
			"my.cnf": config,
		},
	}

	existing := &corev1.ConfigMap{}
	err := c.Get(ctx, client.ObjectKeyFromObject(cm), existing)
	if err == nil {
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Error(err, "获取 ConfigMap 失败", "name", name)
		return nil, err
	}
	if err := controllerutil.SetControllerReference(owner, cm, scheme); err != nil {
		logf.FromContext(ctx).Error(err, "设置 ConfigMap OwnerReference 失败", "name", name)
		return nil, err
	}
	if err := c.Create(ctx, cm); err != nil {
		logf.FromContext(ctx).Error(err, "创建 ConfigMap 失败", "name", name)
		return nil, err
	}
	return cm, nil
}
