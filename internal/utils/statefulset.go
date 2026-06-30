package utils

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "k8s.io/api/apps/v1"
)

const (
	mysqlContainerPort = 3306
	configMountPath    = "/etc/mysql/conf.d"
)

// StatefulSetConfig StatefulSet 构造参数
type StatefulSetConfig struct {
	Name               string
	Namespace          string
	ClusterName        string
	Replicas           int32
	Image              string
	StorageSize        string
	StorageClass       string
	ServiceName        string
	RootPasswordSecret *corev1.LocalObjectReference
	Resources          corev1.ResourceRequirements
}

// GetOrCreateStatefulSet 构造并创建 StatefulSet（含 PVC 模板），已存在则直接返回。
func GetOrCreateStatefulSet(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, cfg StatefulSetConfig) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.Name,
			Namespace: cfg.Namespace,
			Labels:    map[string]string{"app": cfg.ClusterName},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: cfg.ServiceName,
			Replicas:    &cfg.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": cfg.ClusterName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": cfg.ClusterName},
				},
				Spec: corev1.PodSpec{
					InitContainers: nil, // 后续主从初始化时用到
					Containers: []corev1.Container{
						buildMySQLContainer(cfg),
					},
					Volumes: nil, // ConfigMap 通过 StatefulSet volumeClaimTemplate 以外的 volume 挂载？不，每个 pod 有自己的 ConfigMap，应该用 volume 的方式挂载。
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				buildVolumeClaimTemplate(cfg),
			},
		},
	}

	// 每个 Pod 挂载自己的 ConfigMap
	// 注意：ConfigMap 名称为 {clusterName}-{i}-config，需用 subPath 或在 init container 中处理
	// 此处先预留，后续完善

	existing := &appsv1.StatefulSet{}
	err := c.Get(ctx, client.ObjectKeyFromObject(sts), existing)
	if err == nil {
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Error(err, "获取 StatefulSet 失败", "name", cfg.Name)
		return nil, err
	}
	if err := controllerutil.SetControllerReference(owner, sts, scheme); err != nil {
		logf.FromContext(ctx).Error(err, "设置 StatefulSet OwnerReference 失败", "name", cfg.Name)
		return nil, err
	}
	if err := c.Create(ctx, sts); err != nil {
		logf.FromContext(ctx).Error(err, "创建 StatefulSet 失败", "name", cfg.Name)
		return nil, err
	}
	return sts, nil
}

// buildMySQLContainer 构造 MySQL 容器定义
func buildMySQLContainer(cfg StatefulSetConfig) corev1.Container {
	c := corev1.Container{
		Name:  "mysql",
		Image: cfg.Image,
		Ports: []corev1.ContainerPort{
			{ContainerPort: mysqlContainerPort, Name: "mysql"},
		},
		Env:          nil,
		VolumeMounts: nil,
		Resources:    cfg.Resources,
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt32(mysqlContainerPort),
				},
			},
			InitialDelaySeconds: 30,
			PeriodSeconds:       10,
		},
	}

	// root 密码
	if cfg.RootPasswordSecret != nil {
		c.Env = append(c.Env, corev1.EnvVar{
			Name: "MYSQL_ROOT_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: *cfg.RootPasswordSecret,
					Key:                  "password",
				},
			},
		})
	}

	// 数据目录挂载
	c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
		Name:      "data",
		MountPath: "/var/lib/mysql",
	})

	return c
}

// buildVolumeClaimTemplate 构造 PVC 模板
func buildVolumeClaimTemplate(cfg StatefulSetConfig) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(cfg.StorageSize),
				},
			},
		},
	}
}

// buildInitializerContainer 构造初始化容器（用于主从初始化）
func buildInitializerContainer(cfg StatefulSetConfig) corev1.Container {
	return corev1.Container{
		Name:  "init-mysql",
		Image: cfg.Image,
		Command: []string{
			"bash", "-c",
			fmt.Sprintf(`
if [ ! -f /var/lib/mysql/initialized ]; then
  # 主从初始化逻辑在此执行
  touch /var/lib/mysql/initialized
fi
`),
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "data",
				MountPath: "/var/lib/mysql",
			},
		},
	}
}
