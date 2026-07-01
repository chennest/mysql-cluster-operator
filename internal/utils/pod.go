package utils

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// PodConfig Pod 构造参数
type PodConfig struct {
	Name               string
	Namespace          string
	ClusterName        string
	Index              int32
	Role               string
	Image              string
	PVCName            string
	ConfigMapName      string
	RootPasswordSecret *corev1.LocalObjectReference
	Resources          corev1.ResourceRequirements
	ReadinessProbe     *corev1.Probe
}

// GetOrCreatePod 构造并创建 MySQL Pod，已存在则直接返回
func GetOrCreatePod(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, cfg PodConfig) (*corev1.Pod, error) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.Name,
			Namespace: cfg.Namespace,
			Labels: map[string]string{
				"app":   cfg.ClusterName,
				"index": fmt.Sprintf("%d", cfg.Index),
				"role":  cfg.Role,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "mysql",
					Image: cfg.Image,
					Ports: []corev1.ContainerPort{
						{ContainerPort: 3306, Name: "mysql"},
					},
					Env:       buildPodEnv(cfg),
					Resources: cfg.Resources,
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "data",
							MountPath: "/var/lib/mysql",
						},
						{
							Name:      "config",
							MountPath: "/etc/mysql/conf.d/my.cnf",
							SubPath:   "my.cnf",
						},
					},
					ReadinessProbe: readinessProbeOrDefault(cfg.ReadinessProbe),
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: cfg.PVCName,
						},
					},
				},
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: cfg.ConfigMapName,
							},
						},
					},
				},
			},
		},
	}

	existing := &corev1.Pod{}
	err := c.Get(ctx, client.ObjectKeyFromObject(pod), existing)
	if err == nil {
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Error(err, "获取 Pod 失败", "name", cfg.Name)
		return nil, err
	}
	if err := controllerutil.SetControllerReference(owner, pod, scheme); err != nil {
		logf.FromContext(ctx).Error(err, "设置 Pod OwnerReference 失败", "name", cfg.Name)
		return nil, err
	}
	if err := c.Create(ctx, pod); err != nil {
		logf.FromContext(ctx).Error(err, "创建 Pod 失败", "name", cfg.Name)
		return nil, err
	}
	return pod, nil
}

// buildPodEnv 构造 Pod 环境变量
func buildPodEnv(cfg PodConfig) []corev1.EnvVar {
	var envs []corev1.EnvVar
	if cfg.RootPasswordSecret != nil {
		envs = append(envs, corev1.EnvVar{
			Name: "MYSQL_ROOT_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: *cfg.RootPasswordSecret,
					Key:                  "password",
				},
			},
		})
	}
	return envs
}

// readinessProbeOrDefault 返回用户自定义探针，为空时使用默认 TCP 3306 探针
func readinessProbeOrDefault(p *corev1.Probe) *corev1.Probe {
	if p != nil {
		return p
	}
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt32(3306),
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       10,
	}
}
