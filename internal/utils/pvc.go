package utils

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// GetOrCreatePVC 构造并创建 PVC，已存在则直接返回
func GetOrCreatePVC(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, namespace, clusterName, name, storageSize, storageClass string) (*corev1.PersistentVolumeClaim, error) {
	sc := storageClass
	if sc == "" {
		sc = "local-path"
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": clusterName},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(storageSize),
				},
			},
		},
	}

	existing := &corev1.PersistentVolumeClaim{}
	err := c.Get(ctx, client.ObjectKeyFromObject(pvc), existing)
	if err == nil {
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Error(err, "获取 PVC 失败", "name", name)
		return nil, err
	}
	if err := controllerutil.SetControllerReference(owner, pvc, scheme); err != nil {
		logf.FromContext(ctx).Error(err, "设置 PVC OwnerReference 失败", "name", name)
		return nil, err
	}
	if err := c.Create(ctx, pvc); err != nil {
		logf.FromContext(ctx).Error(err, "创建 PVC 失败", "name", name)
		return nil, err
	}
	return pvc, nil
}

// BuildPVCNames 生成所有 PVC 名称，返回 [pvc-0, pvc-1, ...]
func BuildPVCNames(clusterName string, replicas int32) []string {
	names := make([]string, replicas+1)
	for i := int32(0); i < replicas+1; i++ {
		names[i] = fmt.Sprintf("data-%s-%d", clusterName, i)
	}
	return names
}
