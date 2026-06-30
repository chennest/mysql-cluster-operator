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

// GetOrCreateService 构造并创建 Service，已存在则直接返回。headless 为 true 时创建无头 Service。
func GetOrCreateService(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, namespace, clusterName, name, role string, headless bool) (*corev1.Service, error) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": clusterName},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": clusterName},
			Ports:    []corev1.ServicePort{{Port: 3306}},
		},
	}
	if !headless {
		svc.Spec.Selector["role"] = role
	}
	if headless {
		svc.Spec.ClusterIP = "None"
	}

	existing := &corev1.Service{}
	err := c.Get(ctx, client.ObjectKeyFromObject(svc), existing)
	if err == nil {
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Error(err, "获取 Service 失败", "name", name)
		return nil, err
	}
	if err := controllerutil.SetControllerReference(owner, svc, scheme); err != nil {
		logf.FromContext(ctx).Error(err, "设置 Service OwnerReference 失败", "name", name)
		return nil, err
	}
	if err := c.Create(ctx, svc); err != nil {
		logf.FromContext(ctx).Error(err, "创建 Service 失败", "name", name)
		return nil, err
	}
	return svc, nil
}
