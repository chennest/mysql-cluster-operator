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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// MysqlClusterSpec 定义用户期望的集群状态
type MysqlClusterSpec struct {
	// 集群名称，用于生成 StatefulSet/Service 名称
	// +kubebuilder:validation:Required
	ClusterName string `json:"clusterName"`

	// 从库副本数（不包含主库）
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	Replicas int32 `json:"replicas,omitempty"`

	// MySQL 镜像版本
	// +kubebuilder:default="mysql:8.0"
	Image string `json:"image,omitempty"`

	// 资源限制
	// +kubebuilder:validation:Optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// 存储配置（主从共用）
	// +kubebuilder:validation:Optional
	Storage StorageConfig `json:"storage,omitempty"`

	// primary Service 名称
	// +kubebuilder:default="mysql-primary"
	PrimaryServiceName string `json:"primaryServiceName,omitempty"`

	// replica Service 名称
	// +kubebuilder:default="mysql-replica"
	ReplicaServiceName string `json:"replicaServiceName,omitempty"`

	// root 密码 Secret 引用
	// +kubebuilder:validation:Optional
	RootPasswordSecretRef *corev1.LocalObjectReference `json:"rootPasswordSecretRef,omitempty"`

	// 自定义 my.cnf（可选）
	// +kubebuilder:validation:Optional
	MysqlConfig string `json:"mysqlConfig,omitempty"`
}

// StorageConfig 存储配置
type StorageConfig struct {
	// 存储容量
	// +kubebuilder:default="10Gi"
	Size string `json:"size,omitempty"`

	// 存储类名称
	// +kubebuilder:default="local-path"
	StorageClassName string `json:"storageClassName,omitempty"`
}

// MysqlClusterStatus 定义集群当前状态
type MysqlClusterStatus struct {
	// 当前主库 Pod 名称
	PrimaryPod string `json:"primaryPod,omitempty"`

	// 各节点状态（含主库和从库）
	Nodes []NodeStatus `json:"nodes,omitempty"`

	// 集群就绪状态
	Ready bool `json:"ready"`

	// 当前集群阶段（Init/Running/Failover/Scaling）
	Phase string `json:"phase,omitempty"`

	// 最近一次 GTID 采集时间
	LastGTIDSync metav1.Time `json:"lastGTIDSync,omitempty"`
}

// NodeStatus 单个节点的运行状态
type NodeStatus struct {
	// Pod 名称
	Name string `json:"name"`

	// 节点角色（primary / replica）
	Role string `json:"role,omitempty"`

	// 当前 GTID（SELECT @@global.gtid_executed）
	GTID string `json:"gtid,omitempty"`

	// 节点条件（Master/Replicating/Lagged/ReadOnly）
	Conditions []NodeCondition `json:"conditions,omitempty"`
}

// NodeCondition 节点健康条件
type NodeCondition struct {
	// 条件类型：Master / Replicating / Lagged / ReadOnly
	Type NodeConditionType `json:"type"`

	// True / False / Unknown
	Status corev1.ConditionStatus `json:"status"`

	// 最近一次状态变更时间
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
}

// NodeConditionType 条件类型
type NodeConditionType string

const (
	NodeConditionMaster      NodeConditionType = "Master"
	NodeConditionReplicating NodeConditionType = "Replicating"
	NodeConditionLagged      NodeConditionType = "Lagged"
	NodeConditionReadOnly    NodeConditionType = "ReadOnly"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Primary",type="string",JSONPath=".status.primaryPod"
// +kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="Ready",type="boolean",JSONPath=".status.ready"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type MysqlCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of MysqlCluster
	// +required
	Spec MysqlClusterSpec `json:"spec"`

	// status defines the observed state of MysqlCluster
	// +optional
	Status MysqlClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// MysqlClusterList contains a list of MysqlCluster
type MysqlClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []MysqlCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &MysqlCluster{}, &MysqlClusterList{})
		return nil
	})
}
