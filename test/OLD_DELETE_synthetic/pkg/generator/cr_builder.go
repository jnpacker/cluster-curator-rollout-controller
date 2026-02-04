package generator

import (
	"fmt"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/models"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// CRBuilder generates Kubernetes Custom Resources for synthetic clusters
type CRBuilder struct {
	testID string // Unique test identifier
}

// NewCRBuilder creates a new CR builder
func NewCRBuilder(testID string) *CRBuilder {
	return &CRBuilder{
		testID: testID,
	}
}

// BuildNamespace creates a Namespace resource for a cluster
func (b *CRBuilder) BuildNamespace(cluster *models.SyntheticCluster) *corev1.Namespace {
	ns := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: cluster.Name,
			Labels: map[string]string{
				"synthetic-test-id": b.testID,
			},
		},
	}
	return ns
}

// BuildManagedCluster creates a ManagedCluster resource for a cluster
func (b *CRBuilder) BuildManagedCluster(cluster *models.SyntheticCluster) *unstructured.Unstructured {
	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1",
		Kind:    "ManagedCluster",
	})

	mc.SetName(cluster.Name)
	mc.SetNamespace(cluster.Name)

	// Add labels
	labels := make(map[string]string)
	for k, v := range cluster.Labels {
		labels[k] = v
	}
	// Add required labels for ManagedCluster
	labels["cloud"] = "auto-detect"
	labels["vendor"] = "auto-detect"
	// Add synthetic test ID label
	labels["synthetic-test-id"] = b.testID

	mc.SetLabels(labels)

	// Set spec
	spec := map[string]interface{}{
		"hubAcceptsClient": true,
	}
	mc.Object["spec"] = spec

	return mc
}

// BuildClusterCurator creates a ClusterCurator resource for a cluster
func (b *CRBuilder) BuildClusterCurator(cluster *models.SyntheticCluster) *unstructured.Unstructured {
	cc := &unstructured.Unstructured{}
	cc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1",
		Kind:    "ClusterCurator",
	})

	cc.SetName(fmt.Sprintf("curator-%s", cluster.Name))
	cc.SetNamespace(cluster.Name)

	// Add test ID label
	cc.SetLabels(map[string]string{
		"synthetic-test-id": b.testID,
	})

	// Add owner reference to ManagedCluster
	cc.SetOwnerReferences([]metav1.OwnerReference{
		{
			APIVersion: "cluster.open-cluster-management.io/v1",
			Kind:       "ManagedCluster",
			Name:       cluster.Name,
		},
	})

	// Set spec with basic configuration
	spec := map[string]interface{}{
		"desiredCuration": "install",
		"mode":           "auto",
	}
	cc.Object["spec"] = spec

	return cc
}

// CRSet represents all CRs for a single cluster
type CRSet struct {
	Namespace      *corev1.Namespace
	ManagedCluster *unstructured.Unstructured
	ClusterCurator *unstructured.Unstructured
}

// BuildCRSet creates all required CRs for a cluster
func (b *CRBuilder) BuildCRSet(cluster *models.SyntheticCluster) *CRSet {
	return &CRSet{
		Namespace:      b.BuildNamespace(cluster),
		ManagedCluster: b.BuildManagedCluster(cluster),
		ClusterCurator: b.BuildClusterCurator(cluster),
	}
}

