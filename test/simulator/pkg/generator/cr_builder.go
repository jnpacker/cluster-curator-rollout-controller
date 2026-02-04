package generator

import (
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/constants"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/models"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	// Start with all cluster labels
	labels := make(map[string]string)
	for k, v := range cluster.Labels {
		labels[k] = v
	}
	// Add infra-test-id label for tracking and cleanup
	labels[constants.LabelInfraTestID] = b.testID

	ns := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   cluster.Name,
			Labels: labels,
		},
	}
	return ns
}

// BuildManagedCluster creates a ManagedCluster resource for a cluster
func (b *CRBuilder) BuildManagedCluster(cluster *models.SyntheticCluster) *unstructured.Unstructured {
	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(constants.ManagedClusterGVK)

	mc.SetName(cluster.Name)
	// NOTE: ManagedCluster is cluster-scoped, do NOT set namespace

	// Add labels
	labels := make(map[string]string)
	for k, v := range cluster.Labels {
		labels[k] = v
	}
	// Add required labels for ManagedCluster
	labels["cloud"] = "auto-detect"
	labels["vendor"] = "auto-detect"
	// Add infra test ID label
	labels[constants.LabelInfraTestID] = b.testID

	// Add version labels (matching real ManagedCluster pattern)
	if cluster.BaseVersion != "" {
		for k, v := range constants.ParseVersionLabels(cluster.BaseVersion) {
			labels[k] = v
		}
	}

	mc.SetLabels(labels)

	// Set spec
	spec := map[string]interface{}{
		"hubAcceptsClient": true,
	}
	mc.Object["spec"] = spec

	return mc
}

// CRSet represents all CRs for a single cluster
// Note: ClusterCurators are NOT created by the simulator - they should be created
// by the ClusterCuratorRollout controller when an upgrade is initiated
type CRSet struct {
	Namespace      *corev1.Namespace
	ManagedCluster *unstructured.Unstructured
}

// BuildCRSet creates all required CRs for a cluster
func (b *CRBuilder) BuildCRSet(cluster *models.SyntheticCluster) *CRSet {
	return &CRSet{
		Namespace:      b.BuildNamespace(cluster),
		ManagedCluster: b.BuildManagedCluster(cluster),
	}
}

