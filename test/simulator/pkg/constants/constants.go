package constants

import (
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Curation states for ClusterCurator spec.desiredCuration
// Valid values per CRD: "install", "scale", "upgrade", "destroy", "delete-cluster-namespace"
const (
	CurationInstall = "install"
	CurationUpgrade = "upgrade"
	// Note: There is no "done" state - to indicate idle/ready, desiredCuration should be removed/empty
)

// Simulator states for internal tracking
const (
	StateIdle      = "idle"
	StateUpgrading = "upgrading"
	StateCompleted = "completed"
	StateFailed    = "failed"
)

// Label keys
const (
	LabelInfraTestID = "infra-test-id"

	// ManagedCluster version labels (matching real OCM ManagedCluster)
	LabelOpenShiftVersion           = "openshiftVersion"
	LabelOpenShiftVersionMajor      = "openshiftVersion-major"
	LabelOpenShiftVersionMajorMinor = "openshiftVersion-major-minor"
)

// Condition types for ClusterCurator status (matching real OCM ClusterCurator)
const (
	ConditionCuratorJob     = "clustercurator-job"
	ConditionUpgradeCluster = "upgrade-cluster"
	ConditionMonitorUpgrade = "monitor-upgrade"
)

// Simulator annotations for tracking upgrade state
const (
	// AnnotationUpgradeCompleteAt stores the RFC3339 timestamp when the simulated upgrade should complete
	AnnotationUpgradeCompleteAt = "simulator.test/upgrade-complete-at"
	// AnnotationUpgradeWillFail stores whether this upgrade should fail ("true" or "false")
	AnnotationUpgradeWillFail = "simulator.test/upgrade-will-fail"
)

// GVK definitions for OCM resources
var (
	ManagedClusterGVK = schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1",
		Kind:    "ManagedCluster",
	}

	ClusterCuratorGVK = schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1beta1",
		Kind:    "ClusterCurator",
	}
)

// GVR definitions for OCM resources
var (
	ManagedClusterGVR = schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "managedclusters",
	}

	ClusterCuratorGVR = schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1beta1",
		Resource: "clustercurators",
	}
)

// ParseVersionLabels parses a version string (e.g., "4.20.0") and returns
// the labels map with openshiftVersion, openshiftVersion-major, openshiftVersion-major-minor
func ParseVersionLabels(version string) map[string]string {
	labels := map[string]string{
		LabelOpenShiftVersion: version,
	}

	parts := strings.Split(version, ".")
	if len(parts) >= 1 {
		labels[LabelOpenShiftVersionMajor] = parts[0]
	}
	if len(parts) >= 2 {
		labels[LabelOpenShiftVersionMajorMinor] = parts[0] + "." + parts[1]
	}

	return labels
}
