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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SelectionType defines the type of cluster selection
// +kubebuilder:validation:Enum=group;filter;placement
type SelectionType string

const (
	// SelectionTypeGroup groups clusters by a label key, creating placements for each unique value
	SelectionTypeGroup SelectionType = "group"

	// SelectionTypeFilter filters clusters by a full label selector
	SelectionTypeFilter SelectionType = "filter"

	// SelectionTypePlacement uses an existing Placement resource
	SelectionTypePlacement SelectionType = "placement"
)

// ConcurrencyType defines how concurrency is measured
// +kubebuilder:validation:Enum=percent;count
type ConcurrencyType string

const (
	// ConcurrencyTypePercent specifies concurrency as a percentage
	ConcurrencyTypePercent ConcurrencyType = "percent"

	// ConcurrencyTypeCount specifies concurrency as an absolute count
	ConcurrencyTypeCount ConcurrencyType = "count"
)

// RolloutPhase represents the current phase of the rollout
type RolloutPhase string

const (
	// RolloutPhaseIdle indicates the controller is waiting for changes
	RolloutPhaseIdle RolloutPhase = "Idle"

	// RolloutPhaseProcessing indicates the controller is actively processing
	RolloutPhaseProcessing RolloutPhase = "Processing"

	// RolloutPhaseError indicates an error occurred during the last reconcile
	RolloutPhaseError RolloutPhase = "Error"
)

// ClusterUpgradeState represents the upgrade state of a cluster
type ClusterUpgradeState string

const (
	// ClusterUpgradeStatePending indicates the cluster is pending upgrade
	ClusterUpgradeStatePending ClusterUpgradeState = "Pending"

	// ClusterUpgradeStateInProgress indicates the cluster upgrade is in progress
	ClusterUpgradeStateInProgress ClusterUpgradeState = "InProgress"

	// ClusterUpgradeStateCompleted indicates the cluster upgrade completed successfully
	ClusterUpgradeStateCompleted ClusterUpgradeState = "Completed"

	// ClusterUpgradeStateFailed indicates the cluster upgrade failed
	ClusterUpgradeStateFailed ClusterUpgradeState = "Failed"

	// ClusterUpgradeStateSkipped indicates the cluster was skipped (e.g., wrong version)
	ClusterUpgradeStateSkipped ClusterUpgradeState = "Skipped"
)

// ConcurrencySpec defines the concurrency settings for rollouts
type ConcurrencySpec struct {
	// Type specifies how the concurrency value is interpreted
	// +kubebuilder:default=percent
	// +optional
	Type ConcurrencyType `json:"type,omitempty"`

	// Value is the concurrency value (percentage or absolute count)
	// A value of 0 means no concurrency (sequential)
	// +kubebuilder:default=0
	// +optional
	Value int `json:"value,omitempty"`
}

// RolloutSpec defines the rollout strategy
type RolloutSpec struct {
	// Concurrency defines the concurrency settings for the rollout
	// +optional
	Concurrency *ConcurrencySpec `json:"concurrency,omitempty"`
}

// PlacementReference is a reference to a Placement resource
type PlacementReference struct {
	// Name is the name of the Placement resource
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace is the namespace of the Placement resource
	// If not specified, defaults to the ClusterCuratorRollout's namespace
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// SelectionSpec defines how clusters are selected - this structure can be nested recursively
type SelectionSpec struct {
	// Type defines the selection type: group, filter, or placement
	// - group: Groups clusters by a label key, generating placements for each unique value
	// - filter: Filters clusters using a full label selector
	// - placement: Uses an existing Placement resource
	// +kubebuilder:validation:Required
	Type SelectionType `json:"type"`

	// LabelSelector is used differently based on the selection type:
	// - For type "group": Only the matchLabels keys are used to group clusters by those label keys.
	//   The values are ignored and clusters are grouped by unique values of those keys.
	// - For type "filter": The full label selector is used to filter clusters.
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`

	// PlacementRef references an existing Placement resource
	// Required when type is "placement"
	// +optional
	PlacementRef *PlacementReference `json:"placementRef,omitempty"`

	// Rollout defines the rollout strategy for this selection level
	// +optional
	Rollout *RolloutSpec `json:"rollout,omitempty"`

	// Children contains nested selection specs for hierarchical rollout strategies
	// Each child inherits the filtering from its parent
	// +optional
	Children []SelectionSpec `json:"children,omitempty"`
}

// ClusterCuratorRolloutSpec defines the desired state of ClusterCuratorRollout.
// The spec embeds SelectionSpec to allow recursive nesting of selection strategies.
type ClusterCuratorRolloutSpec struct {
	// ============= TOP-LEVEL ONLY FIELDS =============

	// OpenShiftVersion is the target OpenShift version to upgrade clusters to
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^\d+\.\d+(\.\d+)?$`
	OpenShiftVersion string `json:"openShiftVersion"`

	// OnlyUpgradeFromOpenShiftVersion restricts upgrades to only clusters
	// currently running this specific version
	// +optional
	OnlyUpgradeFromOpenShiftVersion string `json:"onlyUpgradeFromOpenShiftVersion,omitempty"`

	// OnFailure defines the behavior when a cluster upgrade fails
	// This field is reserved for future implementation
	// Suggested options:
	// - "continue": Continue with other clusters (default)
	// - "pause": Pause the rollout and wait for manual intervention
	// - "abort": Abort the entire rollout
	// - "retry": Retry the failed cluster upgrade
	// - "rollback": Attempt to rollback the failed cluster
	// +optional
	// +kubebuilder:validation:Enum=continue;pause;abort;retry;rollback
	// OnFailure string `json:"onFailure,omitempty"`

	// ============= RECURSIVE SELECTION FIELDS (inherited via embedding) =============
	SelectionSpec `json:",inline"`
}

// ============= CONDENSED STATUS TYPES (optimized for 4000+ clusters) =============

// PlacementStatus tracks the status of a generated or referenced Placement
// Size: ~200 bytes per placement (manageable even with 100s of placements)
type PlacementStatus struct {
	// Name is the name of the Placement
	Name string `json:"name"`

	// Namespace is the namespace of the Placement
	Namespace string `json:"namespace"`

	// GroupKey is the label key used to create this placement (for group type)
	// +optional
	GroupKey string `json:"groupKey,omitempty"`

	// GroupValue is the label value used to create this placement (for group type)
	// +optional
	GroupValue string `json:"groupValue,omitempty"`

	// Generated indicates whether this placement was generated by the controller
	Generated bool `json:"generated"`

	// Counts contains cluster counts by upgrade state for this placement
	Counts StateCounts `json:"counts"`

	// LastUpdated is the last time this placement status was updated
	// +optional
	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`
}

// StateCounts provides counts of clusters in each upgrade state
// This is a fixed-size structure (~50 bytes) regardless of cluster count
type StateCounts struct {
	// Total is the total number of clusters
	Total int `json:"total"`

	// Pending is the number of clusters pending upgrade
	Pending int `json:"pending"`

	// InProgress is the number of clusters currently upgrading
	InProgress int `json:"inProgress"`

	// Completed is the number of clusters that completed upgrade
	Completed int `json:"completed"`

	// Failed is the number of clusters that failed upgrade
	Failed int `json:"failed"`

	// Skipped is the number of clusters that were skipped
	Skipped int `json:"skipped"`
}

// FailedClusterInfo contains minimal info about a failed cluster for troubleshooting
// Only failed clusters are stored individually (typically a small subset)
// Size: ~150 bytes per failed cluster
type FailedClusterInfo struct {
	// Name is the name of the cluster
	Name string `json:"name"`

	// PlacementName is the placement that selected this cluster
	PlacementName string `json:"placementName"`

	// CurrentVersion is the cluster's current OpenShift version
	CurrentVersion string `json:"currentVersion"`

	// FailureReason provides a brief description of why the upgrade failed
	FailureReason string `json:"failureReason"`

	// FailedAt is when the failure was detected
	FailedAt *metav1.Time `json:"failedAt,omitempty"`
}

// InProgressClusterInfo contains minimal info about a cluster being upgraded
// Size: ~100 bytes per in-progress cluster
type InProgressClusterInfo struct {
	// Name is the name of the cluster
	Name string `json:"name"`

	// PlacementName is the placement that selected this cluster
	PlacementName string `json:"placementName"`

	// StartedAt is when the upgrade started
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
}

// ClusterCuratorRolloutStatus defines the observed state of ClusterCuratorRollout.
//
// Design for 4000+ clusters:
// - Only store aggregate counts (fixed size regardless of cluster count)
// - Only store details for failed clusters (typically small subset)
// - Only store details for in-progress clusters (bounded by concurrency settings)
// - Reference PlacementDecisions for full cluster lists
//
// Estimated max size: ~50KB for 100 placements + 100 failed + 50 in-progress
type ClusterCuratorRolloutStatus struct {
	// Phase indicates the current phase of the rollout
	// +optional
	Phase RolloutPhase `json:"phase,omitempty"`

	// LastReconcileTime is the timestamp of the last reconciliation
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// LastReconcileError contains the error from the last reconciliation if any
	// +optional
	LastReconcileError string `json:"lastReconcileError,omitempty"`

	// LastReconcileDuration is how long the last reconciliation took
	// Useful for monitoring performance with large cluster counts
	// +optional
	LastReconcileDuration *metav1.Duration `json:"lastReconcileDuration,omitempty"`

	// Placements contains the status of all placements (generated or referenced)
	// Each placement includes aggregate counts, not individual cluster details
	// +optional
	Placements []PlacementStatus `json:"placements,omitempty"`

	// TotalCounts provides aggregate counts across all placements
	// +optional
	TotalCounts *StateCounts `json:"totalCounts,omitempty"`

	// FailedClusters contains details only for clusters that failed upgrade
	// This list is bounded - in a healthy rollout, it should be empty or small
	// If there are more than MaxFailedClusterDetails failures, only the most recent are shown
	// +optional
	FailedClusters []FailedClusterInfo `json:"failedClusters,omitempty"`

	// InProgressClusters contains details for clusters currently being upgraded
	// This list is bounded by the concurrency settings
	// +optional
	InProgressClusters []InProgressClusterInfo `json:"inProgressClusters,omitempty"`

	// Conditions represent the latest available observations of the rollout's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the generation of the spec that was last processed
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Message provides a human-readable summary of the current state
	// +optional
	Message string `json:"message,omitempty"`
}

const (
	// MaxFailedClusterDetails is the maximum number of failed clusters stored in status
	// Additional failures are counted but not stored individually
	MaxFailedClusterDetails = 100

	// MaxInProgressClusterDetails is the maximum number of in-progress clusters stored
	MaxInProgressClusterDetails = 50
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ccr;ccrollout
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Target Version",type=string,JSONPath=`.spec.openShiftVersion`
// +kubebuilder:printcolumn:name="Total",type=integer,JSONPath=`.status.totalCounts.total`
// +kubebuilder:printcolumn:name="Pending",type=integer,JSONPath=`.status.totalCounts.pending`
// +kubebuilder:printcolumn:name="InProgress",type=integer,JSONPath=`.status.totalCounts.inProgress`
// +kubebuilder:printcolumn:name="Completed",type=integer,JSONPath=`.status.totalCounts.completed`
// +kubebuilder:printcolumn:name="Failed",type=integer,JSONPath=`.status.totalCounts.failed`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClusterCuratorRollout is the Schema for the clustercuratorrollouts API.
// It orchestrates the upgrade of OpenShift clusters using ClusterCurator CRDs
// in a hierarchical, controlled manner supporting up to 4000+ clusters.
type ClusterCuratorRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterCuratorRolloutSpec   `json:"spec,omitempty"`
	Status ClusterCuratorRolloutStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterCuratorRolloutList contains a list of ClusterCuratorRollout.
type ClusterCuratorRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterCuratorRollout `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterCuratorRollout{}, &ClusterCuratorRolloutList{})
}
