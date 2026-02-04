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

	// RolloutPhaseSoaking indicates the controller is waiting for a soak period to complete
	RolloutPhaseSoaking RolloutPhase = "Soaking"

	// RolloutPhasePaused indicates the rollout is paused due to failure (onFailure: pause)
	RolloutPhasePaused RolloutPhase = "Paused"

	// RolloutPhaseError indicates an error occurred during the last reconcile
	RolloutPhaseError RolloutPhase = "Error"

	// RolloutPhaseCompletedWithFailures indicates the rollout finished but some clusters failed
	RolloutPhaseCompletedWithFailures RolloutPhase = "CompletedWithFailures"
)

// OnFailure behavior constants
const (
	OnFailureContinue = "continue"
	OnFailurePause    = "pause"
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

// RolloutStepState represents the state of a rollout step
type RolloutStepState string

const (
	// RolloutStepStatePending indicates the step is waiting to be processed
	RolloutStepStatePending RolloutStepState = "Pending"

	// RolloutStepStateActive indicates the step is currently being processed
	RolloutStepStateActive RolloutStepState = "Active"

	// RolloutStepStateBatchSoaking indicates a batch within the step completed and is in soak period
	// After the batch soak ends, the next batch will be processed
	RolloutStepStateBatchSoaking RolloutStepState = "BatchSoaking"

	// RolloutStepStateSoaking indicates the step completed and is in soak period
	// This is the step-level soak after ALL batches complete
	RolloutStepStateSoaking RolloutStepState = "Soaking"

	// RolloutStepStateCompleted indicates the step finished successfully
	RolloutStepStateCompleted RolloutStepState = "Completed"

	// RolloutStepStateFailed indicates the step encountered a failure
	RolloutStepStateFailed RolloutStepState = "Failed"
)

// RolloutStep represents one step in the ordered rollout plan
// Each step corresponds to a placement and tracks its progress through the rollout
type RolloutStep struct {
	// Index is the order index (0-based) defining rollout priority
	Index int `json:"index"`

	// PlacementName is the name of the placement for this step
	PlacementName string `json:"placementName"`

	// Path is the hierarchical path derived from filter labels (e.g., "environment-canary")
	Path string `json:"path"`

	// State is the current state of this step
	State RolloutStepState `json:"state"`

	// Counts has the cluster counts for this step
	Counts StateCounts `json:"counts"`

	// SoakDuration is the configured soak duration for this step (if any)
	// This is the step-level soak that applies after ALL clusters in the step complete.
	// +optional
	SoakDuration *metav1.Duration `json:"soakDuration,omitempty"`

	// Concurrency defines how many clusters to process concurrently within this step
	// +optional
	Concurrency *ConcurrencySpec `json:"concurrency,omitempty"`

	// CurrentBatch is the 1-based index of the current batch being processed
	// A batch is a group of clusters processed together based on concurrency settings
	// Value starts at 1 for the first batch and equals TotalBatches for the final batch
	// +optional
	CurrentBatch int `json:"currentBatch,omitempty"`

	// TotalBatches is the total number of batches for this step
	// Calculated based on total clusters and concurrency settings
	// +optional
	TotalBatches int `json:"totalBatches,omitempty"`

	// BatchClusters contains the list of cluster names that have been selected for the current batch
	// This is used to track which clusters should have ClusterCurators created
	// +optional
	BatchClusters []string `json:"batchClusters,omitempty"`

	// BatchSoakEndsAt is when the current batch's soak period ends (if currently in batch soak)
	// This is separate from step-level SoakEndsAt which applies after all batches complete
	// +optional
	BatchSoakEndsAt *metav1.Time `json:"batchSoakEndsAt,omitempty"`

	// StartedAt is when this step became Active
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when this step completed (before soak)
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// SoakEndsAt is when the soak period ends (if currently soaking)
	// +optional
	SoakEndsAt *metav1.Time `json:"soakEndsAt,omitempty"`

	// FailedClusters contains the names of clusters that failed upgrade in this step
	// Only populated when state is Failed
	// +optional
	FailedClusters []string `json:"failedClusters,omitempty"`
}

// ConcurrencySpec defines the concurrency settings for rollouts
type ConcurrencySpec struct {
	// Type specifies how the concurrency value is interpreted
	// - "count": Value is an absolute number of clusters to process concurrently
	// - "percent": Value is a percentage of clusters to process concurrently
	// +kubebuilder:default=count
	// +optional
	Type ConcurrencyType `json:"type,omitempty"`

	// Value is the concurrency value (percentage or absolute count depending on Type)
	// For type "count": number of clusters to upgrade concurrently (1 = sequential, one at a time)
	// For type "percent": percentage of clusters to upgrade concurrently (0 = sequential, 100 = all at once)
	// +kubebuilder:default=1
	// +optional
	Value int `json:"value,omitempty"`

	// SoakDuration is the time to wait between each concurrent group completing an upgrade.
	// If not specified, the controller maintains the concurrency level continuously (rolling upgrades).
	// When specified, the controller waits for the entire concurrent batch to complete,
	// then waits for this duration before starting the next batch.
	// Format: duration string (e.g., "2h", "30m", "4h")
	// +optional
	SoakDuration *metav1.Duration `json:"soakDuration,omitempty"`
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
// Note: CEL validation is applied at the ClusterCuratorRolloutSpec level for the top-level selection.
// Nested children selections are validated by the controller at runtime.
//
// Structural rules:
// - "group" type must have "filter" type children (defines the branches and their order)
// - "filter" type can have either another "filter" or a "group" as children
// - "filter" without children creates a Placement (leaf node)
// - Order of filter children defines the rollout priority order
type SelectionSpec struct {
	// Type defines the selection type: group, filter, or placement
	// - group: A branching point that must have filter children. The groupByLabelKey
	//   should match the label key used in the children's labelSelectors.
	// - filter: Adds label predicates to the placement. Can contain nested groups or filters.
	//   When a filter has no children, it creates a Placement (leaf node).
	// - placement: Uses an existing Placement resource directly.
	// +kubebuilder:validation:Required
	Type SelectionType `json:"type"`

	// GroupByLabelKey specifies the label key that this group branches on.
	// The children (filters) should have labelSelectors that match values of this key.
	// Required when type is "group".
	// +optional
	GroupByLabelKey string `json:"groupByLabelKey,omitempty"`

	// LabelSelector filters clusters using a standard Kubernetes label selector.
	// This predicate is added to any Placement created at or below this level.
	// Required when type is "filter".
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`

	// PlacementRef references an existing Placement resource
	// Required when type is "placement"
	// +optional
	PlacementRef *PlacementReference `json:"placementRef,omitempty"`

	// Concurrency defines how many clusters to upgrade in parallel within this filter's placement.
	// Only applicable when type is "filter".
	// +optional
	Concurrency *ConcurrencySpec `json:"concurrency,omitempty"`

	// SoakDuration is the time to wait after all filtered clusters complete
	// before proceeding to the next sibling filter.
	// Only applicable when type is "filter".
	// The last filter in a group does not need a soak duration (nothing follows it).
	// Format: duration string (e.g., "24h", "48h", "12h")
	// +optional
	SoakDuration *metav1.Duration `json:"soakDuration,omitempty"`

	// Children contains nested selection specs for hierarchical rollout strategies.
	// - For "group" type: Must contain "filter" type children. Order defines priority.
	// - For "filter" type: Can contain "group" or "filter" children, or be empty (leaf).
	// Each child inherits the label predicates from its ancestors.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Children []SelectionSpec `json:"children,omitempty"`
}

// ClusterCuratorRolloutSpec defines the desired state of ClusterCuratorRollout.
// The spec embeds SelectionSpec to allow recursive nesting of selection strategies.
// +kubebuilder:validation:XValidation:rule="self.type != 'placement' || has(self.placementRef)",message="placementRef is required when type is 'placement'"
// +kubebuilder:validation:XValidation:rule="self.type != 'group' || has(self.groupByLabelKey)",message="groupByLabelKey is required when type is 'group'"
// +kubebuilder:validation:XValidation:rule="self.type != 'filter' || has(self.labelSelector)",message="labelSelector is required when type is 'filter'"
// +kubebuilder:validation:XValidation:rule="self.type == 'placement' || !has(self.placementRef)",message="placementRef should only be set when type is 'placement'"
// +kubebuilder:validation:XValidation:rule="self.type == 'group' || !has(self.groupByLabelKey)",message="groupByLabelKey should only be set when type is 'group'"
// +kubebuilder:validation:XValidation:rule="self.type == 'filter' || !has(self.labelSelector)",message="labelSelector should only be set when type is 'filter'"
type ClusterCuratorRolloutSpec struct {
	// ============= TOP-LEVEL ONLY FIELDS =============

	// OpenShiftVersion is the target OpenShift version to upgrade clusters to.
	// Must be a full version string including patch version (e.g., "4.15.3").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^\d+\.\d+\.\d+$`
	OpenShiftVersion string `json:"openShiftVersion"`

	// OnlyUpgradeFromOpenShiftVersions restricts upgrades to only clusters
	// currently running one of these versions. Each entry can be:
	// - major.minor (e.g., "4.14") - matches all 4.14.x clusters
	// - major.minor.patch (e.g., "4.14.5") - matches only that exact version
	// If a minor version (4.14) and a patch version (4.14.5) are both specified,
	// the minor version already covers the patch version.
	// +optional
	// +kubebuilder:validation:items:Pattern=`^\d+\.\d+(\.\d+)?$`
	OnlyUpgradeFromOpenShiftVersions []string `json:"onlyUpgradeFromOpenShiftVersions,omitempty"`

	// OnFailure defines the behavior when a cluster upgrade fails.
	// - "continue": Continue with other clusters (default behavior)
	// - "pause": Pause the rollout and wait for manual intervention
	// +optional
	// +kubebuilder:default=continue
	// +kubebuilder:validation:Enum=continue;pause
	OnFailure string `json:"onFailure,omitempty"`

	// ============= RECURSIVE SELECTION FIELDS (inherited via embedding) =============
	SelectionSpec `json:",inline"`
}

// ============= CONDENSED STATUS TYPES (optimized for 4000+ clusters) =============

// PlacementStatus tracks the status of a generated or referenced Placement
// Size: ~150 bytes per placement (manageable even with 100s of placements)
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

	// Total is the total number of clusters selected by this placement
	// Detailed state counts are tracked in the RolloutPlan steps
	Total int `json:"total"`

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

	// ObservedOpenShiftVersion is the target OpenShift version that was last processed.
	// When spec.openShiftVersion changes to a different value, the controller will
	// reset all status fields and start the rollout from scratch.
	// +optional
	ObservedOpenShiftVersion string `json:"observedOpenShiftVersion,omitempty"`

	// Message provides a human-readable summary of the current state
	// +optional
	Message string `json:"message,omitempty"`

	// SoakStatus contains information about the current soak period, if any
	// +optional
	SoakStatus *SoakStatus `json:"soakStatus,omitempty"`

	// RolloutPlan is the ordered list of rollout steps derived from the spec
	// Each step represents a placement and its progress through the rollout
	// The order is determined by the filter hierarchy in the spec
	// +optional
	RolloutPlan []RolloutStep `json:"rolloutPlan,omitempty"`

	// CurrentStepIndex is the index of the currently active step in the rollout plan
	// A nil value indicates the rollout has not started or is complete
	// +optional
	CurrentStepIndex *int `json:"currentStepIndex,omitempty"`
}

// SoakStatus tracks the current soak period
type SoakStatus struct {
	// GroupPath identifies the group that completed and is now soaking
	// Format: "key1=value1/key2=value2" (e.g., "environment=production/region=us-east")
	GroupPath string `json:"groupPath"`

	// StartedAt is when the soak period started (when the group completed)
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// EndsAt is when the soak period will end
	EndsAt *metav1.Time `json:"endsAt,omitempty"`

	// Duration is the configured soak duration
	Duration *metav1.Duration `json:"duration,omitempty"`
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
// +kubebuilder:printcolumn:name="Skipped",type=integer,JSONPath=`.status.totalCounts.skipped`
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
