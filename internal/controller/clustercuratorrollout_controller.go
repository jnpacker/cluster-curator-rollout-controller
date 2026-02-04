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

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1 "open-cluster-management.io/api/cluster/v1"
	clusterv1beta1 "open-cluster-management.io/api/cluster/v1beta1"

	rolloutv1alpha1 "github.com/stolostron/cluster-curator-rollout-controller/api/v1alpha1"
)

const (
	// FinalizerName is the finalizer used by this controller
	FinalizerName = "rollout.open-cluster-management.io/placement-cleanup"

	// LabelRolloutOwner marks resources owned by a ClusterCuratorRollout
	LabelRolloutOwner = "rollout.open-cluster-management.io/owner"

	// LabelRolloutGroupKey marks the group key used to create a placement
	LabelRolloutGroupKey = "rollout.open-cluster-management.io/group-key"

	// LabelRolloutGroupValue marks the group value used to create a placement
	LabelRolloutGroupValue = "rollout.open-cluster-management.io/group-value"

	// LabelRolloutManagedCurator marks ClusterCurators managed by this rollout
	LabelRolloutManagedCurator = "rollout.open-cluster-management.io/managed-curator"

	// RequeueAfterProcessing is the requeue time when processing
	// This is a safety-net interval; primary updates come from watches on ClusterCurator,
	// PlacementDecision, and ManagedCluster resources which trigger immediate reconciles
	RequeueAfterProcessing = 1 * time.Minute

	// RequeueAfterIdle is the requeue time when idle
	RequeueAfterIdle = 5 * time.Minute
)

// ClusterCurator GVK and GVR
var (
	ClusterCuratorGVK = schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1beta1",
		Kind:    "ClusterCurator",
	}

	ClusterCuratorGVR = schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1beta1",
		Resource: "clustercurators",
	}
)

// ClusterCuratorRolloutReconciler reconciles a ClusterCuratorRollout object
type ClusterCuratorRolloutReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// patchStatus updates the status of a ClusterCuratorRollout using a merge patch
// This avoids conflicts that can occur with Update when the object has been modified
// The originalStatus parameter is used to create a proper diff patch that can remove fields
func (r *ClusterCuratorRolloutReconciler) patchStatus(ctx context.Context, rollout *rolloutv1alpha1.ClusterCuratorRollout, originalStatus *rolloutv1alpha1.ClusterCuratorRolloutStatus) error {
	// Create an original object for proper diff patching
	// This ensures that fields set to nil are properly removed
	original := rollout.DeepCopy()
	original.Status = *originalStatus
	return r.Status().Patch(ctx, rollout, client.MergeFrom(original))
}

// +kubebuilder:rbac:groups=rollout.open-cluster-management.io,resources=clustercuratorrollouts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rollout.open-cluster-management.io,resources=clustercuratorrollouts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rollout.open-cluster-management.io,resources=clustercuratorrollouts/finalizers,verbs=update
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=placements,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=placementdecisions,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclustersets,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclustersetbindings,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=clustercurators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

// placementInfo holds information about a placement to be created
type placementInfo struct {
	name         string
	groupKey     string
	groupValue   string
	predicates   []clusterv1beta1.ClusterPredicate
	parentPath   []string
	soakDuration *metav1.Duration                 // step-level soak duration from the filter spec
	concurrency  *rolloutv1alpha1.ConcurrencySpec // concurrency settings from the filter spec
	order        int                              // order index for rollout plan
}

// clusterCuratorCache holds a map of cluster name to ClusterCurator for efficient lookups
// This is built once per reconcile to avoid repeated API calls
type clusterCuratorCache map[string]*unstructured.Unstructured

// buildClusterCuratorCache fetches all ClusterCurators for a rollout in a single API call
// and returns a cache keyed by cluster name for O(1) lookups
func (r *ClusterCuratorRolloutReconciler) buildClusterCuratorCache(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
) clusterCuratorCache {
	logger := log.FromContext(ctx)
	cache := make(clusterCuratorCache)

	// List all ClusterCurators with the managed-curator label for this rollout
	curatorList := &unstructured.UnstructuredList{}
	curatorList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   ClusterCuratorGVK.Group,
		Version: ClusterCuratorGVK.Version,
		Kind:    "ClusterCuratorList",
	})

	if err := r.List(ctx, curatorList, client.MatchingLabels{
		LabelRolloutManagedCurator: rollout.Name,
	}); err != nil {
		logger.Error(err, "Failed to list ClusterCurators for cache")
		return cache
	}

	// Also list ClusterCurators without the label (they might exist from before controller management)
	// by checking clusters from all placements
	allClusterCurators := &unstructured.UnstructuredList{}
	allClusterCurators.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   ClusterCuratorGVK.Group,
		Version: ClusterCuratorGVK.Version,
		Kind:    "ClusterCuratorList",
	})

	if err := r.List(ctx, allClusterCurators); err != nil {
		logger.V(1).Info("Failed to list all ClusterCurators", "error", err)
	}

	// Build cache from labeled curators
	for i := range curatorList.Items {
		curator := &curatorList.Items[i]
		// ClusterCurator name matches cluster name
		cache[curator.GetName()] = curator
	}

	// Add unlabeled curators (prefer labeled ones if both exist)
	for i := range allClusterCurators.Items {
		curator := &allClusterCurators.Items[i]
		name := curator.GetName()
		if _, exists := cache[name]; !exists {
			cache[name] = curator
		}
	}

	logger.V(1).Info("Built ClusterCurator cache", "count", len(cache))
	return cache
}

// Reconcile handles the reconciliation of ClusterCuratorRollout resources
func (r *ClusterCuratorRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the ClusterCuratorRollout
	rollout := &rolloutv1alpha1.ClusterCuratorRollout{}
	if err := r.Get(ctx, req.NamespacedName, rollout); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("ClusterCuratorRollout not found, ignoring")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if we're currently in a soak period FIRST before any other processing
	// This avoids unnecessary work and logging during soak periods
	// The soak check handles both step-level soak (SoakStatus) and batch-level soak (BatchSoakEndsAt)
	if result, handled := r.handleSoakPeriod(ctx, rollout); handled {
		return result, nil
	}

	// Now we're not soaking - proceed with normal reconciliation
	logger.Info("Reconciling ClusterCuratorRollout")
	startTime := time.Now()

	// Capture the original status for proper diff patching
	// This is needed to properly remove fields (like soakStatus) when set to nil
	originalStatus := rollout.Status.DeepCopy()

	// Update status to processing
	reconcileTime := metav1.Now()
	rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseProcessing
	rollout.Status.LastReconcileTime = &reconcileTime
	rollout.Status.LastReconcileError = ""

	// Handle deletion
	if !rollout.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, rollout)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(rollout, FinalizerName) {
		controllerutil.AddFinalizer(rollout, FinalizerName)
		if err := r.Update(ctx, rollout); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if target version has changed - if so, reset status for fresh rollout
	// This must happen before any status processing to ensure clean state
	if r.checkAndResetForVersionChange(ctx, rollout) {
		// Status was reset - save it immediately and requeue to continue with fresh state
		if err := r.patchStatus(ctx, rollout, originalStatus); err != nil {
			logger.Error(err, "Failed to patch status after version change reset")
			return ctrl.Result{Requeue: true}, nil
		}
		logger.Info("Status reset for version change, requeuing")
		return ctrl.Result{Requeue: true}, nil
	}

	// If we had a soak status but it's now expired, clear it
	// (This should have been caught by handleSoakPeriod, but handle edge cases)
	if rollout.Status.SoakStatus != nil {
		logger.Info("Soak period ended, resuming rollout",
			"groupPath", rollout.Status.SoakStatus.GroupPath)
		r.clearSoakStatus(rollout)
	}

	// Process the selection spec and generate placements
	result, err := r.reconcilePlacements(ctx, rollout)
	if err != nil {
		logger.Error(err, "Failed to reconcile placements")
		rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseError
		rollout.Status.LastReconcileError = err.Error()
		duration := metav1.Duration{Duration: time.Since(startTime)}
		rollout.Status.LastReconcileDuration = &duration
		if statusErr := r.patchStatus(ctx, rollout, originalStatus); statusErr != nil {
			logger.Error(statusErr, "Failed to patch status on error")
			// Will retry on requeue
		}
		return ctrl.Result{RequeueAfter: RequeueAfterProcessing}, err
	}

	// Update status with results
	// Note: Phase and Message are set by reconcileRolloutProgression based on rollout state
	// Only set to Idle if no specific phase was set (e.g., no rollout plan)
	if rollout.Status.Phase == "" {
		rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseIdle
		rollout.Status.Message = fmt.Sprintf("Processed %d placements with %d total clusters",
			len(rollout.Status.Placements), rollout.Status.TotalCounts.Total)
	}
	rollout.Status.ObservedGeneration = rollout.Generation
	rollout.Status.ObservedOpenShiftVersion = rollout.Spec.OpenShiftVersion
	duration := metav1.Duration{Duration: time.Since(startTime)}
	rollout.Status.LastReconcileDuration = &duration

	if err := r.patchStatus(ctx, rollout, originalStatus); err != nil {
		logger.Error(err, "Failed to patch status")
		return ctrl.Result{Requeue: true}, nil
	}

	logger.Info("Reconciliation complete",
		"duration", duration.Duration,
		"placements", len(rollout.Status.Placements),
		"totalClusters", rollout.Status.TotalCounts.Total)

	return result, nil
}

// handleSoakPeriod checks if we're in a soak period and handles it efficiently.
// This function is called early in Reconcile to avoid unnecessary work during soak.
// It checks both step-level soak (SoakStatus) and batch-level soak (BatchSoakEndsAt).
// Returns (result, handled) - if handled is true, the caller should return immediately.
//
// Behavior:
// - If soak is still active: return with requeue set to when soak ends (no status update needed)
// - If soak has expired: return handled=false so normal reconcile continues
// - Handles restart case: if controller restarts during soak, correctly calculates remaining time
func (r *ClusterCuratorRolloutReconciler) handleSoakPeriod(ctx context.Context, rollout *rolloutv1alpha1.ClusterCuratorRollout) (ctrl.Result, bool) {
	logger := log.FromContext(ctx)
	now := time.Now()

	// If onFailure=pause, check for failures even during soak periods
	// Failures should interrupt the soak and pause the rollout
	if rollout.Spec.OnFailure == rolloutv1alpha1.OnFailurePause {
		currentFailures := r.countCurrentFailures(ctx, rollout)
		if currentFailures > 0 {
			logger.Info("Failures detected during soak period, interrupting soak to pause",
				"failedCount", currentFailures)
			// Clear soak status so we proceed to normal reconcile which will pause
			rollout.Status.SoakStatus = nil
			// Also clear any step-level soak states
			for i := range rollout.Status.RolloutPlan {
				step := &rollout.Status.RolloutPlan[i]
				if step.State == rolloutv1alpha1.RolloutStepStateSoaking ||
					step.State == rolloutv1alpha1.RolloutStepStateBatchSoaking {
					step.State = rolloutv1alpha1.RolloutStepStateActive
					step.SoakEndsAt = nil
					step.BatchSoakEndsAt = nil
				}
			}
			// Continue to normal reconcile which will handle the failures
			return ctrl.Result{}, false
		}
	}

	// Check step-level soak (stored in SoakStatus)
	if rollout.Status.SoakStatus != nil && rollout.Status.SoakStatus.EndsAt != nil {
		endsAt := rollout.Status.SoakStatus.EndsAt.Time

		if now.Before(endsAt) {
			// Still soaking - requeue for when soak ends
			timeRemaining := endsAt.Sub(now)
			logger.V(1).Info("Soak period active, skipping reconcile",
				"groupPath", rollout.Status.SoakStatus.GroupPath,
				"endsAt", endsAt.Format("15:04:05"),
				"timeRemaining", formatDuration(timeRemaining))

			// Requeue when soak ends (add small buffer)
			return ctrl.Result{RequeueAfter: timeRemaining + time.Second}, true
		}
		// Soak expired - let normal reconcile handle clearing it
		logger.Info("Soak period ended",
			"groupPath", rollout.Status.SoakStatus.GroupPath)
		return ctrl.Result{}, false
	}

	// Check batch-level soak (stored in step.BatchSoakEndsAt)
	for i := range rollout.Status.RolloutPlan {
		step := &rollout.Status.RolloutPlan[i]
		if step.State == rolloutv1alpha1.RolloutStepStateBatchSoaking && step.BatchSoakEndsAt != nil {
			endsAt := step.BatchSoakEndsAt.Time

			if now.Before(endsAt) {
				// Still in batch soak - requeue for when soak ends
				timeRemaining := endsAt.Sub(now)
				logger.V(1).Info("Batch soak period active, skipping reconcile",
					"step", step.PlacementName,
					"batch", step.CurrentBatch,
					"endsAt", endsAt.Format("15:04:05"),
					"timeRemaining", formatDuration(timeRemaining))

				// Requeue when soak ends (add small buffer)
				return ctrl.Result{RequeueAfter: timeRemaining + time.Second}, true
			}
			// Batch soak expired - let normal reconcile handle clearing it
			logger.Info("Batch soak period ended",
				"step", step.PlacementName,
				"batch", step.CurrentBatch)
			return ctrl.Result{}, false
		}

		// Also check for step-level soaking state
		if step.State == rolloutv1alpha1.RolloutStepStateSoaking && step.SoakEndsAt != nil {
			endsAt := step.SoakEndsAt.Time

			if now.Before(endsAt) {
				// Still in step soak - requeue for when soak ends
				timeRemaining := endsAt.Sub(now)
				logger.V(1).Info("Step soak period active, skipping reconcile",
					"step", step.PlacementName,
					"endsAt", endsAt.Format("15:04:05"),
					"timeRemaining", formatDuration(timeRemaining))

				// Requeue when soak ends (add small buffer)
				return ctrl.Result{RequeueAfter: timeRemaining + time.Second}, true
			}
			// Step soak expired - let normal reconcile handle it
			logger.Info("Step soak period ended",
				"step", step.PlacementName)
			return ctrl.Result{}, false
		}
	}

	// Not in any soak period
	return ctrl.Result{}, false
}

// checkSoakStatus checks if we're currently in a soak period
// Returns (isSoaking, timeUntilSoakEnds)
// Note: This is a simpler check used after handleSoakPeriod for edge cases
func (r *ClusterCuratorRolloutReconciler) checkSoakStatus(rollout *rolloutv1alpha1.ClusterCuratorRollout) (bool, time.Duration) {
	if rollout.Status.SoakStatus == nil {
		return false, 0
	}

	if rollout.Status.SoakStatus.EndsAt == nil {
		return false, 0
	}

	now := time.Now()
	endsAt := rollout.Status.SoakStatus.EndsAt.Time

	if now.Before(endsAt) {
		return true, endsAt.Sub(now)
	}

	return false, 0
}

// clearSoakStatus clears the soak status when the soak period has ended
func (r *ClusterCuratorRolloutReconciler) clearSoakStatus(rollout *rolloutv1alpha1.ClusterCuratorRollout) {
	rollout.Status.SoakStatus = nil
}

// formatDuration formats a duration in a human-friendly way, rounding to appropriate units
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "0s"
	}
	// Round to seconds
	d = d.Round(time.Second)

	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}

	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60

	if d < time.Hour {
		if seconds == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}

	hours := int(d.Hours())
	minutes = minutes % 60

	if minutes == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh%dm", hours, minutes)
}

// Note: In the new model, soak duration is specified directly on each filter's SoakDuration field.
// The controller will use filter.SoakDuration to determine how long to wait after all clusters
// in that filter complete before proceeding to the next sibling filter.

// startSoak initiates a soak period for a completed group
func (r *ClusterCuratorRolloutReconciler) startSoak(rollout *rolloutv1alpha1.ClusterCuratorRollout, groupPath string, duration time.Duration) {
	now := metav1.Now()
	endsAt := metav1.NewTime(now.Add(duration))

	rollout.Status.SoakStatus = &rolloutv1alpha1.SoakStatus{
		GroupPath: groupPath,
		StartedAt: &now,
		EndsAt:    &endsAt,
		Duration:  &metav1.Duration{Duration: duration},
	}
	rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseSoaking
}

// checkAndResetForVersionChange checks if the target OpenShift version has changed
// and resets all status fields if it has. This ensures a fresh rollout when the
// target version changes.
// Returns true if status was reset (caller should continue with fresh state).
func (r *ClusterCuratorRolloutReconciler) checkAndResetForVersionChange(ctx context.Context, rollout *rolloutv1alpha1.ClusterCuratorRollout) bool {
	logger := log.FromContext(ctx)

	// Get the current target version from spec
	targetVersion := rollout.Spec.OpenShiftVersion

	// Get the last observed version from status
	observedVersion := rollout.Status.ObservedOpenShiftVersion

	// If the versions match (or this is first reconcile with empty observed version that matches),
	// no reset is needed
	if observedVersion == targetVersion {
		return false
	}

	// If observedVersion is empty, this is the first time processing this resource
	// or it's from before we added version tracking. Set it without full reset.
	if observedVersion == "" {
		logger.Info("First reconcile or migration: setting ObservedOpenShiftVersion",
			"targetVersion", targetVersion)
		rollout.Status.ObservedOpenShiftVersion = targetVersion
		return false
	}

	// Version has changed - reset all status fields for a fresh rollout
	logger.Info("Target OpenShift version changed, resetting rollout status",
		"previousVersion", observedVersion,
		"newVersion", targetVersion)

	// Reset the entire status while preserving only essential metadata
	now := metav1.Now()
	rollout.Status = rolloutv1alpha1.ClusterCuratorRolloutStatus{
		Phase:                    rolloutv1alpha1.RolloutPhaseProcessing,
		LastReconcileTime:        &now,
		ObservedOpenShiftVersion: targetVersion,
		Message:                  fmt.Sprintf("Rollout reset for new target version %s (was %s)", targetVersion, observedVersion),
		// All other fields are reset to zero values:
		// - Placements: nil
		// - TotalCounts: nil
		// - RolloutPlan: nil
		// - CurrentStepIndex: nil
		// - SoakStatus: nil
		// - FailedClusters: nil
		// - InProgressClusters: nil
		// - Conditions: nil
		// - LastReconcileError: ""
		// - LastReconcileDuration: nil
	}

	return true
}

// handleDeletion handles the cleanup when the ClusterCuratorRollout is being deleted
func (r *ClusterCuratorRolloutReconciler) handleDeletion(ctx context.Context, rollout *rolloutv1alpha1.ClusterCuratorRollout) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Handling deletion of ClusterCuratorRollout")

	// Delete all placements owned by this rollout
	if err := r.cleanupOwnedPlacements(ctx, rollout); err != nil {
		logger.Error(err, "Failed to cleanup owned placements")
		return ctrl.Result{}, err
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(rollout, FinalizerName)
	if err := r.Update(ctx, rollout); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// cleanupOwnedPlacements deletes all placements created by this rollout
func (r *ClusterCuratorRolloutReconciler) cleanupOwnedPlacements(ctx context.Context, rollout *rolloutv1alpha1.ClusterCuratorRollout) error {
	logger := log.FromContext(ctx)

	placementList := &clusterv1beta1.PlacementList{}
	if err := r.List(ctx, placementList, client.InNamespace(rollout.Namespace), client.MatchingLabels{
		LabelRolloutOwner: rollout.Name,
	}); err != nil {
		return err
	}

	for _, placement := range placementList.Items {
		logger.Info("Deleting placement", "name", placement.Name)
		if err := r.Delete(ctx, &placement); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// reconcilePlacements processes the selection spec and creates/updates placements
func (r *ClusterCuratorRolloutReconciler) reconcilePlacements(ctx context.Context, rollout *rolloutv1alpha1.ClusterCuratorRollout) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Collect all placement infos from the selection spec
	// Note: We no longer need to list ManagedClusters - placements are created based on
	// the filter hierarchy, not discovered from cluster labels
	placementInfos, err := r.collectPlacementInfos(ctx, rollout, &rollout.Spec.SelectionSpec, nil, nil)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to collect placement infos: %w", err)
	}

	logger.Info("Collected placement infos", "count", len(placementInfos))

	// Get existing placements owned by this rollout
	existingPlacements := &clusterv1beta1.PlacementList{}
	if err := r.List(ctx, existingPlacements, client.InNamespace(rollout.Namespace), client.MatchingLabels{
		LabelRolloutOwner: rollout.Name,
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list existing placements: %w", err)
	}

	// Build a map of existing placements
	existingPlacementMap := make(map[string]*clusterv1beta1.Placement)
	for i := range existingPlacements.Items {
		existingPlacementMap[existingPlacements.Items[i].Name] = &existingPlacements.Items[i]
	}

	// Create or update placements
	desiredPlacementNames := make(map[string]bool)
	placementStatuses := []rolloutv1alpha1.PlacementStatus{}

	for _, info := range placementInfos {
		desiredPlacementNames[info.name] = true

		placement := r.buildPlacement(rollout, info)
		if existing, ok := existingPlacementMap[info.name]; ok {
			// Update existing placement if needed
			if r.placementNeedsUpdate(existing, placement) {
				existing.Spec = placement.Spec
				if err := r.Update(ctx, existing); err != nil {
					return ctrl.Result{}, fmt.Errorf("failed to update placement %s: %w", info.name, err)
				}
				logger.Info("Updated placement", "name", info.name)
			}
		} else {
			// Create new placement
			if err := controllerutil.SetControllerReference(rollout, placement, r.Scheme); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to set controller reference: %w", err)
			}
			if err := r.Create(ctx, placement); err != nil {
				if !errors.IsAlreadyExists(err) {
					return ctrl.Result{}, fmt.Errorf("failed to create placement %s: %w", info.name, err)
				}
			}
			logger.Info("Created placement", "name", info.name)
		}

		// Build placement status (total will be populated below)
		now := metav1.Now()
		placementStatuses = append(placementStatuses, rolloutv1alpha1.PlacementStatus{
			Name:        info.name,
			Namespace:   rollout.Namespace,
			GroupKey:    info.groupKey,
			GroupValue:  info.groupValue,
			Generated:   true,
			Total:       0, // Will be populated below
			LastUpdated: &now,
		})
	}

	// Delete placements that are no longer needed
	for name, existing := range existingPlacementMap {
		if !desiredPlacementNames[name] {
			logger.Info("Deleting orphaned placement", "name", name)
			if err := r.Delete(ctx, existing); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("failed to delete placement %s: %w", name, err)
			}
		}
	}

	// Update placement statuses with cluster counts from PlacementDecisions
	placementStatuses, totalCounts := r.updatePlacementStatusesWithCounts(ctx, rollout.Namespace, placementStatuses, rollout.Spec.OpenShiftVersion)

	rollout.Status.Placements = placementStatuses
	rollout.Status.TotalCounts = &totalCounts

	// Build the rollout plan from placement infos (preserving existing state)
	rollout.Status.RolloutPlan = r.buildRolloutPlan(placementInfos, rollout.Status.RolloutPlan)

	// Update rollout plan step counts from placement statuses
	r.updateRolloutPlanCounts(rollout)

	// Manage rollout progression - activate steps and create ClusterCurators
	result, err := r.reconcileRolloutProgression(ctx, rollout)
	if err != nil {
		return result, err
	}

	// Recalculate TotalCounts from step counts after progression
	// This ensures TotalCounts reflects any state changes (failures, completions)
	// that occurred during reconcileRolloutProgression
	r.aggregateTotalCountsFromSteps(ctx, rollout)

	// Collect failed clusters for troubleshooting
	rollout.Status.FailedClusters = r.collectFailedClusters(ctx, rollout)
	rollout.Status.InProgressClusters = nil

	return result, nil
}

// updateRolloutPlanCounts updates the total count in the rollout plan from placement statuses
func (r *ClusterCuratorRolloutReconciler) updateRolloutPlanCounts(rollout *rolloutv1alpha1.ClusterCuratorRollout) {
	// Build a map of placement name to total for quick lookup
	placementTotalMap := make(map[string]int)
	for i := range rollout.Status.Placements {
		placementTotalMap[rollout.Status.Placements[i].Name] = rollout.Status.Placements[i].Total
	}

	// Update each step's total from the corresponding placement status
	// Detailed state counts (pending, inProgress, etc.) are updated via updateStepCounts
	for i := range rollout.Status.RolloutPlan {
		step := &rollout.Status.RolloutPlan[i]
		if total, ok := placementTotalMap[step.PlacementName]; ok {
			step.Counts.Total = total
		}
	}
}

// aggregateTotalCountsFromSteps recalculates TotalCounts by refreshing and summing counts
// from all steps in the rollout plan. This ensures TotalCounts reflects the current state
// of all clusters, including failures that may have occurred during soak periods or in
// steps that haven't been actively processed yet.
// Uses a batch-fetched cache for efficient ClusterCurator lookups (1-2 API calls instead of ~100)
func (r *ClusterCuratorRolloutReconciler) aggregateTotalCountsFromSteps(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
) {
	logger := log.FromContext(ctx)
	totalCounts := rolloutv1alpha1.StateCounts{}

	// Build ClusterCurator cache once for all steps - this is the key performance optimization
	// Instead of ~100 individual GET calls, we do 1-2 LIST calls
	cache := r.buildClusterCuratorCache(ctx, rollout)
	logger.V(1).Info("Refreshing all step counts with cached ClusterCurators", "cacheSize", len(cache))

	for i := range rollout.Status.RolloutPlan {
		step := &rollout.Status.RolloutPlan[i]

		// Refresh step counts from actual cluster states using the cache
		// This is especially important for:
		// - Pending steps that haven't been activated yet
		// - Soaking steps where failures may have occurred during the soak period
		// - Completed/Failed steps that may need count updates
		r.updateStepCountsFromClustersWithCache(ctx, rollout, step, cache)

		totalCounts.Total += step.Counts.Total
		totalCounts.Pending += step.Counts.Pending
		totalCounts.InProgress += step.Counts.InProgress
		totalCounts.Completed += step.Counts.Completed
		totalCounts.Failed += step.Counts.Failed
		totalCounts.Skipped += step.Counts.Skipped
	}

	rollout.Status.TotalCounts = &totalCounts
}

// collectPlacementInfos recursively collects placement information from the selection spec
//
// New model:
// - "group" is a branching point that iterates through its filter children (in order)
// - "filter" adds label predicates and builds the path; creates a placement if no children (leaf)
// - Order of filter children defines the rollout priority order
// - Placements are always created based on the filter hierarchy, not discovered from clusters
//
// SoakDuration inheritance:
// - A filter's soakDuration means "wait after all my clusters complete before my sibling starts"
// - When a filter has children, its soakDuration is inherited by its LAST descendant placement
// - This ensures the soak happens after ALL of the filter's clusters complete, then the sibling starts
//
// Important: We use a single ClusterPredicate with merged matchLabels so that all label
// requirements are AND'd together. Multiple predicates in a Placement are OR'd, which would
// cause incorrect cluster selection.
func (r *ClusterCuratorRolloutReconciler) collectPlacementInfos(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	spec *rolloutv1alpha1.SelectionSpec,
	parentMatchLabels map[string]string,
	parentPath []string,
) ([]placementInfo, error) {
	return r.collectPlacementInfosWithInheritedSoak(ctx, rollout, spec, parentMatchLabels, parentPath, nil)
}

// collectPlacementInfosWithInheritedSoak is the internal implementation that tracks inherited soak duration
func (r *ClusterCuratorRolloutReconciler) collectPlacementInfosWithInheritedSoak(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	spec *rolloutv1alpha1.SelectionSpec,
	parentMatchLabels map[string]string,
	parentPath []string,
	inheritedSoakDuration *metav1.Duration,
) ([]placementInfo, error) {
	logger := log.FromContext(ctx)
	var result []placementInfo

	switch spec.Type {
	case rolloutv1alpha1.SelectionTypeGroup:
		// Group is a branching point - it iterates through its filter children in order.
		// The groupByLabelKey is informational (for validation that children filter on this key).
		// Groups do NOT create placements themselves - their filter children do.
		if spec.GroupByLabelKey == "" {
			return nil, fmt.Errorf("group type requires groupByLabelKey to be set")
		}

		if len(spec.Children) == 0 {
			return nil, fmt.Errorf("group type requires at least one filter child")
		}

		logger.Info("Processing group", "groupByLabelKey", spec.GroupByLabelKey, "childCount", len(spec.Children))

		// Iterate through children in order (order defines priority)
		for i, child := range spec.Children {
			if child.Type != rolloutv1alpha1.SelectionTypeFilter {
				return nil, fmt.Errorf("group children must be of type 'filter', got '%s' at index %d", child.Type, i)
			}

			// Groups don't have soakDuration, so we don't pass inherited soak from groups
			// Each filter child handles its own soakDuration
			childInfos, err := r.collectPlacementInfosWithInheritedSoak(ctx, rollout, &child, parentMatchLabels, parentPath, nil)
			if err != nil {
				return nil, err
			}
			result = append(result, childInfos...)
		}

	case rolloutv1alpha1.SelectionTypeFilter:
		// Filter adds label requirements and builds the path.
		// If no children, this is a leaf node and creates a Placement.
		// We merge all matchLabels into a single map so they are AND'd in one predicate.
		mergedLabels := cloneMatchLabels(parentMatchLabels)
		currentPath := clonePath(parentPath)

		if spec.LabelSelector != nil {
			// Merge this filter's matchLabels into the accumulated labels
			for k, v := range spec.LabelSelector.MatchLabels {
				mergedLabels[k] = v
			}

			// Build path segment from label selector for naming
			pathSegment := buildPathSegmentFromSelector(spec.LabelSelector)
			if pathSegment != "" {
				currentPath = append(currentPath, pathSegment)
			}
		}

		// If there are children, recurse into them
		if len(spec.Children) > 0 {
			for i, child := range spec.Children {
				// Determine the soak duration to pass to this child
				// Only the LAST child inherits the parent's soakDuration
				// This ensures the soak happens after ALL of parent's descendants complete
				var childInheritedSoak *metav1.Duration
				isLastChild := (i == len(spec.Children)-1)

				if isLastChild {
					// Last child inherits: use this filter's soakDuration if set, else propagate inherited
					if spec.SoakDuration != nil {
						childInheritedSoak = spec.SoakDuration
					} else {
						childInheritedSoak = inheritedSoakDuration
					}
				}
				// Non-last children don't inherit soak (they may have their own)

				childInfos, err := r.collectPlacementInfosWithInheritedSoak(ctx, rollout, &child, mergedLabels, currentPath, childInheritedSoak)
				if err != nil {
					return nil, err
				}
				result = append(result, childInfos...)
			}
		} else {
			// Leaf node - create a placement with a single predicate containing all merged labels
			var predicates []clusterv1beta1.ClusterPredicate
			if len(mergedLabels) > 0 {
				predicates = []clusterv1beta1.ClusterPredicate{
					{
						RequiredClusterSelector: clusterv1beta1.ClusterSelector{
							LabelSelector: metav1.LabelSelector{
								MatchLabels: mergedLabels,
							},
						},
					},
				}
			}

			// Determine effective soak duration:
			// 1. If leaf has its own soakDuration, use it
			// 2. Else if inherited soakDuration exists, use it
			// 3. Else no soak
			effectiveSoakDuration := spec.SoakDuration
			if effectiveSoakDuration == nil {
				effectiveSoakDuration = inheritedSoakDuration
			}

			info := placementInfo{
				name:         generatePlacementName(rollout.Name, currentPath),
				predicates:   predicates,
				parentPath:   currentPath,
				soakDuration: effectiveSoakDuration,
				concurrency:  spec.Concurrency,
			}
			logger.V(1).Info("Collected placement info for filter leaf", "name", info.name, "path", currentPath, "matchLabels", mergedLabels, "hasConcurrency", spec.Concurrency != nil, "soakDuration", effectiveSoakDuration)
			result = append(result, info)
		}

	case rolloutv1alpha1.SelectionTypePlacement:
		// For placement type, we reference an existing placement
		// This is handled differently - we don't generate new placements
		if spec.PlacementRef == nil {
			return nil, fmt.Errorf("placement type requires placementRef")
		}
		// We don't generate placements for this type, just track the reference
		logger.Info("Placement type uses existing placement", "ref", spec.PlacementRef.Name)
	}

	return result, nil
}

// buildPathSegmentFromSelector creates a path segment from a label selector for naming
func buildPathSegmentFromSelector(selector *metav1.LabelSelector) string {
	if selector == nil {
		return ""
	}

	var segments []string

	// Add matchLabels
	for key, value := range selector.MatchLabels {
		segments = append(segments, fmt.Sprintf("%s-%s", key, value))
	}

	// Sort for deterministic naming
	sort.Strings(segments)

	if len(segments) == 0 {
		return ""
	}

	return strings.Join(segments, "-")
}

// generatePlacementName generates a unique placement name based on the rollout name and path
func generatePlacementName(rolloutName string, path []string) string {
	if len(path) == 0 {
		return fmt.Sprintf("%s-placement", rolloutName)
	}

	// Create a deterministic name from the path
	name := rolloutName
	for _, segment := range path {
		// Sanitize segment for Kubernetes naming
		sanitized := sanitizeForK8sName(segment)
		name = fmt.Sprintf("%s-%s", name, sanitized)
	}

	// Truncate if too long (Kubernetes name limit is 253 characters)
	if len(name) > 63 {
		// Use a hash for uniqueness
		name = fmt.Sprintf("%s-%x", name[:50], hashString(name))
	}

	return name
}

// sanitizeForK8sName sanitizes a string for use in Kubernetes resource names
func sanitizeForK8sName(s string) string {
	result := make([]byte, 0, len(s))
	for _, c := range []byte(s) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			result = append(result, c)
		} else if c >= 'A' && c <= 'Z' {
			result = append(result, c+32) // lowercase
		} else if c == '=' || c == '_' || c == '.' {
			result = append(result, '-')
		}
	}
	return string(result)
}

// hashString creates a simple hash of a string
func hashString(s string) uint32 {
	var hash uint32
	for _, c := range s {
		hash = hash*31 + uint32(c)
	}
	return hash
}

// cloneMatchLabels creates a copy of a matchLabels map
func cloneMatchLabels(labels map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range labels {
		result[k] = v
	}
	return result
}

// clonePath creates a copy of a path slice
func clonePath(path []string) []string {
	if path == nil {
		return nil
	}
	result := make([]string, len(path))
	copy(result, path)
	return result
}

// buildPlacement creates a Placement resource from placementInfo
func (r *ClusterCuratorRolloutReconciler) buildPlacement(rollout *rolloutv1alpha1.ClusterCuratorRollout, info placementInfo) *clusterv1beta1.Placement {
	placement := &clusterv1beta1.Placement{
		ObjectMeta: metav1.ObjectMeta{
			Name:      info.name,
			Namespace: rollout.Namespace,
			Labels: map[string]string{
				LabelRolloutOwner: rollout.Name,
			},
		},
		Spec: clusterv1beta1.PlacementSpec{
			Predicates: info.predicates,
			// Tolerate unreachable clusters so placements can select them
			// This allows the rollout controller to manage clusters regardless of availability
			Tolerations: []clusterv1beta1.Toleration{
				{
					Key:      "cluster.open-cluster-management.io/unreachable",
					Operator: clusterv1beta1.TolerationOpExists,
				},
			},
		},
	}

	if info.groupKey != "" {
		placement.Labels[LabelRolloutGroupKey] = info.groupKey
	}
	if info.groupValue != "" {
		placement.Labels[LabelRolloutGroupValue] = info.groupValue
	}

	return placement
}

// placementNeedsUpdate checks if a placement needs to be updated
func (r *ClusterCuratorRolloutReconciler) placementNeedsUpdate(existing, desired *clusterv1beta1.Placement) bool {
	// Compare predicates (simplified comparison)
	if len(existing.Spec.Predicates) != len(desired.Spec.Predicates) {
		return true
	}
	// More detailed comparison could be added here
	return false
}

// updatePlacementStatusesWithCounts updates placement statuses with cluster counts from PlacementDecisions
// and ClusterCurator statuses. This returns condensed StateCounts instead of individual cluster details.
// targetVersion is used to detect clusters that should be skipped due to already being at the target version
func (r *ClusterCuratorRolloutReconciler) updatePlacementStatusesWithCounts(
	ctx context.Context,
	namespace string,
	statuses []rolloutv1alpha1.PlacementStatus,
	targetVersion string,
) ([]rolloutv1alpha1.PlacementStatus, rolloutv1alpha1.StateCounts) {
	logger := log.FromContext(ctx)
	totalCounts := rolloutv1alpha1.StateCounts{}

	for i := range statuses {
		// List PlacementDecisions for this placement
		decisions := &clusterv1beta1.PlacementDecisionList{}
		if err := r.List(ctx, decisions, client.InNamespace(namespace), client.MatchingLabels{
			clusterv1beta1.PlacementLabel: statuses[i].Name,
		}); err != nil {
			continue // Skip on error, will be updated on next reconcile
		}

		// Collect all cluster names from decisions
		var clusterNames []string
		for _, decision := range decisions.Items {
			for _, d := range decision.Status.Decisions {
				clusterNames = append(clusterNames, d.ClusterName)
			}
		}

		// Get counts by checking ClusterCurator status for each cluster
		counts := r.getClusterUpgradeCounts(ctx, clusterNames, targetVersion)
		statuses[i].Total = counts.Total

		// Aggregate into totals
		totalCounts.Total += counts.Total
		totalCounts.Pending += counts.Pending
		totalCounts.InProgress += counts.InProgress
		totalCounts.Completed += counts.Completed
		totalCounts.Failed += counts.Failed
		totalCounts.Skipped += counts.Skipped
	}

	logger.V(1).Info("Updated placement status counts",
		"placements", len(statuses),
		"totalClusters", totalCounts.Total,
		"pending", totalCounts.Pending,
		"inProgress", totalCounts.InProgress,
		"completed", totalCounts.Completed,
		"failed", totalCounts.Failed)

	return statuses, totalCounts
}

// getClusterUpgradeCounts queries ClusterCurator status for each cluster and returns aggregated counts
// targetVersion is used to detect clusters that should be skipped due to already being at the target version
// Note: For batch operations, prefer getClusterUpgradeCountsWithCache for better performance
func (r *ClusterCuratorRolloutReconciler) getClusterUpgradeCounts(
	ctx context.Context,
	clusterNames []string,
	targetVersion string,
) rolloutv1alpha1.StateCounts {
	counts := rolloutv1alpha1.StateCounts{
		Total: len(clusterNames),
	}

	for _, clusterName := range clusterNames {
		state := r.getClusterCuratorState(ctx, clusterName, targetVersion)
		switch state {
		case rolloutv1alpha1.ClusterUpgradeStatePending:
			counts.Pending++
		case rolloutv1alpha1.ClusterUpgradeStateInProgress:
			counts.InProgress++
		case rolloutv1alpha1.ClusterUpgradeStateCompleted:
			counts.Completed++
		case rolloutv1alpha1.ClusterUpgradeStateFailed:
			counts.Failed++
		case rolloutv1alpha1.ClusterUpgradeStateSkipped:
			counts.Skipped++
		default:
			counts.Pending++ // Default to pending if unknown
		}
	}

	return counts
}

// getClusterUpgradeCountsWithCache is like getClusterUpgradeCounts but uses a pre-built cache
// This is significantly faster when processing many clusters
func (r *ClusterCuratorRolloutReconciler) getClusterUpgradeCountsWithCache(
	ctx context.Context,
	clusterNames []string,
	targetVersion string,
	cache clusterCuratorCache,
) rolloutv1alpha1.StateCounts {
	counts := rolloutv1alpha1.StateCounts{
		Total: len(clusterNames),
	}

	for _, clusterName := range clusterNames {
		state := r.getClusterCuratorStateWithCache(ctx, clusterName, targetVersion, cache)
		switch state {
		case rolloutv1alpha1.ClusterUpgradeStatePending:
			counts.Pending++
		case rolloutv1alpha1.ClusterUpgradeStateInProgress:
			counts.InProgress++
		case rolloutv1alpha1.ClusterUpgradeStateCompleted:
			counts.Completed++
		case rolloutv1alpha1.ClusterUpgradeStateFailed:
			counts.Failed++
		case rolloutv1alpha1.ClusterUpgradeStateSkipped:
			counts.Skipped++
		default:
			counts.Pending++ // Default to pending if unknown
		}
	}

	return counts
}

// collectFailedClusters collects details about failed clusters for troubleshooting
// Returns a list of FailedClusterInfo for all clusters in Failed state
func (r *ClusterCuratorRolloutReconciler) collectFailedClusters(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
) []rolloutv1alpha1.FailedClusterInfo {
	logger := log.FromContext(ctx)
	var failedClusters []rolloutv1alpha1.FailedClusterInfo

	// Iterate through all steps in the rollout plan
	for _, step := range rollout.Status.RolloutPlan {
		clusters, err := r.getClustersFromPlacement(ctx, rollout.Namespace, step.PlacementName)
		if err != nil {
			logger.V(1).Info("Failed to get clusters from placement for failed cluster collection",
				"placement", step.PlacementName, "error", err)
			continue
		}

		for _, clusterName := range clusters {
			state := r.getClusterCuratorState(ctx, clusterName, rollout.Spec.OpenShiftVersion)
			if state == rolloutv1alpha1.ClusterUpgradeStateFailed {
				// Get failure details from ClusterCurator
				failureReason := r.getClusterCuratorFailureReason(ctx, clusterName)
				currentVersion := r.getClusterCurrentVersion(ctx, clusterName)

				failedClusters = append(failedClusters, rolloutv1alpha1.FailedClusterInfo{
					Name:           clusterName,
					PlacementName:  step.PlacementName,
					CurrentVersion: currentVersion,
					FailureReason:  failureReason,
					FailedAt:       &metav1.Time{Time: time.Now()},
				})

				// Limit to MaxFailedClusterDetails
				if len(failedClusters) >= rolloutv1alpha1.MaxFailedClusterDetails {
					return failedClusters
				}
			}
		}
	}

	return failedClusters
}

// getClusterCuratorFailureReason extracts the failure reason from a ClusterCurator's status
func (r *ClusterCuratorRolloutReconciler) getClusterCuratorFailureReason(
	ctx context.Context,
	clusterName string,
) string {
	curator := &unstructured.Unstructured{}
	curator.SetGroupVersionKind(ClusterCuratorGVK)

	err := r.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: clusterName}, curator)
	if err != nil {
		return "Unable to retrieve ClusterCurator"
	}

	// Check status conditions for failure message
	conditions, found, _ := unstructured.NestedSlice(curator.Object, "status", "conditions")
	if !found {
		return "No status conditions available"
	}

	for _, condInterface := range conditions {
		cond, ok := condInterface.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(cond, "type")
		message, _, _ := unstructured.NestedString(cond, "message")

		// Look for the clustercurator-job condition which typically has the failure message
		if condType == "clustercurator-job" && message != "" {
			if strings.Contains(strings.ToLower(message), "failed") ||
				strings.Contains(strings.ToLower(message), "error") {
				return message
			}
		}
	}

	return "Unknown failure reason"
}

// getClusterCurrentVersion gets the current OpenShift version from ManagedCluster
func (r *ClusterCuratorRolloutReconciler) getClusterCurrentVersion(
	ctx context.Context,
	clusterName string,
) string {
	mc := &clusterv1.ManagedCluster{}
	err := r.Get(ctx, types.NamespacedName{Name: clusterName}, mc)
	if err != nil {
		return "unknown"
	}

	if mc.Labels != nil {
		if version, ok := mc.Labels["openshiftVersion"]; ok {
			return version
		}
	}
	return "unknown"
}

// getFailedClusterNamesForStep returns the names of clusters that failed upgrade in a step
func (r *ClusterCuratorRolloutReconciler) getFailedClusterNamesForStep(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	step *rolloutv1alpha1.RolloutStep,
) []string {
	var failedNames []string

	clusters, err := r.getClustersFromPlacement(ctx, rollout.Namespace, step.PlacementName)
	if err != nil {
		return failedNames
	}

	for _, clusterName := range clusters {
		state := r.getClusterCuratorState(ctx, clusterName, rollout.Spec.OpenShiftVersion)
		if state == rolloutv1alpha1.ClusterUpgradeStateFailed {
			failedNames = append(failedNames, clusterName)
		}
	}

	return failedNames
}

// getClusterCuratorStateWithCache checks the ClusterCurator status using a pre-built cache
// This is more efficient when checking many clusters as it avoids repeated API calls
func (r *ClusterCuratorRolloutReconciler) getClusterCuratorStateWithCache(
	ctx context.Context,
	clusterName string,
	targetVersion string,
	cache clusterCuratorCache,
) rolloutv1alpha1.ClusterUpgradeState {
	curator, exists := cache[clusterName]
	if !exists {
		// No ClusterCurator in cache - check if cluster should be skipped due to version
		if targetVersion != "" {
			shouldSkip, _ := r.shouldSkipClusterUpgrade(ctx, clusterName, targetVersion)
			if shouldSkip {
				return rolloutv1alpha1.ClusterUpgradeStateSkipped
			}
		}
		// No ClusterCurator and not skipped means pending (not started)
		return rolloutv1alpha1.ClusterUpgradeStatePending
	}

	return r.evaluateClusterCuratorState(ctx, curator, clusterName, targetVersion)
}

// getClusterCuratorState checks the ClusterCurator status for a cluster and returns its upgrade state
// If targetVersion is provided, clusters already at or above that version will return Skipped
// Note: For batch operations, prefer getClusterCuratorStateWithCache for better performance
func (r *ClusterCuratorRolloutReconciler) getClusterCuratorState(
	ctx context.Context,
	clusterName string,
	targetVersion string,
) rolloutv1alpha1.ClusterUpgradeState {
	// ClusterCurator is in the cluster's namespace with the same name
	curator := &unstructured.Unstructured{}
	curator.SetGroupVersionKind(ClusterCuratorGVK)

	err := r.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: clusterName}, curator)
	if err != nil {
		if errors.IsNotFound(err) {
			// No ClusterCurator - check if cluster should be skipped due to version
			if targetVersion != "" {
				shouldSkip, _ := r.shouldSkipClusterUpgrade(ctx, clusterName, targetVersion)
				if shouldSkip {
					return rolloutv1alpha1.ClusterUpgradeStateSkipped
				}
			}
			// No ClusterCurator and not skipped means pending (not started)
			return rolloutv1alpha1.ClusterUpgradeStatePending
		}
		// Error fetching - treat as pending
		return rolloutv1alpha1.ClusterUpgradeStatePending
	}

	return r.evaluateClusterCuratorState(ctx, curator, clusterName, targetVersion)
}

// evaluateClusterCuratorState evaluates the upgrade state from a ClusterCurator object
func (r *ClusterCuratorRolloutReconciler) evaluateClusterCuratorState(
	ctx context.Context,
	curator *unstructured.Unstructured,
	clusterName string,
	targetVersion string,
) rolloutv1alpha1.ClusterUpgradeState {

	// Check the ClusterCurator status
	// The ClusterCurator has conditions that indicate the upgrade state
	// Key condition: "clustercurator-job" with status and message

	spec, _, _ := unstructured.NestedMap(curator.Object, "spec")
	if spec == nil {
		return rolloutv1alpha1.ClusterUpgradeStatePending
	}

	// Check if desiredCuration is set to "upgrade"
	desiredCuration, _, _ := unstructured.NestedString(spec, "desiredCuration")
	if desiredCuration != "upgrade" {
		// Not configured for upgrade yet
		return rolloutv1alpha1.ClusterUpgradeStatePending
	}

	// Check if the ClusterCurator is configured for our target version
	// If it's configured for a different version, check if cluster should be skipped or is pending
	if targetVersion != "" {
		upgrade, _, _ := unstructured.NestedMap(spec, "upgrade")
		if upgrade != nil {
			desiredUpdate, _, _ := unstructured.NestedString(upgrade, "desiredUpdate")
			if desiredUpdate != "" && desiredUpdate != targetVersion {
				// ClusterCurator exists but is for a different version
				// Before treating as pending, check if cluster is already at the target version
				// (e.g., upgraded through another mechanism or the version was bumped externally)
				shouldSkip, _ := r.shouldSkipClusterUpgrade(ctx, clusterName, targetVersion)
				if shouldSkip {
					return rolloutv1alpha1.ClusterUpgradeStateSkipped
				}
				// Cluster is not at target version - treat as pending
				// The controller will patch ClusterCurator to the new target version
				return rolloutv1alpha1.ClusterUpgradeStatePending
			}
		}
	}

	// Check status conditions
	status, _, _ := unstructured.NestedMap(curator.Object, "status")
	if status == nil {
		// Has upgrade spec but no status yet - in progress
		return rolloutv1alpha1.ClusterUpgradeStateInProgress
	}

	conditions, _, _ := unstructured.NestedSlice(status, "conditions")
	if conditions == nil {
		return rolloutv1alpha1.ClusterUpgradeStateInProgress
	}

	// Look for the clustercurator-job condition
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		condType, _, _ := unstructured.NestedString(cond, "type")
		if condType != "clustercurator-job" {
			continue
		}

		condStatus, _, _ := unstructured.NestedString(cond, "status")
		message, _, _ := unstructured.NestedString(cond, "message")

		if condStatus == "True" {
			// Job completed - check if it was successful
			if strings.Contains(strings.ToLower(message), "failed") ||
				strings.Contains(strings.ToLower(message), "error") {
				return rolloutv1alpha1.ClusterUpgradeStateFailed
			}
			// ClusterCurator shows completion, but we must verify the actual cluster version
			// to avoid trusting stale completion conditions from a previous upgrade.
			// This is critical when the target version changes - the old completion status
			// is still present even though the spec was patched to a new version.
			if targetVersion != "" {
				shouldSkip, err := r.shouldSkipClusterUpgrade(ctx, clusterName, targetVersion)
				if err != nil {
					// Error checking version - assume in progress to be safe
					return rolloutv1alpha1.ClusterUpgradeStateInProgress
				}
				if shouldSkip {
					// Cluster is at or above target version - truly completed
					return rolloutv1alpha1.ClusterUpgradeStateCompleted
				}
				// Cluster is NOT at target version despite ClusterCurator showing completion
				// This means the completion is stale (from a previous upgrade)
				// Treat as Pending - the new upgrade hasn't started yet, the curator controller
				// will process the updated spec and start a fresh upgrade
				return rolloutv1alpha1.ClusterUpgradeStatePending
			}
			// No target version to verify against - trust the completion status
			return rolloutv1alpha1.ClusterUpgradeStateCompleted
		} else if condStatus == "False" {
			// Job in progress or failed
			reason, _, _ := unstructured.NestedString(cond, "reason")
			if strings.Contains(strings.ToLower(reason), "failed") ||
				strings.Contains(strings.ToLower(message), "failed") {
				return rolloutv1alpha1.ClusterUpgradeStateFailed
			}
			// Still in progress
			return rolloutv1alpha1.ClusterUpgradeStateInProgress
		}
	}

	// If we have upgrade spec and status but unclear conditions, assume in progress
	return rolloutv1alpha1.ClusterUpgradeStateInProgress
}

// buildRolloutPlan creates an ordered list of rollout steps from placement infos
// The order is determined by the collection order (which follows the spec hierarchy)
func (r *ClusterCuratorRolloutReconciler) buildRolloutPlan(
	placementInfos []placementInfo,
	existingPlan []rolloutv1alpha1.RolloutStep,
) []rolloutv1alpha1.RolloutStep {
	// Build a map of existing steps to preserve state
	existingStepMap := make(map[string]*rolloutv1alpha1.RolloutStep)
	for i := range existingPlan {
		existingStepMap[existingPlan[i].PlacementName] = &existingPlan[i]
	}

	plan := make([]rolloutv1alpha1.RolloutStep, len(placementInfos))
	for i, info := range placementInfos {
		step := rolloutv1alpha1.RolloutStep{
			Index:         i,
			PlacementName: info.name,
			Path:          strings.Join(info.parentPath, "/"),
			State:         rolloutv1alpha1.RolloutStepStatePending,
			SoakDuration:  info.soakDuration,
			Concurrency:   info.concurrency,
		}

		// Preserve existing state if step already exists
		if existing, ok := existingStepMap[info.name]; ok {
			step.State = existing.State
			step.Counts = existing.Counts
			step.StartedAt = existing.StartedAt
			step.CompletedAt = existing.CompletedAt
			step.SoakEndsAt = existing.SoakEndsAt
			// Preserve batch tracking state
			step.CurrentBatch = existing.CurrentBatch
			step.TotalBatches = existing.TotalBatches
			step.BatchClusters = existing.BatchClusters
			step.BatchSoakEndsAt = existing.BatchSoakEndsAt
		}

		plan[i] = step
	}

	return plan
}

// reconcileRolloutProgression manages the progression through the rollout plan
// It activates the next step when the current step completes (and soak ends)
// It also handles batch-level soaking when concurrency.soakDuration is configured
func (r *ClusterCuratorRolloutReconciler) reconcileRolloutProgression(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(rollout.Status.RolloutPlan) == 0 {
		logger.Info("No rollout plan, nothing to progress")
		return ctrl.Result{}, nil
	}

	logger.Info("Rollout plan status",
		"stepCount", len(rollout.Status.RolloutPlan),
		"phase", rollout.Status.Phase)

	// Check if rollout is paused - but first check if failures have been resolved
	// (e.g., user deleted failed ClusterCurators to trigger retry)
	if rollout.Status.Phase == rolloutv1alpha1.RolloutPhasePaused {
		// Refresh failure counts by checking actual ClusterCurator states
		currentFailures := r.countCurrentFailures(ctx, rollout)
		if currentFailures == 0 {
			// All failures resolved (ClusterCurators deleted or succeeded) - auto-resume
			logger.Info("Paused rollout resuming - all failed ClusterCurators have been deleted or resolved")
			rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseProcessing
			rollout.Status.Message = "Rollout resumed after failed clusters were reset"

			// Reset Failed step states back to Active so processing can continue
			// Also reset the failure counts and failed clusters list since the failures are resolved
			for i := range rollout.Status.RolloutPlan {
				step := &rollout.Status.RolloutPlan[i]
				if step.State == rolloutv1alpha1.RolloutStepStateFailed {
					step.State = rolloutv1alpha1.RolloutStepStateActive
					// Clear stale failure data - the deleted curators are no longer failed
					step.Counts.Failed = 0
					step.FailedClusters = nil
					logger.Info("Resetting step state from Failed to Active", "step", step.PlacementName)
				}
			}

			// Refresh counts for all steps to get actual current cluster states
			// This is critical because the clusters with deleted curators are now Pending again
			r.aggregateTotalCountsFromSteps(ctx, rollout)

			// Continue with normal processing below
		} else {
			logger.Info("Rollout is paused due to failure, waiting for manual intervention",
				"currentFailures", currentFailures)
			return ctrl.Result{RequeueAfter: RequeueAfterIdle}, nil
		}
	}

	// Check for failures and handle according to OnFailure policy
	result, shouldReturn := r.handleFailures(ctx, rollout)
	if shouldReturn {
		return result, nil
	}

	// Find the current active step or determine the next one to activate
	var activeStepIndex *int
	var nextPendingIndex *int

	for i := range rollout.Status.RolloutPlan {
		step := &rollout.Status.RolloutPlan[i]
		logger.V(1).Info("Checking step",
			"index", i,
			"placement", step.PlacementName,
			"state", step.State,
			"total", step.Counts.Total,
			"pending", step.Counts.Pending)
		switch step.State {
		case rolloutv1alpha1.RolloutStepStateActive:
			// Use the FIRST active step by index order to maintain rollout order
			if activeStepIndex == nil {
				activeStepIndex = &i
			}
		case rolloutv1alpha1.RolloutStepStateBatchSoaking:
			// Check if batch soak has ended
			if step.BatchSoakEndsAt != nil && time.Now().After(step.BatchSoakEndsAt.Time) {
				// Batch soak ended - advance to next batch
				step.CurrentBatch++
				step.BatchSoakEndsAt = nil
				step.BatchClusters = nil
				step.State = rolloutv1alpha1.RolloutStepStateActive
				logger.Info("Batch soak period ended, advancing to next batch",
					"step", step.PlacementName,
					"newBatch", step.CurrentBatch,
					"totalBatches", step.TotalBatches)
				// Use the FIRST active step by index order to maintain rollout order
				if activeStepIndex == nil {
					activeStepIndex = &i
				}
			} else {
				// Still in batch soak, this is the "active" step
				// Use the FIRST active step by index order to maintain rollout order
				if activeStepIndex == nil {
					activeStepIndex = &i
				}
			}
		case rolloutv1alpha1.RolloutStepStateSoaking:
			// Check if step-level soak has ended
			if step.SoakEndsAt != nil && time.Now().After(step.SoakEndsAt.Time) {
				// Refresh step counts to detect any failures that occurred during soak period
				r.updateStepCountsFromClusters(ctx, rollout, step)

				// Check for failures before marking complete (for onFailure: continue)
				if step.Counts.Failed > 0 {
					step.State = rolloutv1alpha1.RolloutStepStateFailed
					step.FailedClusters = r.getFailedClusterNamesForStep(ctx, rollout, step)
					logger.Info("Soak period ended, marking step failed due to cluster failures",
						"step", step.PlacementName, "failedCount", step.Counts.Failed)
				} else {
					step.State = rolloutv1alpha1.RolloutStepStateCompleted
					logger.Info("Soak period ended, marking step completed", "step", step.PlacementName)
				}
			} else {
				// Still soaking, this is the "active" step
				// Use the FIRST active step by index order to maintain rollout order
				if activeStepIndex == nil {
					activeStepIndex = &i
				}
			}
		case rolloutv1alpha1.RolloutStepStatePending:
			if nextPendingIndex == nil {
				nextPendingIndex = &i
			}
		}
	}

	logger.Info("Step analysis complete",
		"activeStepIndex", activeStepIndex,
		"nextPendingIndex", nextPendingIndex)

	// If there's an active step, ensure ClusterCurators exist and check if it's complete
	if activeStepIndex != nil {
		step := &rollout.Status.RolloutPlan[*activeStepIndex]
		logger.Info("Processing active step",
			"stepIndex", *activeStepIndex,
			"stepState", step.State,
			"expectedState", rolloutv1alpha1.RolloutStepStateActive,
			"statesMatch", step.State == rolloutv1alpha1.RolloutStepStateActive)

		if step.State == rolloutv1alpha1.RolloutStepStateActive {
			// Ensure ClusterCurators exist for clusters in the active step (or current batch)
			logger.Info("About to ensure ClusterCurators", "step", step.PlacementName)
			if err := r.ensureClusterCuratorsForStep(ctx, rollout, step); err != nil {
				logger.Error(err, "Failed to ensure ClusterCurators for active step", "step", step.PlacementName)
				// Continue - we'll retry on next reconcile
			}

			// Check completion - different logic for batch mode vs regular mode
			if r.hasConcurrencySoakDuration(step) {
				// Batch mode: check if current batch is complete
				if r.isBatchComplete(ctx, step, rollout.Spec.OpenShiftVersion) {
					now := metav1.Now()

					// Mark the cluster upgrades as complete by updating step counts before applying soak
					r.updateStepCountsFromClusters(ctx, rollout, step)

					// Check if this was the last batch
					isLastBatch := step.CurrentBatch >= step.TotalBatches

					if isLastBatch {
						// All batches complete - handle step-level soak or completion
						step.CompletedAt = &now
						// Clear batch tracking - all batches are done
						step.BatchClusters = nil

						if step.SoakDuration != nil && step.SoakDuration.Duration > 0 {
							step.State = rolloutv1alpha1.RolloutStepStateSoaking
							soakEnds := metav1.NewTime(now.Add(step.SoakDuration.Duration))
							step.SoakEndsAt = &soakEnds
							logger.Info("All batches completed, starting step-level soak period",
								"step", step.PlacementName,
								"soakDuration", step.SoakDuration.Duration,
								"soakEndsAt", soakEnds)

							r.startSoak(rollout, step.Path, step.SoakDuration.Duration)
							return ctrl.Result{RequeueAfter: step.SoakDuration.Duration + time.Second}, nil
						}

						// No step-level soak - check for failures (for onFailure: continue)
						if step.Counts.Failed > 0 {
							step.State = rolloutv1alpha1.RolloutStepStateFailed
							step.FailedClusters = r.getFailedClusterNamesForStep(ctx, rollout, step)
							logger.Info("All batches completed with failures (no step-level soak)",
								"step", step.PlacementName, "failedCount", step.Counts.Failed)
						} else {
							step.State = rolloutv1alpha1.RolloutStepStateCompleted
							logger.Info("All batches completed (no step-level soak)", "step", step.PlacementName)
						}
						activeStepIndex = nil
					} else {
						// Start batch soak before next batch
						batchSoakDuration := step.Concurrency.SoakDuration.Duration
						step.State = rolloutv1alpha1.RolloutStepStateBatchSoaking
						batchSoakEnds := metav1.NewTime(now.Add(batchSoakDuration))
						step.BatchSoakEndsAt = &batchSoakEnds
						// Clear batchClusters during soak - the batch is complete, not processing
						step.BatchClusters = nil

						logger.Info("Batch completed, starting batch soak period",
							"step", step.PlacementName,
							"completedBatch", step.CurrentBatch,
							"totalBatches", step.TotalBatches,
							"batchSoakDuration", batchSoakDuration,
							"batchSoakEndsAt", batchSoakEnds)

						// Update soak status for visibility
						batchPath := fmt.Sprintf("%s/batch-%d", step.Path, step.CurrentBatch)
						r.startSoak(rollout, batchPath, batchSoakDuration)

						return ctrl.Result{RequeueAfter: batchSoakDuration + time.Second}, nil
					}
				}
			} else {
				// Regular mode: update step counts from actual cluster states
				r.updateStepCountsFromClusters(ctx, rollout, step)

				// Check if all clusters in step are done
				if r.isStepComplete(step) {
					now := metav1.Now()
					step.CompletedAt = &now

					// Start soak if configured
					if step.SoakDuration != nil && step.SoakDuration.Duration > 0 {
						step.State = rolloutv1alpha1.RolloutStepStateSoaking
						soakEnds := metav1.NewTime(now.Add(step.SoakDuration.Duration))
						step.SoakEndsAt = &soakEnds
						logger.Info("Step completed, starting soak period",
							"step", step.PlacementName,
							"soakDuration", step.SoakDuration.Duration,
							"soakEndsAt", soakEnds)

						r.startSoak(rollout, step.Path, step.SoakDuration.Duration)
						return ctrl.Result{RequeueAfter: step.SoakDuration.Duration + time.Second}, nil
					}

					// No soak configured - check for failures (for onFailure: continue)
					if step.Counts.Failed > 0 {
						step.State = rolloutv1alpha1.RolloutStepStateFailed
						step.FailedClusters = r.getFailedClusterNamesForStep(ctx, rollout, step)
						logger.Info("Step completed with failures (no soak)", "step", step.PlacementName,
							"failedCount", step.Counts.Failed)
					} else {
						step.State = rolloutv1alpha1.RolloutStepStateCompleted
						logger.Info("Step completed (no soak)", "step", step.PlacementName)
					}
					activeStepIndex = nil
				}
			}
		}
	}

	// FIRST: Check for terminal steps (Completed/Failed) that have pending clusters.
	// This must happen BEFORE activating the next pending step to ensure we resume
	// earlier steps that need retrying before moving forward.
	if resumed := r.resumeTerminalStepsWithPendingClusters(ctx, rollout); resumed {
		// A terminal step was resumed - it's now Active.
		// Re-find the active step index since we just activated one.
		for i := range rollout.Status.RolloutPlan {
			if rollout.Status.RolloutPlan[i].State == rolloutv1alpha1.RolloutStepStateActive {
				activeStepIndex = &i
				break
			}
		}
	}

	// If no active step and there's a pending step, activate it
	if activeStepIndex == nil && nextPendingIndex != nil {
		step := &rollout.Status.RolloutPlan[*nextPendingIndex]
		now := metav1.Now()
		step.State = rolloutv1alpha1.RolloutStepStateActive
		step.StartedAt = &now
		step.CurrentBatch = 1 // Initialize batch tracking (1-indexed)
		step.BatchClusters = nil
		step.BatchSoakEndsAt = nil
		rollout.Status.CurrentStepIndex = nextPendingIndex
		logger.Info("Activating next step", "step", step.PlacementName, "index", *nextPendingIndex)

		// Initialize step counts from actual cluster states
		r.updateStepCountsFromClusters(ctx, rollout, step)

		// Create/patch ClusterCurators for clusters in this step (or first batch)
		if err := r.ensureClusterCuratorsForStep(ctx, rollout, step); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to ensure ClusterCurators for step %s: %w", step.PlacementName, err)
		}
	}

	// Check if all steps are complete (Completed or Failed are both terminal states)
	allComplete := true
	hasFailures := false
	var activeStep *rolloutv1alpha1.RolloutStep
	for i := range rollout.Status.RolloutPlan {
		step := &rollout.Status.RolloutPlan[i]
		// Failed is also a terminal state - the step is done, just with failures
		if step.State != rolloutv1alpha1.RolloutStepStateCompleted &&
			step.State != rolloutv1alpha1.RolloutStepStateFailed {
			allComplete = false
		}
		if step.State == rolloutv1alpha1.RolloutStepStateFailed {
			hasFailures = true
		}
		if step.State == rolloutv1alpha1.RolloutStepStateActive ||
			step.State == rolloutv1alpha1.RolloutStepStateBatchSoaking {
			activeStep = step
		}
	}

	if allComplete {
		rollout.Status.CurrentStepIndex = nil
		if hasFailures {
			rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseCompletedWithFailures
			rollout.Status.Message = "Rollout completed with some cluster failures"
			logger.Info("All rollout steps completed with failures")
		} else {
			rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseIdle
			rollout.Status.Message = "Rollout completed successfully"
			logger.Info("All rollout steps completed")
		}
		return ctrl.Result{RequeueAfter: RequeueAfterIdle}, nil
	}

	// Set phase based on active step state
	if activeStep != nil {
		// Check if we're in a soak period (batch soak or step soak)
		if activeStep.State == rolloutv1alpha1.RolloutStepStateBatchSoaking {
			// Batch soak - set phase to Soaking
			rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseSoaking
			rollout.Status.Message = fmt.Sprintf("Soaking after batch %d/%d in step %s",
				activeStep.CurrentBatch,
				activeStep.TotalBatches,
				activeStep.PlacementName)
		} else if activeStep.State == rolloutv1alpha1.RolloutStepStateSoaking {
			// Step-level soak - set phase to Soaking
			rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseSoaking
			rollout.Status.Message = fmt.Sprintf("Soaking after completing step %s",
				activeStep.PlacementName)
		} else {
			// Actively processing - set phase to Processing
			rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseProcessing
			if r.hasConcurrencySoakDuration(activeStep) {
				rollout.Status.Message = fmt.Sprintf("Processing step %s batch %d/%d (%d/%d clusters complete)",
					activeStep.PlacementName,
					activeStep.CurrentBatch,
					activeStep.TotalBatches,
					activeStep.Counts.Completed+activeStep.Counts.Skipped+activeStep.Counts.Failed,
					activeStep.Counts.Total)
			} else {
				rollout.Status.Message = fmt.Sprintf("Processing step %s (%d/%d clusters complete)",
					activeStep.PlacementName,
					activeStep.Counts.Completed+activeStep.Counts.Skipped+activeStep.Counts.Failed,
					activeStep.Counts.Total)
			}
		}
	}

	return ctrl.Result{RequeueAfter: RequeueAfterProcessing}, nil
}

// isStepComplete checks if all clusters in a step have completed their upgrades
func (r *ClusterCuratorRolloutReconciler) isStepComplete(step *rolloutv1alpha1.RolloutStep) bool {
	// A step is complete when:
	// 1. There are clusters to process (Total > 0)
	// 2. All clusters are either Completed, Skipped, or Failed (none Pending or InProgress)
	// 3. The counts are consistent (sum of states equals Total)
	//
	// If Total is 0, the step is NOT complete - we're still waiting for PlacementDecisions
	// to populate the cluster count, or there are genuinely no clusters matching the placement.
	if step.Counts.Total == 0 {
		return false
	}

	// Validate that counts are consistent before marking complete
	// This prevents premature completion due to count calculation issues
	countSum := step.Counts.Pending + step.Counts.InProgress + step.Counts.Completed + step.Counts.Failed + step.Counts.Skipped
	if countSum != step.Counts.Total {
		// Counts are inconsistent - don't mark as complete yet
		// This can happen if counts weren't properly calculated
		return false
	}

	return step.Counts.Pending == 0 && step.Counts.InProgress == 0
}

// resumeTerminalStepsWithPendingClusters checks terminal steps (Completed/Failed) for any remaining
// pending clusters and reactivates them. This provides resilience against:
// - Steps that were prematurely marked complete due to count calculation issues
// - Clusters that were added to a placement after the step completed
// - Failed ClusterCurators that were deleted to trigger a retry
// - Race conditions or other edge cases that caused clusters to be missed
// Returns true if a step was resumed (caller should update activeStepIndex).
//
// IMPORTANT: This function respects the rollout order. It only resumes terminal steps
// that come BEFORE any Active, Pending, Soaking, or BatchSoaking steps. This ensures
// that when a new rollout starts (e.g., target version changes), steps are processed
// in order rather than all at once.
func (r *ClusterCuratorRolloutReconciler) resumeTerminalStepsWithPendingClusters(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
) bool {
	logger := log.FromContext(ctx)

	// Helper to check if a step is in a terminal state (Completed or Failed)
	isTerminalState := func(state rolloutv1alpha1.RolloutStepState) bool {
		return state == rolloutv1alpha1.RolloutStepStateCompleted ||
			state == rolloutv1alpha1.RolloutStepStateFailed
	}

	// Find the index of the first non-terminal step (Active, Pending, Soaking, etc.)
	// We should only resume terminal steps that come BEFORE this index
	firstNonTerminalIndex := -1
	for i := range rollout.Status.RolloutPlan {
		step := &rollout.Status.RolloutPlan[i]
		if !isTerminalState(step.State) {
			firstNonTerminalIndex = i
			break
		}
	}

	// If all steps are in terminal state, we need to check if any have pending clusters
	// due to a target version change or deleted ClusterCurators. In this case, we should
	// reset the first step with pending clusters to Active to restart the rollout in order.
	if firstNonTerminalIndex == -1 {
		// Check if any terminal step has pending clusters (indicates new rollout or retry needed)
		for i := range rollout.Status.RolloutPlan {
			step := &rollout.Status.RolloutPlan[i]
			r.updateStepCountsFromClusters(ctx, rollout, step)
			if step.Counts.Pending > 0 {
				// Reset this step to Active to restart the rollout
				logger.Info("Resetting terminal step to Active due to pending clusters (new rollout or retry)",
					"step", step.PlacementName,
					"path", step.Path,
					"previousState", step.State,
					"pending", step.Counts.Pending,
					"total", step.Counts.Total)
				now := metav1.Now()
				step.State = rolloutv1alpha1.RolloutStepStateActive
				step.StartedAt = &now
				step.CompletedAt = nil
				step.SoakEndsAt = nil
				step.CurrentBatch = 1
				step.BatchClusters = nil
				step.BatchSoakEndsAt = nil
				step.FailedClusters = nil

				// Reset all steps AFTER this one back to Pending to maintain order
				for j := i + 1; j < len(rollout.Status.RolloutPlan); j++ {
					laterStep := &rollout.Status.RolloutPlan[j]
					if laterStep.State != rolloutv1alpha1.RolloutStepStatePending {
						logger.Info("Resetting later step to Pending due to earlier step resumption",
							"step", laterStep.PlacementName,
							"path", laterStep.Path,
							"previousState", laterStep.State)
						laterStep.State = rolloutv1alpha1.RolloutStepStatePending
						laterStep.StartedAt = nil
						laterStep.CompletedAt = nil
						laterStep.SoakEndsAt = nil
						laterStep.CurrentBatch = 0
						laterStep.BatchClusters = nil
						laterStep.BatchSoakEndsAt = nil
					}
				}

				// Create ClusterCurators for the pending clusters
				if err := r.ensureClusterCuratorsForStep(ctx, rollout, step); err != nil {
					logger.Error(err, "Failed to ensure ClusterCurators for resumed step",
						"step", step.PlacementName)
				}

				// Only resume one step at a time - the earliest one with pending clusters
				return true
			}
		}
		return false
	}

	// Check terminal steps that come BEFORE the first non-terminal step for pending clusters.
	// If any are found, we need to reset those steps to Active and stop the current active step.
	// This ensures we don't skip ahead when failed clusters need to be retried.
	for i := 0; i < firstNonTerminalIndex; i++ {
		step := &rollout.Status.RolloutPlan[i]

		// Only check terminal steps
		if !isTerminalState(step.State) {
			continue
		}

		// Refresh counts for this terminal step
		r.updateStepCountsFromClusters(ctx, rollout, step)

		// If there are pending clusters, we need to reset this step to Active
		// and pause the later steps to maintain rollout order
		if step.Counts.Pending > 0 {
			logger.Info("Found pending clusters in terminal step, resetting to Active to resume processing",
				"step", step.PlacementName,
				"path", step.Path,
				"previousState", step.State,
				"pending", step.Counts.Pending,
				"total", step.Counts.Total)

			// Reset this step to Active
			now := metav1.Now()
			step.State = rolloutv1alpha1.RolloutStepStateActive
			step.StartedAt = &now
			step.CompletedAt = nil
			step.SoakEndsAt = nil
			step.CurrentBatch = 1
			step.BatchClusters = nil
			step.BatchSoakEndsAt = nil
			step.FailedClusters = nil

			// Reset all steps AFTER this one back to Pending to maintain order
			for j := i + 1; j < len(rollout.Status.RolloutPlan); j++ {
				laterStep := &rollout.Status.RolloutPlan[j]
				if laterStep.State != rolloutv1alpha1.RolloutStepStatePending {
					logger.Info("Resetting later step to Pending due to earlier step resumption",
						"step", laterStep.PlacementName,
						"path", laterStep.Path,
						"previousState", laterStep.State)
					laterStep.State = rolloutv1alpha1.RolloutStepStatePending
					laterStep.StartedAt = nil
					laterStep.CompletedAt = nil
					laterStep.SoakEndsAt = nil
					laterStep.CurrentBatch = 0
					laterStep.BatchClusters = nil
					laterStep.BatchSoakEndsAt = nil
				}
			}

			// Create ClusterCurators for the pending clusters
			if err := r.ensureClusterCuratorsForStep(ctx, rollout, step); err != nil {
				logger.Error(err, "Failed to ensure ClusterCurators for resumed step",
					"step", step.PlacementName)
			}

			// Only resume one step at a time - the earliest one with pending clusters
			return true
		}
	}
	return false
}

// updateStepCountsFromClusters refreshes the step's counts by querying the current state
// of all clusters in the step's placement. This ensures counts are accurate before
// applying batch soak or completing the step.
func (r *ClusterCuratorRolloutReconciler) updateStepCountsFromClusters(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	step *rolloutv1alpha1.RolloutStep,
) {
	logger := log.FromContext(ctx)

	// Get all clusters from the placement
	clusters, err := r.getClustersFromPlacement(ctx, rollout.Namespace, step.PlacementName)
	if err != nil {
		logger.Error(err, "Failed to get clusters from placement for count update", "placement", step.PlacementName)
		return
	}

	// Get fresh counts by querying ClusterCurator states
	counts := r.getClusterUpgradeCounts(ctx, clusters, rollout.Spec.OpenShiftVersion)
	step.Counts = counts

	logger.V(1).Info("Updated step counts from clusters",
		"step", step.PlacementName,
		"total", counts.Total,
		"completed", counts.Completed,
		"inProgress", counts.InProgress,
		"pending", counts.Pending,
		"failed", counts.Failed)
}

// updateStepCountsFromClustersWithCache updates the counts for a step by querying the actual
// ClusterCurator states using a pre-built cache for O(1) lookups.
// This is the performance-optimized version that avoids individual API calls per cluster.
func (r *ClusterCuratorRolloutReconciler) updateStepCountsFromClustersWithCache(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	step *rolloutv1alpha1.RolloutStep,
	cache clusterCuratorCache,
) {
	logger := log.FromContext(ctx)

	// Get all clusters from the placement
	clusters, err := r.getClustersFromPlacement(ctx, rollout.Namespace, step.PlacementName)
	if err != nil {
		logger.Error(err, "Failed to get clusters from placement for count update", "placement", step.PlacementName)
		return
	}

	// Get fresh counts using the cache for O(1) lookups
	counts := r.getClusterUpgradeCountsWithCache(ctx, clusters, rollout.Spec.OpenShiftVersion, cache)
	step.Counts = counts

	logger.V(1).Info("Updated step counts from clusters (cached)",
		"step", step.PlacementName,
		"total", counts.Total,
		"completed", counts.Completed,
		"inProgress", counts.InProgress,
		"pending", counts.Pending,
		"failed", counts.Failed)
}

// calculateBatchSize calculates the batch size based on concurrency settings
func (r *ClusterCuratorRolloutReconciler) calculateBatchSize(concurrency *rolloutv1alpha1.ConcurrencySpec, totalClusters int) int {
	if concurrency == nil || totalClusters == 0 {
		// No concurrency settings means process all at once
		return totalClusters
	}

	switch concurrency.Type {
	case rolloutv1alpha1.ConcurrencyTypeCount:
		if concurrency.Value <= 0 {
			return totalClusters
		}
		return concurrency.Value
	case rolloutv1alpha1.ConcurrencyTypePercent:
		if concurrency.Value <= 0 {
			return totalClusters
		}
		if concurrency.Value >= 100 {
			return totalClusters
		}
		batchSize := (totalClusters * concurrency.Value) / 100
		if batchSize < 1 {
			batchSize = 1 // At least 1 cluster per batch
		}
		return batchSize
	default:
		return totalClusters
	}
}

// hasConcurrencySoakDuration checks if concurrency.soakDuration is configured
func (r *ClusterCuratorRolloutReconciler) hasConcurrencySoakDuration(step *rolloutv1alpha1.RolloutStep) bool {
	return step.Concurrency != nil &&
		step.Concurrency.SoakDuration != nil &&
		step.Concurrency.SoakDuration.Duration > 0
}

// isBatchComplete checks if all clusters in the current batch have completed their upgrades
// targetVersion is used to detect clusters that should be skipped (already at target version)
func (r *ClusterCuratorRolloutReconciler) isBatchComplete(ctx context.Context, step *rolloutv1alpha1.RolloutStep, targetVersion string) bool {
	if len(step.BatchClusters) == 0 {
		return false
	}

	// Check the upgrade state of each cluster in the batch
	for _, clusterName := range step.BatchClusters {
		state := r.getClusterCuratorState(ctx, clusterName, targetVersion)
		// If any cluster is still pending or in progress, batch is not complete
		// Note: Skipped clusters are considered "complete" for batch purposes
		if state == rolloutv1alpha1.ClusterUpgradeStatePending ||
			state == rolloutv1alpha1.ClusterUpgradeStateInProgress {
			return false
		}
	}
	return true
}

// getBatchClusters returns the clusters for a specific batch number (1-indexed)
// batchNumber is 1-indexed (1 = first batch, 2 = second batch, etc.)
func (r *ClusterCuratorRolloutReconciler) getBatchClusters(allClusters []string, batchNumber, batchSize int) []string {
	if len(allClusters) == 0 || batchSize <= 0 || batchNumber < 1 {
		return nil
	}

	// Convert 1-indexed batch number to 0-indexed for array access
	batchIndex := batchNumber - 1
	startIdx := batchIndex * batchSize
	if startIdx >= len(allClusters) {
		return nil
	}

	endIdx := startIdx + batchSize
	if endIdx > len(allClusters) {
		endIdx = len(allClusters)
	}

	return allClusters[startIdx:endIdx]
}

// calculateTotalBatches calculates the total number of batches for a step
func (r *ClusterCuratorRolloutReconciler) calculateTotalBatches(totalClusters, batchSize int) int {
	if batchSize <= 0 || totalClusters <= 0 {
		return 1
	}
	batches := totalClusters / batchSize
	if totalClusters%batchSize != 0 {
		batches++
	}
	return batches
}

// handleFailures checks for failed clusters and handles them according to the OnFailure policy
// Returns (result, shouldReturn) - if shouldReturn is true, the caller should return immediately
func (r *ClusterCuratorRolloutReconciler) handleFailures(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
) (ctrl.Result, bool) {
	logger := log.FromContext(ctx)

	// Count total failures across all steps
	totalFailed := 0
	for _, step := range rollout.Status.RolloutPlan {
		totalFailed += step.Counts.Failed
	}

	// Also check TotalCounts if available
	if rollout.Status.TotalCounts != nil && rollout.Status.TotalCounts.Failed > totalFailed {
		totalFailed = rollout.Status.TotalCounts.Failed
	}

	if totalFailed == 0 {
		// No failures, continue normally
		return ctrl.Result{}, false
	}

	// Get the OnFailure policy (default to "continue")
	onFailure := rollout.Spec.OnFailure
	if onFailure == "" {
		onFailure = rolloutv1alpha1.OnFailureContinue
	}

	switch onFailure {
	case rolloutv1alpha1.OnFailureContinue:
		// Continue with other clusters - don't stop progression
		// Failures are recorded but rollout continues
		logger.V(1).Info("Failures detected but onFailure=continue, proceeding", "failedCount", totalFailed)
		return ctrl.Result{}, false

	case rolloutv1alpha1.OnFailurePause:
		// Pause the rollout and wait for manual intervention
		rollout.Status.Phase = rolloutv1alpha1.RolloutPhasePaused
		rollout.Status.Message = fmt.Sprintf("Rollout paused due to %d failed cluster(s). Manual intervention required.", totalFailed)

		// Mark steps with failures as Failed state and collect failed cluster names
		for i := range rollout.Status.RolloutPlan {
			step := &rollout.Status.RolloutPlan[i]
			if step.Counts.Failed > 0 && step.State != rolloutv1alpha1.RolloutStepStateCompleted {
				step.State = rolloutv1alpha1.RolloutStepStateFailed
				step.FailedClusters = r.getFailedClusterNamesForStep(ctx, rollout, step)
				logger.Info("Marking step as failed", "step", step.PlacementName, "failedCount", step.Counts.Failed, "failedClusters", step.FailedClusters)
			}
		}

		logger.Info("Pausing rollout due to failures", "failedCount", totalFailed)
		return ctrl.Result{RequeueAfter: RequeueAfterIdle}, true

	default:
		// Unknown policy, treat as continue
		logger.Info("Unknown OnFailure policy, treating as continue", "policy", onFailure)
		return ctrl.Result{}, false
	}
}

// countCurrentFailures counts the current number of failed clusters by checking
// actual ClusterCurator states (not cached counts). This is used to detect when
// failed ClusterCurators have been deleted (to trigger retry) so the rollout can resume.
func (r *ClusterCuratorRolloutReconciler) countCurrentFailures(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
) int {
	logger := log.FromContext(ctx)
	failedCount := 0

	// Iterate through all steps and check actual ClusterCurator states
	for _, step := range rollout.Status.RolloutPlan {
		clusters, err := r.getClustersFromPlacement(ctx, rollout.Namespace, step.PlacementName)
		if err != nil {
			logger.V(1).Info("Failed to get clusters from placement for failure count",
				"placement", step.PlacementName, "error", err)
			continue
		}

		for _, clusterName := range clusters {
			state := r.getClusterCuratorState(ctx, clusterName, rollout.Spec.OpenShiftVersion)
			if state == rolloutv1alpha1.ClusterUpgradeStateFailed {
				failedCount++
			}
		}
	}

	return failedCount
}

// ensureClusterCuratorsForStep creates or patches ClusterCurators for clusters in a step
// Processing modes:
// 1. With concurrency.soakDuration: Batch mode - process batch, soak, next batch
// 2. With concurrency but no soakDuration: Rolling mode - keep N clusters in progress at all times
// 3. Without concurrency: Process all clusters at once
func (r *ClusterCuratorRolloutReconciler) ensureClusterCuratorsForStep(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	step *rolloutv1alpha1.RolloutStep,
) error {
	logger := log.FromContext(ctx)

	// Get all clusters from the placement decisions
	allClusters, err := r.getClustersFromPlacement(ctx, rollout.Namespace, step.PlacementName)
	if err != nil {
		return fmt.Errorf("failed to get clusters from placement: %w", err)
	}

	// If concurrency.soakDuration is configured, use batch-based processing
	if r.hasConcurrencySoakDuration(step) {
		return r.ensureClusterCuratorsForBatch(ctx, rollout, step, allClusters)
	}

	// If concurrency is configured (but no soakDuration), use rolling mode
	if step.Concurrency != nil {
		return r.ensureClusterCuratorsRolling(ctx, rollout, step, allClusters)
	}

	// No concurrency settings - process all clusters at once
	logger.Info("Ensuring ClusterCurators for step (no concurrency limit)", "step", step.PlacementName, "clusterCount", len(allClusters))

	for _, clusterName := range allClusters {
		if err := r.ensureClusterCurator(ctx, rollout, clusterName); err != nil {
			logger.Error(err, "Failed to ensure ClusterCurator", "cluster", clusterName)
			// Continue with other clusters, don't fail the whole step
		}
	}

	return nil
}

// ensureClusterCuratorsRolling implements a rolling upgrade pattern.
// It keeps a constant number of clusters upgrading at any time (based on concurrency setting).
// As clusters complete, new ones are started immediately (no soak between batches).
// This provides continuous progress while respecting the concurrency limit.
func (r *ClusterCuratorRolloutReconciler) ensureClusterCuratorsRolling(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	step *rolloutv1alpha1.RolloutStep,
	allClusters []string,
) error {
	logger := log.FromContext(ctx)

	// Calculate max concurrent upgrades
	maxConcurrent := r.calculateBatchSize(step.Concurrency, len(allClusters))

	// Count current in-progress upgrades and find pending clusters
	// Skipped clusters are not counted as pending (they're already at target version)
	inProgressCount := 0
	var pendingClusters []string

	for _, clusterName := range allClusters {
		state := r.getClusterCuratorState(ctx, clusterName, rollout.Spec.OpenShiftVersion)
		switch state {
		case rolloutv1alpha1.ClusterUpgradeStateInProgress:
			inProgressCount++
		case rolloutv1alpha1.ClusterUpgradeStatePending:
			pendingClusters = append(pendingClusters, clusterName)
			// ClusterUpgradeStateSkipped, Completed, Failed are not counted
		}
	}

	// Calculate how many new clusters we can start
	slotsAvailable := maxConcurrent - inProgressCount
	if slotsAvailable <= 0 {
		logger.V(1).Info("Rolling upgrade at capacity",
			"step", step.PlacementName,
			"inProgress", inProgressCount,
			"maxConcurrent", maxConcurrent)
		return nil
	}

	// Start upgrades for pending clusters up to available slots
	clustersToStart := slotsAvailable
	if clustersToStart > len(pendingClusters) {
		clustersToStart = len(pendingClusters)
	}

	logger.Info("Rolling upgrade - starting new clusters",
		"step", step.PlacementName,
		"inProgress", inProgressCount,
		"maxConcurrent", maxConcurrent,
		"slotsAvailable", slotsAvailable,
		"pendingCount", len(pendingClusters),
		"startingNow", clustersToStart)

	for i := 0; i < clustersToStart; i++ {
		clusterName := pendingClusters[i]
		if err := r.ensureClusterCurator(ctx, rollout, clusterName); err != nil {
			logger.Error(err, "Failed to ensure ClusterCurator", "cluster", clusterName)
			// Continue with other clusters
		}
	}

	return nil
}

// ensureClusterCuratorsForBatch creates ClusterCurators only for the current batch
// This is used when concurrency.soakDuration is configured
func (r *ClusterCuratorRolloutReconciler) ensureClusterCuratorsForBatch(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	step *rolloutv1alpha1.RolloutStep,
	allClusters []string,
) error {
	logger := log.FromContext(ctx)

	// Calculate batch size and total batches
	batchSize := r.calculateBatchSize(step.Concurrency, len(allClusters))
	totalBatches := r.calculateTotalBatches(len(allClusters), batchSize)

	// Update step metadata
	step.TotalBatches = totalBatches

	// Get clusters for the current batch
	batchClusters := r.getBatchClusters(allClusters, step.CurrentBatch, batchSize)
	step.BatchClusters = batchClusters

	logger.Info("Ensuring ClusterCurators for batch",
		"step", step.PlacementName,
		"batch", step.CurrentBatch,
		"totalBatches", totalBatches,
		"batchSize", len(batchClusters),
		"totalClusters", len(allClusters))

	// Create ClusterCurators only for clusters in the current batch
	for _, clusterName := range batchClusters {
		if err := r.ensureClusterCurator(ctx, rollout, clusterName); err != nil {
			logger.Error(err, "Failed to ensure ClusterCurator", "cluster", clusterName)
			// Continue with other clusters, don't fail the whole batch
		}
	}

	return nil
}

// getClustersFromPlacement retrieves the list of cluster names from a placement's decisions
func (r *ClusterCuratorRolloutReconciler) getClustersFromPlacement(
	ctx context.Context,
	namespace string,
	placementName string,
) ([]string, error) {
	decisions := &clusterv1beta1.PlacementDecisionList{}
	if err := r.List(ctx, decisions, client.InNamespace(namespace), client.MatchingLabels{
		clusterv1beta1.PlacementLabel: placementName,
	}); err != nil {
		return nil, err
	}

	var clusters []string
	for _, decision := range decisions.Items {
		for _, d := range decision.Status.Decisions {
			clusters = append(clusters, d.ClusterName)
		}
	}

	return clusters, nil
}

// shouldSkipClusterUpgrade checks if a cluster should be skipped for upgrade
// Returns true if the cluster's current version is >= the target version
func (r *ClusterCuratorRolloutReconciler) shouldSkipClusterUpgrade(
	ctx context.Context,
	clusterName string,
	targetVersion string,
) (bool, error) {
	logger := log.FromContext(ctx)

	// Get the ManagedCluster to check its current version
	mc := &clusterv1.ManagedCluster{}
	if err := r.Get(ctx, types.NamespacedName{Name: clusterName}, mc); err != nil {
		if errors.IsNotFound(err) {
			// Cluster doesn't exist, skip it
			logger.Info("ManagedCluster not found, skipping upgrade", "cluster", clusterName)
			return true, nil
		}
		return false, fmt.Errorf("failed to get ManagedCluster: %w", err)
	}

	// Get current version from labels
	currentVersion := mc.Labels["openshiftVersion"]
	if currentVersion == "" {
		// No version label, allow upgrade to proceed (controller will handle)
		logger.V(1).Info("No openshiftVersion label found on ManagedCluster, allowing upgrade",
			"cluster", clusterName)
		return false, nil
	}

	// Compare versions
	if compareVersions(currentVersion, targetVersion) >= 0 {
		logger.V(1).Info("Cluster version is already at or above target version, skipping upgrade",
			"cluster", clusterName,
			"currentVersion", currentVersion,
			"targetVersion", targetVersion)
		return true, nil
	}

	return false, nil
}

// compareVersions compares two version strings (e.g., "4.15.5" vs "4.16.0")
// Returns: -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func compareVersions(v1, v2 string) int {
	parts1 := strings.Split(v1, ".")
	parts2 := strings.Split(v2, ".")

	maxLen := len(parts1)
	if len(parts2) > maxLen {
		maxLen = len(parts2)
	}

	for i := 0; i < maxLen; i++ {
		var n1, n2 int

		if i < len(parts1) {
			fmt.Sscanf(parts1[i], "%d", &n1)
		}
		if i < len(parts2) {
			fmt.Sscanf(parts2[i], "%d", &n2)
		}

		if n1 < n2 {
			return -1
		}
		if n1 > n2 {
			return 1
		}
	}

	return 0
}

// ensureClusterCurator creates or patches a ClusterCurator for a specific cluster
func (r *ClusterCuratorRolloutReconciler) ensureClusterCurator(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	clusterName string,
) error {
	logger := log.FromContext(ctx)

	// Check if cluster should be skipped due to version
	shouldSkip, err := r.shouldSkipClusterUpgrade(ctx, clusterName, rollout.Spec.OpenShiftVersion)
	if err != nil {
		return err
	}
	if shouldSkip {
		logger.V(1).Info("Skipping cluster upgrade (version check)",
			"cluster", clusterName,
			"targetVersion", rollout.Spec.OpenShiftVersion)
		return nil
	}

	// ClusterCurator lives in the cluster's namespace (same name as cluster)
	curatorName := clusterName
	curatorNamespace := clusterName

	// Check if ClusterCurator already exists
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(ClusterCuratorGVK)
	err = r.Get(ctx, types.NamespacedName{Name: curatorName, Namespace: curatorNamespace}, existing)

	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to get ClusterCurator: %w", err)
	}

	if errors.IsNotFound(err) {
		// Create new ClusterCurator
		return r.createClusterCurator(ctx, rollout, clusterName)
	}

	// ClusterCurator exists - check if we need to patch it
	return r.patchClusterCurator(ctx, rollout, existing)
}

// createClusterCurator creates a new ClusterCurator for a cluster
func (r *ClusterCuratorRolloutReconciler) createClusterCurator(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	clusterName string,
) error {
	logger := log.FromContext(ctx)

	curator := &unstructured.Unstructured{}
	curator.SetGroupVersionKind(ClusterCuratorGVK)
	curator.SetName(clusterName)
	curator.SetNamespace(clusterName)

	// Set labels to track this curator is managed by this rollout
	curator.SetLabels(map[string]string{
		LabelRolloutManagedCurator: rollout.Name,
	})

	// Set the spec for upgrade
	spec := map[string]interface{}{
		"desiredCuration": "upgrade",
		"upgrade": map[string]interface{}{
			"desiredUpdate":  rollout.Spec.OpenShiftVersion,
			"monitorTimeout": int64(120), // 2 hours default
		},
	}
	curator.Object["spec"] = spec

	logger.Info("Creating ClusterCurator", "cluster", clusterName, "targetVersion", rollout.Spec.OpenShiftVersion)

	if err := r.Create(ctx, curator); err != nil {
		return fmt.Errorf("failed to create ClusterCurator: %w", err)
	}

	return nil
}

// patchClusterCurator patches an existing ClusterCurator to trigger an upgrade
func (r *ClusterCuratorRolloutReconciler) patchClusterCurator(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	existing *unstructured.Unstructured,
) error {
	logger := log.FromContext(ctx)

	// Check current state - don't patch if already upgrading to the target version
	spec, _, _ := unstructured.NestedMap(existing.Object, "spec")
	if spec != nil {
		desiredCuration, _, _ := unstructured.NestedString(spec, "desiredCuration")
		if desiredCuration == "upgrade" {
			upgrade, _, _ := unstructured.NestedMap(spec, "upgrade")
			if upgrade != nil {
				desiredUpdate, _, _ := unstructured.NestedString(upgrade, "desiredUpdate")
				if desiredUpdate == rollout.Spec.OpenShiftVersion {
					// Already configured for this upgrade
					logger.V(1).Info("ClusterCurator already configured for target version",
						"cluster", existing.GetName(),
						"targetVersion", rollout.Spec.OpenShiftVersion)
					return nil
				}
			}
		}
	}

	// Patch the ClusterCurator
	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(ClusterCuratorGVK)
	patch.SetName(existing.GetName())
	patch.SetNamespace(existing.GetNamespace())

	// Merge labels
	labels := existing.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[LabelRolloutManagedCurator] = rollout.Name
	patch.SetLabels(labels)

	// Set the upgrade spec
	patch.Object["spec"] = map[string]interface{}{
		"desiredCuration": "upgrade",
		"upgrade": map[string]interface{}{
			"desiredUpdate":  rollout.Spec.OpenShiftVersion,
			"monitorTimeout": int64(120),
		},
	}

	logger.Info("Patching ClusterCurator for upgrade",
		"cluster", existing.GetName(),
		"targetVersion", rollout.Spec.OpenShiftVersion)

	if err := r.Patch(ctx, patch, client.Merge); err != nil {
		return fmt.Errorf("failed to patch ClusterCurator: %w", err)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterCuratorRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Create an unstructured object for watching ClusterCurators
	clusterCurator := &unstructured.Unstructured{}
	clusterCurator.SetGroupVersionKind(ClusterCuratorGVK)

	return ctrl.NewControllerManagedBy(mgr).
		For(&rolloutv1alpha1.ClusterCuratorRollout{}).
		Owns(&clusterv1beta1.Placement{}).
		Watches(
			&clusterv1beta1.PlacementDecision{},
			handler.EnqueueRequestsFromMapFunc(r.mapPlacementDecisionToRollout),
		).
		Watches(
			&clusterv1.ManagedCluster{},
			handler.EnqueueRequestsFromMapFunc(r.mapManagedClusterToRollout),
		).
		Watches(
			clusterCurator,
			handler.EnqueueRequestsFromMapFunc(r.mapClusterCuratorToRollout),
		).
		Named("clustercuratorrollout").
		Complete(r)
}

// mapPlacementDecisionToRollout maps PlacementDecision changes to the owning ClusterCuratorRollout
func (r *ClusterCuratorRolloutReconciler) mapPlacementDecisionToRollout(ctx context.Context, obj client.Object) []reconcile.Request {
	decision := obj.(*clusterv1beta1.PlacementDecision)

	// Get the placement name from the label
	placementName, ok := decision.Labels[clusterv1beta1.PlacementLabel]
	if !ok {
		return nil
	}

	// Find the placement
	placement := &clusterv1beta1.Placement{}
	if err := r.Get(ctx, types.NamespacedName{Name: placementName, Namespace: decision.Namespace}, placement); err != nil {
		return nil
	}

	// Check if this placement is owned by a ClusterCuratorRollout
	rolloutName, ok := placement.Labels[LabelRolloutOwner]
	if !ok {
		return nil
	}

	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Name:      rolloutName,
			Namespace: decision.Namespace,
		},
	}}
}

// mapManagedClusterToRollout maps ManagedCluster changes to all ClusterCuratorRollouts
// This is needed to react to label changes on ManagedClusters
func (r *ClusterCuratorRolloutReconciler) mapManagedClusterToRollout(ctx context.Context, obj client.Object) []reconcile.Request {
	// List all ClusterCuratorRollouts
	rolloutList := &rolloutv1alpha1.ClusterCuratorRolloutList{}
	if err := r.List(ctx, rolloutList); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, rollout := range rolloutList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      rollout.Name,
				Namespace: rollout.Namespace,
			},
		})
	}

	return requests
}

// mapClusterCuratorToRollout maps ClusterCurator changes to the owning ClusterCuratorRollout
// This allows the controller to react immediately when a cluster's upgrade status changes
func (r *ClusterCuratorRolloutReconciler) mapClusterCuratorToRollout(ctx context.Context, obj client.Object) []reconcile.Request {
	// Check if this ClusterCurator is managed by a rollout
	labels := obj.GetLabels()
	if labels == nil {
		return nil
	}

	rolloutName, ok := labels[LabelRolloutManagedCurator]
	if !ok || rolloutName == "" {
		return nil
	}

	// ClusterCurator is in the cluster's namespace, but the rollout is typically in a different namespace
	// We need to find the rollout by name - list all rollouts and find the one with matching name
	rolloutList := &rolloutv1alpha1.ClusterCuratorRolloutList{}
	if err := r.List(ctx, rolloutList); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, rollout := range rolloutList.Items {
		if rollout.Name == rolloutName {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      rollout.Name,
					Namespace: rollout.Namespace,
				},
			})
		}
	}

	return requests
}
