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
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

	// RequeueAfterProcessing is the requeue time when processing
	RequeueAfterProcessing = 30 * time.Second

	// RequeueAfterIdle is the requeue time when idle
	RequeueAfterIdle = 5 * time.Minute
)

// ClusterCuratorRolloutReconciler reconciles a ClusterCuratorRollout object
type ClusterCuratorRolloutReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=rollout.open-cluster-management.io,resources=clustercuratorrollouts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rollout.open-cluster-management.io,resources=clustercuratorrollouts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rollout.open-cluster-management.io,resources=clustercuratorrollouts/finalizers,verbs=update
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=placements,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=placementdecisions,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclustersets,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclustersetbindings,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// placementInfo holds information about a placement to be created
type placementInfo struct {
	name       string
	groupKey   string
	groupValue string
	predicates []clusterv1beta1.ClusterPredicate
	parentPath []string
}

// Reconcile handles the reconciliation of ClusterCuratorRollout resources
func (r *ClusterCuratorRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling ClusterCuratorRollout")

	startTime := time.Now()

	// Fetch the ClusterCuratorRollout
	rollout := &rolloutv1alpha1.ClusterCuratorRollout{}
	if err := r.Get(ctx, req.NamespacedName, rollout); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("ClusterCuratorRollout not found, ignoring")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

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

	// Process the selection spec and generate placements
	result, err := r.reconcilePlacements(ctx, rollout)
	if err != nil {
		logger.Error(err, "Failed to reconcile placements")
		rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseError
		rollout.Status.LastReconcileError = err.Error()
		duration := metav1.Duration{Duration: time.Since(startTime)}
		rollout.Status.LastReconcileDuration = &duration
		if statusErr := r.Status().Update(ctx, rollout); statusErr != nil {
			logger.Error(statusErr, "Failed to update status")
		}
		return ctrl.Result{RequeueAfter: RequeueAfterProcessing}, err
	}

	// Update status with results
	rollout.Status.Phase = rolloutv1alpha1.RolloutPhaseIdle
	rollout.Status.ObservedGeneration = rollout.Generation
	duration := metav1.Duration{Duration: time.Since(startTime)}
	rollout.Status.LastReconcileDuration = &duration
	rollout.Status.Message = fmt.Sprintf("Processed %d placements with %d total clusters",
		len(rollout.Status.Placements), rollout.Status.TotalCounts.Total)

	if err := r.Status().Update(ctx, rollout); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	logger.Info("Reconciliation complete",
		"duration", duration.Duration,
		"placements", len(rollout.Status.Placements),
		"totalClusters", rollout.Status.TotalCounts.Total)

	return result, nil
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

	// Get all ManagedClusters
	managedClusters := &clusterv1.ManagedClusterList{}
	if err := r.List(ctx, managedClusters); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list ManagedClusters: %w", err)
	}

	logger.Info("Listed ManagedClusters", "count", len(managedClusters.Items))

	// Collect all placement infos from the selection spec
	placementInfos, err := r.collectPlacementInfos(ctx, rollout, &rollout.Spec.SelectionSpec, managedClusters.Items, nil, nil)
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

		// Build placement status (counts will be populated below)
		now := metav1.Now()
		placementStatuses = append(placementStatuses, rolloutv1alpha1.PlacementStatus{
			Name:        info.name,
			Namespace:   rollout.Namespace,
			GroupKey:    info.groupKey,
			GroupValue:  info.groupValue,
			Generated:   true,
			Counts:      rolloutv1alpha1.StateCounts{}, // Will be populated below
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
	placementStatuses, totalCounts := r.updatePlacementStatusesWithCounts(ctx, rollout.Namespace, placementStatuses)

	rollout.Status.Placements = placementStatuses
	rollout.Status.TotalCounts = &totalCounts

	// Note: FailedClusters and InProgressClusters would be populated by examining
	// ClusterCurator resources - this is a placeholder for future implementation
	rollout.Status.FailedClusters = nil
	rollout.Status.InProgressClusters = nil

	return ctrl.Result{RequeueAfter: RequeueAfterIdle}, nil
}

// collectPlacementInfos recursively collects placement information from the selection spec
func (r *ClusterCuratorRolloutReconciler) collectPlacementInfos(
	ctx context.Context,
	rollout *rolloutv1alpha1.ClusterCuratorRollout,
	spec *rolloutv1alpha1.SelectionSpec,
	clusters []clusterv1.ManagedCluster,
	parentPredicates []clusterv1beta1.ClusterPredicate,
	parentPath []string,
) ([]placementInfo, error) {
	logger := log.FromContext(ctx)
	var result []placementInfo

	switch spec.Type {
	case rolloutv1alpha1.SelectionTypeGroup:
		// For group type, we need to find all unique values for the label key
		// and create a placement for each
		if spec.LabelSelector == nil || len(spec.LabelSelector.MatchLabels) == 0 {
			return nil, fmt.Errorf("group type requires labelSelector with matchLabels keys")
		}

		// Get the first key from matchLabels (for grouping)
		var groupKey string
		for k := range spec.LabelSelector.MatchLabels {
			groupKey = k
			break
		}

		// Find all unique values for this key across clusters
		uniqueValues := r.findUniqueLabelValues(clusters, groupKey)
		logger.Info("Found unique values for group key", "key", groupKey, "values", uniqueValues, "clusterCount", len(clusters))

		for _, value := range uniqueValues {
			// Build predicate for this group value
			predicates := append(clonePredicates(parentPredicates), clusterv1beta1.ClusterPredicate{
				RequiredClusterSelector: clusterv1beta1.ClusterSelector{
					LabelSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{
							groupKey: value,
						},
					},
				},
			})

			// Filter clusters that match this group value
			filteredClusters := filterClustersByLabel(clusters, groupKey, value)
			currentPath := append(clonePath(parentPath), fmt.Sprintf("%s=%s", groupKey, value))

			// If there are children, recurse into them
			if len(spec.Children) > 0 {
				for _, child := range spec.Children {
					childInfos, err := r.collectPlacementInfos(ctx, rollout, &child, filteredClusters, predicates, currentPath)
					if err != nil {
						return nil, err
					}
					result = append(result, childInfos...)
				}
			} else {
				// Leaf node - create a placement
				info := placementInfo{
					name:       generatePlacementName(rollout.Name, currentPath),
					groupKey:   groupKey,
					groupValue: value,
					predicates: predicates,
					parentPath: currentPath,
				}
				result = append(result, info)
			}
		}

	case rolloutv1alpha1.SelectionTypeFilter:
		// For filter type, we add the label selector as a predicate
		predicates := clonePredicates(parentPredicates)
		if spec.LabelSelector != nil {
			predicates = append(predicates, clusterv1beta1.ClusterPredicate{
				RequiredClusterSelector: clusterv1beta1.ClusterSelector{
					LabelSelector: *spec.LabelSelector,
				},
			})
		}

		// Filter clusters based on label selector
		filteredClusters := filterClustersBySelector(clusters, spec.LabelSelector)

		// If there are children, recurse into them
		if len(spec.Children) > 0 {
			for _, child := range spec.Children {
				childInfos, err := r.collectPlacementInfos(ctx, rollout, &child, filteredClusters, predicates, parentPath)
				if err != nil {
					return nil, err
				}
				result = append(result, childInfos...)
			}
		} else {
			// Leaf node - create a placement
			info := placementInfo{
				name:       generatePlacementName(rollout.Name, parentPath),
				predicates: predicates,
				parentPath: parentPath,
			}
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

// findUniqueLabelValues finds all unique values for a given label key across clusters
func (r *ClusterCuratorRolloutReconciler) findUniqueLabelValues(clusters []clusterv1.ManagedCluster, key string) []string {
	valueSet := make(map[string]bool)
	for _, cluster := range clusters {
		if value, ok := cluster.Labels[key]; ok {
			valueSet[value] = true
		}
	}

	var values []string
	for v := range valueSet {
		values = append(values, v)
	}
	sort.Strings(values)
	return values
}

// filterClustersByLabel filters clusters that have a specific label key=value
func filterClustersByLabel(clusters []clusterv1.ManagedCluster, key, value string) []clusterv1.ManagedCluster {
	var result []clusterv1.ManagedCluster
	for _, cluster := range clusters {
		if v, ok := cluster.Labels[key]; ok && v == value {
			result = append(result, cluster)
		}
	}
	return result
}

// filterClustersBySelector filters clusters that match a label selector
func filterClustersBySelector(clusters []clusterv1.ManagedCluster, selector *metav1.LabelSelector) []clusterv1.ManagedCluster {
	if selector == nil {
		return clusters
	}

	var result []clusterv1.ManagedCluster
	for _, cluster := range clusters {
		if matchesLabelSelector(cluster.Labels, selector) {
			result = append(result, cluster)
		}
	}
	return result
}

// matchesLabelSelector checks if labels match a label selector
func matchesLabelSelector(labels map[string]string, selector *metav1.LabelSelector) bool {
	if selector == nil {
		return true
	}

	// Check matchLabels
	for k, v := range selector.MatchLabels {
		if labels[k] != v {
			return false
		}
	}

	// Check matchExpressions
	for _, expr := range selector.MatchExpressions {
		labelValue, hasLabel := labels[expr.Key]
		switch expr.Operator {
		case metav1.LabelSelectorOpIn:
			if !hasLabel {
				return false
			}
			found := false
			for _, v := range expr.Values {
				if labelValue == v {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		case metav1.LabelSelectorOpNotIn:
			if hasLabel {
				for _, v := range expr.Values {
					if labelValue == v {
						return false
					}
				}
			}
		case metav1.LabelSelectorOpExists:
			if !hasLabel {
				return false
			}
		case metav1.LabelSelectorOpDoesNotExist:
			if hasLabel {
				return false
			}
		}
	}

	return true
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

// clonePredicates creates a copy of a slice of predicates
func clonePredicates(predicates []clusterv1beta1.ClusterPredicate) []clusterv1beta1.ClusterPredicate {
	if predicates == nil {
		return nil
	}
	result := make([]clusterv1beta1.ClusterPredicate, len(predicates))
	copy(result, predicates)
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
// This returns condensed StateCounts instead of individual cluster details
func (r *ClusterCuratorRolloutReconciler) updatePlacementStatusesWithCounts(
	ctx context.Context,
	namespace string,
	statuses []rolloutv1alpha1.PlacementStatus,
) ([]rolloutv1alpha1.PlacementStatus, rolloutv1alpha1.StateCounts) {
	totalCounts := rolloutv1alpha1.StateCounts{}

	for i := range statuses {
		// List PlacementDecisions for this placement
		decisions := &clusterv1beta1.PlacementDecisionList{}
		if err := r.List(ctx, decisions, client.InNamespace(namespace), client.MatchingLabels{
			clusterv1beta1.PlacementLabel: statuses[i].Name,
		}); err != nil {
			continue // Skip on error, will be updated on next reconcile
		}

		// Count clusters from all decisions
		clusterCount := 0
		for _, decision := range decisions.Items {
			clusterCount += len(decision.Status.Decisions)
		}

		// For now, all clusters are "pending" until we examine ClusterCurators
		// In a full implementation, we'd query ClusterCurator status for each cluster
		statuses[i].Counts = rolloutv1alpha1.StateCounts{
			Total:   clusterCount,
			Pending: clusterCount, // Placeholder - would be determined by ClusterCurator status
		}

		// Aggregate into totals
		totalCounts.Total += clusterCount
		totalCounts.Pending += clusterCount
	}

	return statuses, totalCounts
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterCuratorRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
