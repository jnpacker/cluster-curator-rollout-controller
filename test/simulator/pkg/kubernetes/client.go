package kubernetes

import (
	"context"
	"fmt"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/constants"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/progress"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Client wraps Kubernetes API clients
type Client struct {
	clientset kubernetes.Interface
	dynamic   dynamic.Interface
}

// NewClient creates a new Kubernetes client
func NewClient(kubeconfig string) (*Client, error) {
	var config *rest.Config
	var err error

	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			// Try default kubeconfig location
			home, _ := clientcmd.NewDefaultClientConfigLoadingRules().Load()
			if home.Clusters != nil && len(home.Clusters) > 0 {
				config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	// Increase QPS and Burst to avoid client-side throttling warnings
	config.QPS = 100
	config.Burst = 200

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return &Client{
		clientset: clientset,
		dynamic:   dynamicClient,
	}, nil
}

// CreateNamespace creates a namespace
func (c *Client) CreateNamespace(ctx context.Context, ns *corev1.Namespace) error {
	_, err := c.clientset.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	return err
}

// UpdateNamespaceLabels updates a namespace's labels, merging with existing labels
func (c *Client) UpdateNamespaceLabels(ctx context.Context, name string, labels map[string]string) error {
	ns, err := c.clientset.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Merge labels (new labels take precedence)
	if ns.Labels == nil {
		ns.Labels = make(map[string]string)
	}
	for k, v := range labels {
		ns.Labels[k] = v
	}

	_, err = c.clientset.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
	return err
}

// DeleteNamespace deletes a namespace
func (c *Client) DeleteNamespace(ctx context.Context, name string) error {
	return c.clientset.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{})
}

// CreateResource creates an unstructured resource
func (c *Client) CreateResource(ctx context.Context, resource *unstructured.Unstructured) error {
	gvk := resource.GroupVersionKind()
	gvr, err := c.getGVR(gvk)
	if err != nil {
		return err
	}

	namespace := resource.GetNamespace()
	if namespace == "" {
		_, err = c.dynamic.Resource(gvr).Create(ctx, resource, metav1.CreateOptions{})
	} else {
		_, err = c.dynamic.Resource(gvr).Namespace(namespace).Create(ctx, resource, metav1.CreateOptions{})
	}
	return err
}

// DeleteResource deletes an unstructured resource
func (c *Client) DeleteResource(ctx context.Context, resource *unstructured.Unstructured) error {
	gvk := resource.GroupVersionKind()
	gvr, err := c.getGVR(gvk)
	if err != nil {
		return err
	}

	namespace := resource.GetNamespace()
	name := resource.GetName()

	if namespace == "" {
		return c.dynamic.Resource(gvr).Delete(ctx, name, metav1.DeleteOptions{})
	}
	return c.dynamic.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

// getGVR converts GroupVersionKind to GroupVersionResource
func (c *Client) getGVR(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	gvr, _ := meta.UnsafeGuessKindToResource(gvk)
	return gvr, nil
}

// RemoveFinalizersFromManagedClustersByLabel removes finalizers from all ManagedClusters matching a label selector
func (c *Client) RemoveFinalizersFromManagedClustersByLabel(ctx context.Context, labelSelector string) error {
	return c.removeFinalizersFromClusterScopedResources(ctx, constants.ManagedClusterGVR, labelSelector, "ManagedClusters")
}

// RemoveFinalizersFromClusterCuratorsByLabel removes finalizers from all ClusterCurators matching a label selector
func (c *Client) RemoveFinalizersFromClusterCuratorsByLabel(ctx context.Context, labelSelector string) error {
	return c.removeFinalizersFromNamespacedResources(ctx, constants.ClusterCuratorGVR, labelSelector, "ClusterCurators")
}

// removeFinalizersFromClusterScopedResources is a generic method to remove finalizers from cluster-scoped resources
func (c *Client) removeFinalizersFromClusterScopedResources(ctx context.Context, gvr schema.GroupVersionResource, labelSelector, resourceName string) error {
	list, err := c.dynamic.Resource(gvr).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("failed to list %s: %w", resourceName, err)
	}

	if len(list.Items) == 0 {
		fmt.Printf("  No %s found with label selector: %s\n", resourceName, labelSelector)
		return nil
	}

	// Count items with finalizers
	itemsWithFinalizers := 0
	for _, item := range list.Items {
		if len(item.GetFinalizers()) > 0 {
			itemsWithFinalizers++
		}
	}

	if itemsWithFinalizers == 0 {
		fmt.Printf("  No finalizers found to remove on %s\n", resourceName)
		return nil
	}

	bar := progress.New(itemsWithFinalizers, "Removing finalizers")
	updatedCount := 0
	for _, item := range list.Items {
		if len(item.GetFinalizers()) > 0 {
			item.SetFinalizers([]string{})
			if _, err := c.dynamic.Resource(gvr).Update(ctx, &item, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("failed to remove finalizers from %s %s: %w", resourceName, item.GetName(), err)
			}
			updatedCount++
			bar.Update(updatedCount)
		}
	}

	bar.Complete(fmt.Sprintf("Removed finalizers from %d %s", updatedCount, resourceName))
	return nil
}

// removeFinalizersFromNamespacedResources is a generic method to remove finalizers from namespaced resources
func (c *Client) removeFinalizersFromNamespacedResources(ctx context.Context, gvr schema.GroupVersionResource, labelSelector, resourceName string) error {
	namespaces, err := c.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	if len(namespaces.Items) == 0 {
		fmt.Printf("  No namespaces found with label selector: %s\n", labelSelector)
		return nil
	}

	// Count total items with finalizers
	totalWithFinalizers := 0
	for _, ns := range namespaces.Items {
		list, err := c.dynamic.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			continue
		}
		for _, item := range list.Items {
			if len(item.GetFinalizers()) > 0 {
				totalWithFinalizers++
			}
		}
	}

	if totalWithFinalizers == 0 {
		fmt.Printf("  No finalizers found to remove on %s\n", resourceName)
		return nil
	}

	bar := progress.New(totalWithFinalizers, "Removing finalizers")
	updatedCount := 0
	for _, ns := range namespaces.Items {
		list, err := c.dynamic.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			continue
		}

		for _, item := range list.Items {
			if len(item.GetFinalizers()) > 0 {
				item.SetFinalizers([]string{})
				if _, err := c.dynamic.Resource(gvr).Namespace(ns.Name).Update(ctx, &item, metav1.UpdateOptions{}); err != nil {
					return fmt.Errorf("failed to remove finalizers from %s %s: %w", resourceName, item.GetName(), err)
				}
				updatedCount++
				bar.Update(updatedCount)
			}
		}
	}

	bar.Complete(fmt.Sprintf("Removed finalizers from %d %s", updatedCount, resourceName))
	return nil
}

// DeleteNamespacesByLabel deletes all namespaces matching a label selector
func (c *Client) DeleteNamespacesByLabel(ctx context.Context, labelSelector string) error {
	namespaces, err := c.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	if len(namespaces.Items) == 0 {
		fmt.Printf("  No namespaces found with label selector: %s\n", labelSelector)
		return nil
	}

	total := len(namespaces.Items)

	// Remove finalizers first
	finalizerBar := progress.New(total, "Removing NS finalizers")
	for i, ns := range namespaces.Items {
		if len(ns.GetFinalizers()) > 0 {
			ns.SetFinalizers([]string{})
			if _, err := c.clientset.CoreV1().Namespaces().Update(ctx, &ns, metav1.UpdateOptions{}); err != nil {
				fmt.Printf("\n  Warning: failed to remove finalizers from namespace %s: %v\n", ns.Name, err)
			}
		}
		finalizerBar.Update(i + 1)
	}
	finalizerBar.Complete(fmt.Sprintf("Removed finalizers from %d namespaces", total))

	// Delete namespaces
	deleteBar := progress.New(total, "Deleting namespaces")
	deleteCount := 0
	for _, ns := range namespaces.Items {
		if err := c.clientset.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("failed to delete namespace %s: %w", ns.Name, err)
		}
		deleteCount++
		deleteBar.Update(deleteCount)
	}

	deleteBar.Complete(fmt.Sprintf("Initiated deletion of %d namespaces", deleteCount))
	return nil
}

// DeleteManagedClustersByLabel deletes all ManagedClusters matching a label selector
func (c *Client) DeleteManagedClustersByLabel(ctx context.Context, labelSelector string) error {
	return c.deleteClusterScopedResources(ctx, constants.ManagedClusterGVR, labelSelector, "ManagedClusters")
}

// DeleteClusterCuratorsByLabel deletes all ClusterCurators matching a label selector
func (c *Client) DeleteClusterCuratorsByLabel(ctx context.Context, labelSelector string) error {
	return c.deleteNamespacedResources(ctx, constants.ClusterCuratorGVR, labelSelector, "ClusterCurators")
}

// deleteClusterScopedResources is a generic method to delete cluster-scoped resources
func (c *Client) deleteClusterScopedResources(ctx context.Context, gvr schema.GroupVersionResource, labelSelector, resourceName string) error {
	list, err := c.dynamic.Resource(gvr).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("failed to list %s: %w", resourceName, err)
	}

	total := len(list.Items)
	if total == 0 {
		fmt.Printf("  No %s found with label selector: %s\n", resourceName, labelSelector)
		return nil
	}

	bar := progress.New(total, fmt.Sprintf("Deleting %s", resourceName))
	deleteCount := 0
	for _, item := range list.Items {
		if err := c.dynamic.Resource(gvr).Delete(ctx, item.GetName(), metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("failed to delete %s %s: %w", resourceName, item.GetName(), err)
		}
		deleteCount++
		bar.Update(deleteCount)
	}

	bar.Complete(fmt.Sprintf("Initiated deletion of %d %s", deleteCount, resourceName))
	return nil
}

// deleteNamespacedResources is a generic method to delete namespaced resources
func (c *Client) deleteNamespacedResources(ctx context.Context, gvr schema.GroupVersionResource, labelSelector, resourceName string) error {
	namespaces, err := c.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	if len(namespaces.Items) == 0 {
		fmt.Printf("  No namespaces found with label selector: %s\n", labelSelector)
		return nil
	}

	// Count total to delete
	totalToDelete := 0
	for _, ns := range namespaces.Items {
		list, err := c.dynamic.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err == nil {
			totalToDelete += len(list.Items)
		}
	}

	if totalToDelete == 0 {
		fmt.Printf("  No %s found with label selector: %s\n", resourceName, labelSelector)
		return nil
	}

	bar := progress.New(totalToDelete, fmt.Sprintf("Deleting %s", resourceName))
	totalDeleted := 0
	for _, ns := range namespaces.Items {
		list, err := c.dynamic.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return fmt.Errorf("failed to list %s in namespace %s: %w", resourceName, ns.Name, err)
		}

		for _, item := range list.Items {
			if err := c.dynamic.Resource(gvr).Namespace(ns.Name).Delete(ctx, item.GetName(), metav1.DeleteOptions{}); err != nil {
				return fmt.Errorf("failed to delete %s %s in namespace %s: %w", resourceName, item.GetName(), ns.Name, err)
			}
			totalDeleted++
			bar.Update(totalDeleted)
		}
	}

	bar.Complete(fmt.Sprintf("Initiated deletion of %d %s", totalDeleted, resourceName))
	return nil
}

// GetManagedClustersByLabel gets all ManagedClusters matching a label selector
func (c *Client) GetManagedClustersByLabel(ctx context.Context, labelSelector string) (*unstructured.UnstructuredList, error) {
	return c.dynamic.Resource(constants.ManagedClusterGVR).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
}

// GetManagedCluster gets a single ManagedCluster by name
func (c *Client) GetManagedCluster(ctx context.Context, name string) (*unstructured.Unstructured, error) {
	return c.dynamic.Resource(constants.ManagedClusterGVR).Get(ctx, name, metav1.GetOptions{})
}

// GetClusterCurator gets a single ClusterCurator by name and namespace
func (c *Client) GetClusterCurator(ctx context.Context, name, namespace string) (*unstructured.Unstructured, error) {
	return c.dynamic.Resource(constants.ClusterCuratorGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}

// UpdateManagedCluster updates a ManagedCluster resource
func (c *Client) UpdateManagedCluster(ctx context.Context, mc *unstructured.Unstructured) error {
	_, err := c.dynamic.Resource(constants.ManagedClusterGVR).Update(ctx, mc, metav1.UpdateOptions{})
	return err
}

// GetNamespace gets a namespace by name
func (c *Client) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	return c.clientset.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
}

// ListResources lists unstructured resources filtered by infra-test-id label
func (c *Client) ListResources(ctx context.Context, list *unstructured.UnstructuredList, testID string) error {
	gvk := list.GroupVersionKind()
	kind := gvk.Kind
	if len(kind) > 4 && kind[len(kind)-4:] == "List" {
		kind = kind[:len(kind)-4]
	}
	gvr, err := c.getGVR(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    kind,
	})
	if err != nil {
		return err
	}

	labelSelector := fmt.Sprintf("%s=%s", constants.LabelInfraTestID, testID)

	namespaces, err := c.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	for _, ns := range namespaces.Items {
		nsList, err := c.dynamic.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			continue
		}
		list.Items = append(list.Items, nsList.Items...)
	}

	return nil
}

// ListResourcesInNamespace lists unstructured resources in a specific namespace
func (c *Client) ListResourcesInNamespace(ctx context.Context, list *unstructured.UnstructuredList, namespace string) error {
	gvk := list.GroupVersionKind()
	kind := gvk.Kind
	if len(kind) > 4 && kind[len(kind)-4:] == "List" {
		kind = kind[:len(kind)-4]
	}
	gvr, err := c.getGVR(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    kind,
	})
	if err != nil {
		return err
	}

	nsList, err := c.dynamic.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	list.Items = append(list.Items, nsList.Items...)
	return nil
}

// UpdateResource updates an unstructured resource
func (c *Client) UpdateResource(ctx context.Context, resource *unstructured.Unstructured) error {
	gvk := resource.GroupVersionKind()
	gvr, err := c.getGVR(gvk)
	if err != nil {
		return err
	}

	namespace := resource.GetNamespace()
	if namespace == "" {
		_, err = c.dynamic.Resource(gvr).Update(ctx, resource, metav1.UpdateOptions{})
	} else {
		_, err = c.dynamic.Resource(gvr).Namespace(namespace).Update(ctx, resource, metav1.UpdateOptions{})
	}
	return err
}

// WatchClusterCurators watches all ClusterCurator resources across all namespaces.
// Filtering by test ID is done in the event handler by checking the namespace labels.
func (c *Client) WatchClusterCurators(ctx context.Context) (watch.Interface, error) {
	// Watch across all namespaces without label filtering
	// The event handler will filter by checking namespace labels for infra-test-id
	return c.dynamic.Resource(constants.ClusterCuratorGVR).Watch(ctx, metav1.ListOptions{})
}

// GetClusterCuratorsForManagedClusters looks up ClusterCurators by first finding ManagedClusters
// with the given label selector, then looking for ClusterCurators in namespaces matching
// each ManagedCluster's name. This is necessary because ClusterCurators created by the
// controller don't have the infra-test-id label - they're in namespaces matching the cluster name.
func (c *Client) GetClusterCuratorsForManagedClusters(ctx context.Context, labelSelector string) ([]*unstructured.Unstructured, error) {
	// First, find all ManagedClusters with the label selector
	managedClusters, err := c.dynamic.Resource(constants.ManagedClusterGVR).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return nil, fmt.Errorf("failed to list ManagedClusters: %w", err)
	}

	var curators []*unstructured.Unstructured
	for _, mc := range managedClusters.Items {
		clusterName := mc.GetName()
		// ClusterCurator has the same name as the cluster and lives in a namespace with the same name
		curator, err := c.dynamic.Resource(constants.ClusterCuratorGVR).Namespace(clusterName).Get(ctx, clusterName, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				return nil, fmt.Errorf("failed to get ClusterCurator for cluster %s: %w", clusterName, err)
			}
			// ClusterCurator doesn't exist for this cluster, skip it
			continue
		}
		curators = append(curators, curator)
	}

	return curators, nil
}

// RemoveFinalizersFromClusterCuratorsViaManagedClusters removes finalizers from ClusterCurators
// by looking them up via ManagedClusters with the given label selector
func (c *Client) RemoveFinalizersFromClusterCuratorsViaManagedClusters(ctx context.Context, labelSelector string) error {
	curators, err := c.GetClusterCuratorsForManagedClusters(ctx, labelSelector)
	if err != nil {
		return err
	}

	if len(curators) == 0 {
		fmt.Printf("  No ClusterCurators found for ManagedClusters with label selector: %s\n", labelSelector)
		return nil
	}

	// Count items with finalizers
	itemsWithFinalizers := 0
	for _, curator := range curators {
		if len(curator.GetFinalizers()) > 0 {
			itemsWithFinalizers++
		}
	}

	if itemsWithFinalizers == 0 {
		fmt.Printf("  No finalizers found to remove on ClusterCurators\n")
		return nil
	}

	bar := progress.New(itemsWithFinalizers, "Removing finalizers")
	updatedCount := 0
	for _, curator := range curators {
		if len(curator.GetFinalizers()) > 0 {
			curator.SetFinalizers([]string{})
			if _, err := c.dynamic.Resource(constants.ClusterCuratorGVR).Namespace(curator.GetNamespace()).Update(ctx, curator, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("failed to remove finalizers from ClusterCurator %s/%s: %w", curator.GetNamespace(), curator.GetName(), err)
			}
			updatedCount++
			bar.Update(updatedCount)
		}
	}

	bar.Complete(fmt.Sprintf("Removed finalizers from %d ClusterCurators", updatedCount))
	return nil
}

// ListResourcesByGVR lists all cluster-scoped resources of a given GVR
func (c *Client) ListResourcesByGVR(ctx context.Context, gvr schema.GroupVersionResource) (*unstructured.UnstructuredList, error) {
	return c.dynamic.Resource(gvr).List(ctx, metav1.ListOptions{})
}

// DeleteClusterCuratorsViaManagedClusters deletes ClusterCurators by looking them up via
// ManagedClusters with the given label selector
func (c *Client) DeleteClusterCuratorsViaManagedClusters(ctx context.Context, labelSelector string) error {
	curators, err := c.GetClusterCuratorsForManagedClusters(ctx, labelSelector)
	if err != nil {
		return err
	}

	if len(curators) == 0 {
		fmt.Printf("  No ClusterCurators found for ManagedClusters with label selector: %s\n", labelSelector)
		return nil
	}

	bar := progress.New(len(curators), "Deleting ClusterCurators")
	deleteCount := 0
	for _, curator := range curators {
		if err := c.dynamic.Resource(constants.ClusterCuratorGVR).Namespace(curator.GetNamespace()).Delete(ctx, curator.GetName(), metav1.DeleteOptions{}); err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to delete ClusterCurator %s/%s: %w", curator.GetNamespace(), curator.GetName(), err)
			}
		}
		deleteCount++
		bar.Update(deleteCount)
	}
	bar.Complete(fmt.Sprintf("Initiated deletion of %d ClusterCurators", deleteCount))
	return nil
}