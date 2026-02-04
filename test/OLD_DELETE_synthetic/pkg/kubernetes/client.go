package kubernetes

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	gvr := schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "managedclusters",
	}

	// List all ManagedClusters with the label
	list, err := c.dynamic.Resource(gvr).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("failed to list ManagedClusters: %w", err)
	}

	// Remove finalizers from each one
	for _, item := range list.Items {
		if len(item.GetFinalizers()) > 0 {
			item.SetFinalizers([]string{})
			_, err := c.dynamic.Resource(gvr).Update(ctx, &item, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to remove finalizers from ManagedCluster %s: %w", item.GetName(), err)
			}
		}
	}

	return nil
}

// RemoveFinalizersFromClusterCuratorsByLabel removes finalizers from all ClusterCurators matching a label selector
func (c *Client) RemoveFinalizersFromClusterCuratorsByLabel(ctx context.Context, labelSelector string) error {
	gvr := schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "clustercurators",
	}

	// List all namespaces and search for ClusterCurators in each
	namespaces, err := c.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	for _, ns := range namespaces.Items {
		// List all ClusterCurators with the label in this namespace
		list, err := c.dynamic.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			// Continue with other namespaces if this one fails
			continue
		}

		// Remove finalizers from each one
		for _, item := range list.Items {
			if len(item.GetFinalizers()) > 0 {
				item.SetFinalizers([]string{})
				_, err := c.dynamic.Resource(gvr).Namespace(ns.Name).Update(ctx, &item, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to remove finalizers from ClusterCurator %s: %w", item.GetName(), err)
				}
			}
		}
	}

	return nil
}

// DeleteNamespacesByLabel deletes all namespaces matching a label selector
func (c *Client) DeleteNamespacesByLabel(ctx context.Context, labelSelector string) error {
	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "namespaces",
	}
	return c.dynamic.Resource(gvr).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
}

// DeleteManagedClustersByLabel deletes all ManagedClusters matching a label selector
func (c *Client) DeleteManagedClustersByLabel(ctx context.Context, labelSelector string) error {
	gvr := schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "managedclusters",
	}
	return c.dynamic.Resource(gvr).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
}

// DeleteClusterCuratorsByLabel deletes all ClusterCurators matching a label selector (across all namespaces)
func (c *Client) DeleteClusterCuratorsByLabel(ctx context.Context, labelSelector string) error {
	gvr := schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "clustercurators",
	}
	// List all namespaces first, then delete ClusterCurators in each with label selector
	namespaces, err := c.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	for _, ns := range namespaces.Items {
		err = c.dynamic.Resource(gvr).Namespace(ns.Name).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err != nil {
			// Continue with other namespaces even if one fails
			continue
		}
	}

	return nil
}

// GetManagedClustersByLabel gets all ManagedClusters matching a label selector (or all if selector is empty)
func (c *Client) GetManagedClustersByLabel(ctx context.Context, labelSelector string) (*unstructured.UnstructuredList, error) {
	gvr := schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "managedclusters",
	}

	return c.dynamic.Resource(gvr).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
}

