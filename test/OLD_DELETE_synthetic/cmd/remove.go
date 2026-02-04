package cmd

import (
	"context"
	"fmt"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/config"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/generator"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/kubernetes"
	"github.com/spf13/cobra"
)

var removeDryRun bool

// RemoveCmd represents the remove command
var RemoveCmd = &cobra.Command{
	Use:   "remove",
	Short: "Remove synthetic test infrastructure",
	Long:  "Removes per-cluster Namespace, ManagedCluster, and ClusterCurator CRs created by the tool",
	RunE:  runRemove,
}

func init() {
	RemoveCmd.Flags().BoolVar(&removeDryRun, "dry-run", false, "Preview deletion without applying")
	RemoveCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
}

func runRemove(cmd *cobra.Command, args []string) error {
	configFile := args[0]
	if configFile == "" {
		return fmt.Errorf("config file path required as argument")
	}

	// Load configuration
	fmt.Printf("Loading configuration from %s...\n", configFile)
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		return err
	}

	// Distribute labels (needed to know cluster names)
	fmt.Printf("Regenerating %d synthetic clusters for removal...\n", cfg.TotalClusters)
	distributor := generator.NewDistributor(cfg.TotalClusters)
	if err := distributor.Distribute(cfg.Labels); err != nil {
		return err
	}

	clusters := distributor.GetClusters()

	// Display test ID and cluster details
	fmt.Printf("Removing resources for test ID: %s\n", cfg.TestID)
	fmt.Printf("\nCluster Label Distribution:\n")
	for _, cluster := range clusters {
		fmt.Printf("  %s: %v\n", cluster.Name, cluster.Labels)
	}

	if removeDryRun {
		fmt.Printf("\n[DRY RUN] Would remove the above %d clusters and their resources\n", len(clusters))
		return nil
	}

	// Create Kubernetes client
	fmt.Printf("\nConnecting to Kubernetes cluster...\n")
	client, err := kubernetes.NewClient(kubeconfig)
	if err != nil {
		return err
	}

	// Delete all resources using test ID label selector
	fmt.Printf("Deleting resources by test ID: %s\n", cfg.TestID)
	labelSelector := fmt.Sprintf("synthetic-test-id=%s", cfg.TestID)

	ctx := context.Background()

	// Remove finalizers first to allow deletion
	fmt.Printf("Removing finalizers from ClusterCurators...\n")
	if err := client.RemoveFinalizersFromClusterCuratorsByLabel(ctx, labelSelector); err != nil {
		return fmt.Errorf("failed to remove finalizers from ClusterCurators: %w", err)
	}
	fmt.Printf("✓ Removed finalizers from ClusterCurators\n")

	fmt.Printf("Removing finalizers from ManagedClusters...\n")
	if err := client.RemoveFinalizersFromManagedClustersByLabel(ctx, labelSelector); err != nil {
		return fmt.Errorf("failed to remove finalizers from ManagedClusters: %w", err)
	}
	fmt.Printf("✓ Removed finalizers from ManagedClusters\n")

	// Delete resources in order: ClusterCurators -> ManagedClusters -> Namespaces
	fmt.Printf("Deleting ClusterCurators...\n")
	if err := client.DeleteClusterCuratorsByLabel(ctx, labelSelector); err != nil {
		return fmt.Errorf("failed to delete ClusterCurators: %w", err)
	}
	fmt.Printf("✓ Deleted ClusterCurators\n")

	fmt.Printf("Deleting ManagedClusters...\n")
	if err := client.DeleteManagedClustersByLabel(ctx, labelSelector); err != nil {
		return fmt.Errorf("failed to delete ManagedClusters: %w", err)
	}
	fmt.Printf("✓ Deleted ManagedClusters\n")

	fmt.Printf("Deleting Namespaces...\n")
	if err := client.DeleteNamespacesByLabel(ctx, labelSelector); err != nil {
		return fmt.Errorf("failed to delete Namespaces: %w", err)
	}
	fmt.Printf("✓ Deleted Namespaces\n")

	fmt.Printf("\n✓ Successfully removed all resources for %d synthetic clusters\n", len(clusters))
	return nil
}