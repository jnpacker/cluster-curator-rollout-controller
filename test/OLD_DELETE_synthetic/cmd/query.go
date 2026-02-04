package cmd

import (
	"context"
	"fmt"
	"sort"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/kubernetes"
	"github.com/spf13/cobra"
)

// QueryCmd represents the query command
var QueryCmd = &cobra.Command{
	Use:   "query",
	Short: "Show all active synthetic test IDs and cluster counts",
	Long:  "Lists all distinct synthetic test IDs and how many clusters are associated with each",
	RunE:  runQuery,
}

func init() {
	QueryCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
}

func runQuery(cmd *cobra.Command, args []string) error {
	// Create Kubernetes client
	fmt.Printf("Connecting to Kubernetes cluster...\n")
	client, err := kubernetes.NewClient(kubeconfig)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Get all ManagedClusters and count by test ID
	testIDCounts := make(map[string]int)

	fmt.Printf("Scanning for synthetic test resources...\n\n")

	// Query ManagedClusters
	managedClusters, err := client.GetManagedClustersByLabel(ctx, "")
	if err != nil {
		return fmt.Errorf("failed to list ManagedClusters: %w", err)
	}

	// Count clusters by test ID
	for _, mc := range managedClusters.Items {
		labels := mc.GetLabels()
		if testID, exists := labels["synthetic-test-id"]; exists {
			testIDCounts[testID]++
		}
	}

	if len(testIDCounts) == 0 {
		fmt.Printf("No synthetic test resources found.\n")
		return nil
	}

	// Sort test IDs for consistent output
	var sortedTestIDs []string
	for testID := range testIDCounts {
		sortedTestIDs = append(sortedTestIDs, testID)
	}
	sort.Strings(sortedTestIDs)

	// Display results
	fmt.Printf("Active Synthetic Test IDs:\n")
	fmt.Printf("=========================\n\n")

	totalClusters := 0
	for _, testID := range sortedTestIDs {
		count := testIDCounts[testID]
		totalClusters += count
		fmt.Printf("Test ID: %s\n", testID)
		fmt.Printf("  Clusters: %d\n\n", count)
	}

	fmt.Printf("=========================\n")
	fmt.Printf("Total Test IDs: %d\n", len(testIDCounts))
	fmt.Printf("Total Clusters: %d\n", totalClusters)

	return nil
}

