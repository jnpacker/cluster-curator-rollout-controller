package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/config"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/kubernetes"
	"github.com/spf13/cobra"
)

var (
	queryKubeconfig string
	queryConfig     string
)

// QueryCmd represents the query command
var QueryCmd = &cobra.Command{
	Use:   "query",
	Short: "Show all active test IDs and cluster counts",
	Long:  "Lists all active test IDs from the config file and queries cluster counts from Kubernetes",
	RunE:  runQuery,
}

func init() {
	QueryCmd.Flags().StringVar(&queryKubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	QueryCmd.Flags().StringVar(&queryConfig, "config", "", "Path to config file (can also use SIMULATE_CONFIG env var)")
}

func runQuery(cmd *cobra.Command, args []string) error {
	// Resolve config file: flag > env var > positional arg (for backwards compatibility)
	configFile := queryConfig
	if configFile == "" {
		configFile = os.Getenv("SIMULATE_CONFIG")
	}
	if configFile == "" && len(args) > 0 {
		configFile = args[0]
	}
	if configFile == "" {
		return fmt.Errorf("config file required: use --config flag, SIMULATE_CONFIG env var, or provide as argument")
	}

	// Load configuration
	fmt.Printf("Loading configuration from %s...\n", configFile)
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		return err
	}

	// Create Kubernetes client
	fmt.Printf("Connecting to Kubernetes cluster...\n\n")
	client, err := kubernetes.NewClient(queryKubeconfig)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Count clusters by test ID
	testIDCounts := make(map[string]int)

	// Query all ManagedClusters and count by test ID
	managedClusters, err := client.GetManagedClustersByLabel(ctx, "")
	if err != nil {
		return fmt.Errorf("failed to list ManagedClusters: %w", err)
	}

	// Count clusters by test ID
	for _, mc := range managedClusters.Items {
		labels := mc.GetLabels()
		if testID, exists := labels["infra-test-id"]; exists {
			testIDCounts[testID]++
		}
	}

	// Display results from config file
	fmt.Printf("Active Test IDs from Config:\n")
	fmt.Printf("===========================\n\n")

	if len(cfg.ActiveTestIDs) == 0 {
		fmt.Printf("No active test IDs in config file.\n")
	} else {
		totalClusters := 0
		for _, testID := range cfg.ActiveTestIDs {
			count := testIDCounts[testID]
			totalClusters += count
			fmt.Printf("Test ID: %s\n", testID)
			fmt.Printf("  Clusters: %d\n", count)
		}
		fmt.Printf("\n===========================\n")
		fmt.Printf("Total Test IDs: %d\n", len(cfg.ActiveTestIDs))
		fmt.Printf("Total Clusters: %d\n", totalClusters)
	}

	// Show any clusters in Kubernetes not in config (orphaned)
	orphanedIDs := make(map[string]int)
	for testID, count := range testIDCounts {
		foundInConfig := false
		for _, id := range cfg.ActiveTestIDs {
			if id == testID {
				foundInConfig = true
				break
			}
		}
		if !foundInConfig {
			orphanedIDs[testID] = count
		}
	}

	if len(orphanedIDs) > 0 {
		fmt.Printf("\n⚠️  Orphaned Test IDs (in cluster but not in config):\n")
		fmt.Printf("===========================\n")
		var orphanedKeys []string
		for testID := range orphanedIDs {
			orphanedKeys = append(orphanedKeys, testID)
		}
		sort.Strings(orphanedKeys)

		for _, testID := range orphanedKeys {
			count := orphanedIDs[testID]
			fmt.Printf("Test ID: %s\n", testID)
			fmt.Printf("  Clusters: %d\n", count)
		}
		fmt.Printf("\nYou can remove orphaned test IDs manually:\n")
		fmt.Printf("  infra-test-manager remove <config-file>\n")
	}

	return nil
}
