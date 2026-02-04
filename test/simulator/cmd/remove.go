package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/config"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/kubernetes"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/models"
	"github.com/spf13/cobra"
)

var (
	removeDryRun    bool
	removeConfigID  string
	removeConfig    string
	removeKubeconfig string
)

// RemoveCmd represents the remove command
var RemoveCmd = &cobra.Command{
	Use:   "remove",
	Short: "Remove test infrastructure",
	Long: `Removes per-cluster Namespace, ManagedCluster, and ClusterCurator resources created by the tool.

When --config-id is provided, resources are identified directly from the cluster by label selector,
and the config file is not required to have valid cluster definitions.`,
	RunE: runRemove,
}

func init() {
	RemoveCmd.Flags().BoolVar(&removeDryRun, "dry-run", false, "Preview deletion without applying")
	RemoveCmd.Flags().StringVar(&removeKubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	RemoveCmd.Flags().StringVar(&removeConfigID, "config-id", "", "Specific config ID to remove (overrides config file values)")
	RemoveCmd.Flags().StringVar(&removeConfig, "config", "", "Path to config file (can also use SIMULATE_CONFIG env var)")
}

func runRemove(cmd *cobra.Command, args []string) error {
	// Resolve config file: flag > env var > positional arg (for backwards compatibility)
	configFile := removeConfig
	if configFile == "" {
		configFile = os.Getenv("SIMULATE_CONFIG")
	}
	if configFile == "" && len(args) > 0 {
		configFile = args[0]
	}

	// When config-id is provided, config file is optional
	var cfg *models.InfraTestConfig
	var err error

	if removeConfigID != "" {
		// Config-id provided: use it directly, config file is optional
		fmt.Printf("Using config-id: %s\n", removeConfigID)
		cfg = &models.InfraTestConfig{
			TestID: removeConfigID,
		}

		// If config file is provided, load it minimally to update activeTestIDs later
		if configFile != "" {
			fmt.Printf("Loading configuration from %s (minimal mode)...\n", configFile)
			cfg, err = config.LoadConfigMinimal(configFile)
			if err != nil {
				// Config file exists but failed to parse - just use the config-id
				fmt.Printf("Warning: Could not load config file: %v\n", err)
				cfg = &models.InfraTestConfig{
					TestID: removeConfigID,
				}
			} else {
				cfg.TestID = removeConfigID
			}
		}
	} else {
		// No config-id: require config file
		if configFile == "" {
			return fmt.Errorf("config file required: use --config flag, SIMULATE_CONFIG env var, or provide as argument")
		}

		fmt.Printf("Loading configuration from %s...\n", configFile)
		cfg, err = config.LoadConfig(configFile)
		if err != nil {
			return err
		}

		// Check if there are any active test IDs to remove
		if len(cfg.ActiveTestIDs) == 0 {
			fmt.Printf("\n❌ ERROR: No active test IDs found in config file\n")
			fmt.Printf("Nothing to remove. Use 'init' command to create test infrastructure first.\n")
			return fmt.Errorf("no active test IDs found in config file")
		}

		// Check if multiple config IDs exist
		if len(cfg.ActiveTestIDs) > 1 {
			fmt.Printf("\n⚠️  ERROR: Multiple test IDs found in config file:\n")
			fmt.Printf("====================================================================\n")
			for i, id := range cfg.ActiveTestIDs {
				fmt.Printf("  %d. %s\n", i+1, id)
			}
			fmt.Printf("====================================================================\n")
			fmt.Printf("\nPlease specify which one to remove using --config-id flag:\n")
			fmt.Printf("  ./simulate remove --config %s --config-id <ID>\n\n", configFile)
			fmt.Printf("Example:\n")
			fmt.Printf("  ./simulate remove --config %s --config-id %s\n", configFile, cfg.ActiveTestIDs[0])
			return fmt.Errorf("multiple config IDs found - please specify one with --config-id flag")
		}
	}

	// Display test ID
	separator := "======================================================================"
	fmt.Printf("\n%s\n", separator)
	fmt.Printf("REMOVING RESOURCES FOR TEST ID: %s\n", cfg.TestID)
	fmt.Printf("%s\n\n", separator)

	if removeDryRun {
		fmt.Printf("[DRY RUN] Would remove all resources with label infra-test-id=%s\n", cfg.TestID)
		return nil
	}

	// Create Kubernetes client
	fmt.Printf("\nConnecting to Kubernetes cluster...\n")
	client, err := kubernetes.NewClient(removeKubeconfig)
	if err != nil {
		return err
	}

	// Delete all resources using test ID label selector
	fmt.Printf("Deleting resources by test ID: %s\n", cfg.TestID)
	labelSelector := fmt.Sprintf("infra-test-id=%s", cfg.TestID)
	fmt.Printf("  Using label selector: %s\n", labelSelector)

	ctx := context.Background()

	// Remove finalizers first to allow deletion
	// ClusterCurators are looked up via ManagedClusters since they don't have the infra-test-id label
	fmt.Printf("Removing finalizers from ClusterCurators (via ManagedCluster lookup)...\n")
	if err := client.RemoveFinalizersFromClusterCuratorsViaManagedClusters(ctx, labelSelector); err != nil {
		return fmt.Errorf("failed to remove finalizers from ClusterCurators: %w", err)
	}
	fmt.Printf("✓ Removed finalizers from ClusterCurators\n")

	fmt.Printf("Removing finalizers from ManagedClusters...\n")
	if err := client.RemoveFinalizersFromManagedClustersByLabel(ctx, labelSelector); err != nil {
		return fmt.Errorf("failed to remove finalizers from ManagedClusters: %w", err)
	}
	fmt.Printf("✓ Removed finalizers from ManagedClusters\n")

	// Delete resources in order: ClusterCurators -> ManagedClusters -> Namespaces
	// ClusterCurators are looked up via ManagedClusters since they don't have the infra-test-id label
	fmt.Printf("Deleting ClusterCurators (via ManagedCluster lookup)...\n")
	ccErr := client.DeleteClusterCuratorsViaManagedClusters(ctx, labelSelector)
	if ccErr != nil {
		fmt.Printf("✗ Failed to delete ClusterCurators: %v\n", ccErr)
	} else {
		fmt.Printf("✓ Deleted ClusterCurators\n")
	}

	fmt.Printf("Deleting ManagedClusters...\n")
	mcErr := client.DeleteManagedClustersByLabel(ctx, labelSelector)
	if mcErr != nil {
		fmt.Printf("✗ Failed to delete ManagedClusters: %v\n", mcErr)
	} else {
		fmt.Printf("✓ Deleted ManagedClusters\n")
	}

	fmt.Printf("Deleting Namespaces...\n")
	nsErr := client.DeleteNamespacesByLabel(ctx, labelSelector)
	if nsErr != nil {
		fmt.Printf("✗ Failed to delete Namespaces: %v\n", nsErr)
	} else {
		fmt.Printf("✓ Deleted Namespaces\n")
	}

	// Only remove test ID from config if ALL deletions succeeded
	if ccErr == nil && mcErr == nil && nsErr == nil {
		// Only update config file if it was provided
		if configFile != "" {
			fmt.Printf("\nRemoving test ID from config file...\n")
			if config.RemoveTestID(cfg, cfg.TestID) {
				if err := config.SaveConfig(configFile, cfg); err != nil {
					fmt.Printf("Warning: Failed to save config file after removing test ID: %v\n", err)
				} else {
					fmt.Printf("✓ Removed test ID %s from config file\n", cfg.TestID)
				}
			}
		}
		fmt.Printf("\n✓ Successfully removed all resources for test ID: %s\n", cfg.TestID)
		return nil
	}

	// If any deletion failed, report error but keep test ID in config
	fmt.Printf("\n⚠️  Some deletions failed - test ID kept in config file for reference\n")
	if ccErr != nil {
		return fmt.Errorf("failed to delete ClusterCurators: %w", ccErr)
	}
	if mcErr != nil {
		return fmt.Errorf("failed to delete ManagedClusters: %w", mcErr)
	}
	return fmt.Errorf("failed to delete Namespaces: %w", nsErr)
}