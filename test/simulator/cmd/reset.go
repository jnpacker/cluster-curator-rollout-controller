package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/config"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/constants"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/generator"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/kubernetes"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/progress"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var (
	resetDryRun     bool
	resetConfigID   string
	resetKubeconfig string
	resetConfig     string
)

// ResetCmd represents the reset command
var ResetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset ClusterCurators for CCR controller restart",
	Long: `Reset all ClusterCurators belonging to a test ID so the CCR controller can restart.

This command will:
  - Clear spec.desiredCuration (remove upgrade trigger)
  - Clear spec.upgrade.desiredUpdate (remove target version)
  - Clear status conditions (reset upgrade status)
  - Remove the rollout.open-cluster-management.io/managed-curator label
  - Reset ManagedCluster version labels to base version from config

After reset, the ClusterCuratorRollout controller can re-process these clusters.

When --config-id is provided, it overrides any test ID in the config file.

Example:
  ./simulate reset --config config/simple_clusters.yaml
  ./simulate reset --config config/infra_clusters.json --config-id infra-abc123-456789`,
	RunE: runReset,
}

func init() {
	ResetCmd.Flags().BoolVar(&resetDryRun, "dry-run", false, "Preview reset without applying")
	ResetCmd.Flags().StringVar(&resetKubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	ResetCmd.Flags().StringVar(&resetConfigID, "config-id", "", "Specific config ID to reset (overrides config file)")
	ResetCmd.Flags().StringVar(&resetConfig, "config", "", "Path to config file (can also use SIMULATE_CONFIG env var)")
}

func runReset(cmd *cobra.Command, args []string) error {
	// Resolve config file: flag > env var > positional arg (for backwards compatibility)
	configFile := resetConfig
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

	// Handle config ID: if provided via flag, use it directly (overrides config file)
	if resetConfigID != "" {
		fmt.Printf("Using config-id: %s (overriding config file)\n", resetConfigID)
		cfg.TestID = resetConfigID
	} else {
		// No config-id flag: check active test IDs in config file
		if len(cfg.ActiveTestIDs) == 0 {
			fmt.Printf("\n❌ ERROR: No active test IDs found in config file\n")
			fmt.Printf("Nothing to reset. Use 'init' command to create test infrastructure first.\n")
			return fmt.Errorf("no active test IDs found in config file")
		}

		// Handle config ID selection when multiple IDs exist
		if len(cfg.ActiveTestIDs) > 1 {
			fmt.Printf("\n⚠️  ERROR: Multiple test IDs found in config file:\n")
			fmt.Printf("====================================================================\n")
			for i, id := range cfg.ActiveTestIDs {
				fmt.Printf("  %d. %s\n", i+1, id)
			}
			fmt.Printf("====================================================================\n")
			fmt.Printf("\nPlease specify which one to reset using --config-id flag:\n")
			fmt.Printf("  ./simulate reset --config %s --config-id <ID>\n\n", configFile)
			fmt.Printf("Example:\n")
			fmt.Printf("  ./simulate reset --config %s --config-id %s\n", configFile, cfg.ActiveTestIDs[0])
			return fmt.Errorf("multiple config IDs found - please specify one with --config-id flag")
		}
	}

	// Check if baseVersions is configured
	if len(cfg.BaseVersions) == 0 {
		return fmt.Errorf("no baseVersions defined in config - cannot reset to base versions")
	}

	// Distribute labels and versions to get cluster -> version mapping
	fmt.Printf("Regenerating %d synthetic clusters for reset...\n", cfg.TotalClusters)
	distributor := generator.NewDistributor(cfg.TotalClusters)
	if err := distributor.Distribute(cfg.Labels); err != nil {
		return err
	}
	distributor.DistributeVersions(cfg.BaseVersions)

	clusters := distributor.GetClusters()

	// Build cluster name -> base version map
	clusterVersions := make(map[string]string)
	for _, cluster := range clusters {
		clusterVersions[cluster.Name] = cluster.BaseVersion
	}

	// Display summary
	separator := "======================================================================"
	fmt.Printf("\n%s\n", separator)
	fmt.Printf("RESETTING CLUSTERCURATORS FOR TEST ID: %s\n", cfg.TestID)
	fmt.Printf("%s\n\n", separator)

	fmt.Printf("Base version distribution:\n")
	versionCounts := make(map[string]int)
	for _, cluster := range clusters {
		versionCounts[cluster.BaseVersion]++
	}
	for version, count := range versionCounts {
		fmt.Printf("  %s: %d clusters\n", version, count)
	}

	if resetDryRun {
		fmt.Printf("\n[DRY RUN] Would reset %d ClusterCurators for CCR controller restart\n", len(clusters))
		fmt.Printf("\nFor each ClusterCurator:\n")
		fmt.Printf("  - Clear spec.desiredCuration\n")
		fmt.Printf("  - Clear spec.upgrade.desiredUpdate\n")
		fmt.Printf("  - Clear status conditions\n")
		fmt.Printf("  - Remove rollout.open-cluster-management.io/managed-curator label\n")
		fmt.Printf("  - Reset ManagedCluster version labels to base version\n")
		fmt.Printf("\nSample resets (first 5):\n")
		displayCount := 5
		if len(clusters) < displayCount {
			displayCount = len(clusters)
		}
		for i := 0; i < displayCount; i++ {
			fmt.Printf("  %s -> ManagedCluster version %s\n",
				clusters[i].Name, clusters[i].BaseVersion)
		}
		if len(clusters) > displayCount {
			fmt.Printf("  ... and %d more\n", len(clusters)-displayCount)
		}
		return nil
	}

	// Create Kubernetes client
	fmt.Printf("\nConnecting to Kubernetes cluster...\n")
	client, err := kubernetes.NewClient(resetKubeconfig)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// List ManagedClusters by infra-test-id to find which clusters to reset
	// ClusterCurators don't have the infra-test-id label - they are created by the CCR controller
	// The ClusterCurator name matches the ManagedCluster name and lives in the same namespace
	fmt.Printf("Listing ManagedClusters with test ID: %s\n", cfg.TestID)
	mcList, err := client.GetManagedClustersByLabel(ctx, fmt.Sprintf("%s=%s", constants.LabelInfraTestID, cfg.TestID))
	if err != nil {
		return fmt.Errorf("failed to list ManagedClusters: %w", err)
	}

	if len(mcList.Items) == 0 {
		fmt.Printf("No ManagedClusters found with test ID: %s\n", cfg.TestID)
		return nil
	}

	fmt.Printf("Found %d ManagedClusters to reset\n\n", len(mcList.Items))

	// Reset each ClusterCurator (found by ManagedCluster name)
	bar := progress.New(len(mcList.Items), "Resetting ClusterCurators")
	var resetCount int32
	var errorCount int32
	var wg sync.WaitGroup
	var errorsMu sync.Mutex
	var errors []error

	for _, mc := range mcList.Items {
		wg.Add(1)
		go func(managedCluster unstructured.Unstructured) {
			defer wg.Done()

			// ClusterCurator name == ManagedCluster name, and lives in namespace == ManagedCluster name
			clusterName := managedCluster.GetName()
			baseVersion := clusterVersions[clusterName]

			if baseVersion == "" {
				errorsMu.Lock()
				errors = append(errors, fmt.Errorf("no base version found for cluster %s", clusterName))
				errorsMu.Unlock()
				atomic.AddInt32(&errorCount, 1)
				bar.Update(int(atomic.AddInt32(&resetCount, 1)))
				return
			}

			// Update ManagedCluster version labels
			mcLabels := managedCluster.GetLabels()
			if mcLabels == nil {
				mcLabels = make(map[string]string)
			}
			for k, v := range constants.ParseVersionLabels(baseVersion) {
				mcLabels[k] = v
			}
			managedCluster.SetLabels(mcLabels)

			if err := client.UpdateManagedCluster(ctx, &managedCluster); err != nil {
				errorsMu.Lock()
				errors = append(errors, fmt.Errorf("failed to update ManagedCluster %s: %w", clusterName, err))
				errorsMu.Unlock()
				atomic.AddInt32(&errorCount, 1)
				bar.Update(int(atomic.AddInt32(&resetCount, 1)))
				return
			}

			// Get the ClusterCurator with the same name as the ManagedCluster
			// in the namespace with the same name as the ManagedCluster
			cc, err := client.GetClusterCurator(ctx, clusterName, clusterName)
			if err != nil {
				// ClusterCurator might not exist yet - that's OK, nothing to reset
				bar.Update(int(atomic.AddInt32(&resetCount, 1)))
				return
			}

			// Reset the ClusterCurator for CCR controller restart
			updated := cc.DeepCopy()

			// Remove the managed-curator label so CCR controller can re-claim it
			labels := updated.GetLabels()
			if labels == nil {
				labels = make(map[string]string)
			}
			delete(labels, "rollout.open-cluster-management.io/managed-curator")
			delete(labels, "simulator.open-cluster-management.io/current-version")
			updated.SetLabels(labels)

			// Clear spec - remove desiredCuration and upgrade.desiredUpdate
			// Keep standard timeout settings only
			updated.Object["spec"] = map[string]interface{}{
				"install": map[string]interface{}{
					"jobMonitorTimeout": int64(5),
				},
				"upgrade": map[string]interface{}{
					"monitorTimeout": int64(120),
				},
				"scale": map[string]interface{}{
					"jobMonitorTimeout": int64(5),
				},
				"destroy": map[string]interface{}{
					"jobMonitorTimeout": int64(5),
				},
			}

			// Clear status conditions entirely so CCR controller sees clean state
			updated.Object["status"] = map[string]interface{}{}

			if err := client.UpdateResource(ctx, updated); err != nil {
				errorsMu.Lock()
				errors = append(errors, fmt.Errorf("failed to reset ClusterCurator %s: %w", clusterName, err))
				errorsMu.Unlock()
				atomic.AddInt32(&errorCount, 1)
			}

			bar.Update(int(atomic.AddInt32(&resetCount, 1)))
		}(mc)
	}

	wg.Wait()
	bar.Complete(fmt.Sprintf("Reset %d clusters", len(mcList.Items)-int(errorCount)))

	if len(errors) > 0 {
		fmt.Printf("\n⚠️  Encountered %d errors during reset:\n", len(errors))
		displayErrors := 5
		if len(errors) < displayErrors {
			displayErrors = len(errors)
		}
		for i := 0; i < displayErrors; i++ {
			fmt.Printf("  - %v\n", errors[i])
		}
		if len(errors) > displayErrors {
			fmt.Printf("  ... and %d more errors\n", len(errors)-displayErrors)
		}
		return fmt.Errorf("reset completed with %d errors", len(errors))
	}

	fmt.Printf("\n✓ Successfully reset %d clusters to base versions\n", len(mcList.Items))
	return nil
}
