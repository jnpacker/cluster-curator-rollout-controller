package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/config"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/generator"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/kubernetes"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/progress"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var (
	initDryRun     bool
	initKubeconfig string
	initConfigID   string
	initConfig     string
)

// InitCmd represents the init command
var InitCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize test infrastructure",
	Long: `Initializes per-cluster Namespace and ManagedCluster resources based on configuration.

ClusterCurator resources are NOT created by this command - they should be created by the
ClusterCuratorRollout controller when an upgrade is initiated.

When --config-id is provided, the command resumes an incomplete deployment using that ID,
overriding any test ID in the config file.`,
	RunE: runInit,
}

func init() {
	InitCmd.Flags().BoolVar(&initDryRun, "dry-run", false, "Preview changes without applying")
	InitCmd.Flags().StringVar(&initKubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	InitCmd.Flags().StringVar(&initConfigID, "config-id", "", "Resume rollout with existing config-id (overrides config file)")
	InitCmd.Flags().StringVar(&initConfig, "config", "", "Path to config file (can also use SIMULATE_CONFIG env var)")
}

func runInit(cmd *cobra.Command, args []string) error {
	// Resolve config file: flag > env var > positional arg (for backwards compatibility)
	configFile := initConfig
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

	// Handle resume mode vs fresh deployment
	if initConfigID != "" {
		// Resume mode: use the specified config ID (overrides config file)
		fmt.Printf("\n=== RESUME MODE ===\n")
		fmt.Printf("Resuming rollout with config-id: %s\n", initConfigID)
		cfg.TestID = initConfigID
	} else {
		// Fresh deployment: always generate a NEW test ID
		newTestID, err := config.GenerateNewTestID(configFile)
		if err != nil {
			return fmt.Errorf("failed to generate new test ID: %w", err)
		}
		cfg.TestID = newTestID
	}

	// Print label key hierarchy tree
	fmt.Print(cfg.PrintLabelKeyTree())

	// Distribute labels and versions
	fmt.Printf("Generating %d synthetic clusters...\n", cfg.TotalClusters)
	distributor := generator.NewDistributor(cfg.TotalClusters)
	if err := distributor.Distribute(cfg.Labels); err != nil {
		return err
	}
	distributor.DistributeVersions(cfg.BaseVersions)

	clusters := distributor.GetClusters()

	// Display summary with test ID prominently
	separator := "======================================================================"
	fmt.Printf("\n%s\n", separator)
	fmt.Printf("SIMULATION ENVIRONMENT ID (Config ID): %s\n", cfg.TestID)
	if initConfigID != "" {
		fmt.Printf("Mode: RESUME (continuing incomplete rollout)\n")
	} else {
		fmt.Printf("Mode: NEW (fresh rollout)\n")
	}
	fmt.Printf("Use this ID to manage/cleanup resources later\n")
	fmt.Printf("%s\n", separator)
	fmt.Printf("\nCluster Label Distribution:\n")

	// For dry-run, show full output; otherwise show abbreviated
	if initDryRun {
		for _, cluster := range clusters {
			fmt.Printf("  %s: %v\n", cluster.Name, cluster.Labels)
		}
	} else {
		for i, cluster := range clusters {
			fmt.Printf("  %s: %v\n", cluster.Name, cluster.Labels)
			if i >= 2 {
				fmt.Printf("  ... (%d more clusters)\n", len(clusters)-3)
				break
			}
		}
	}

	if initDryRun {
		fmt.Println("\n[DRY RUN] Would create the above resources")
		return nil
	}

	// Save test ID to config file immediately (before creating any resources)
	// This ensures we don't lose track of the ID even if creation fails
	if initConfigID == "" {
		fmt.Printf("\nSaving test ID to config file...\n")
		config.AddTestID(cfg, cfg.TestID)
		if err := config.SaveConfig(configFile, cfg); err != nil {
			fmt.Printf("Warning: Failed to save test ID to config file: %v\n", err)
		} else {
			fmt.Printf("✓ Saved test ID %s to config file\n", cfg.TestID)
		}
	}

	// Create Kubernetes client
	fmt.Printf("\nConnecting to Kubernetes cluster...\n")
	client, err := kubernetes.NewClient(initKubeconfig)
	if err != nil {
		return err
	}

	// Build CRs
	crBuilder := generator.NewCRBuilder(cfg.TestID)
	crSets := make([]*generator.CRSet, len(clusters))
	for i, cluster := range clusters {
		crSets[i] = crBuilder.BuildCRSet(&cluster)
	}

	ctx := context.Background()
	total := len(crSets)
	var errors []error
	var errorsMu sync.Mutex

	// Phase 1: Create Namespaces
	fmt.Printf("\nCreating Namespaces...\n")
	nsBar := progress.New(total, "Creating Namespaces")
	var nsCount int32
	var nsSkipped int32
	var nsWg sync.WaitGroup
	for _, crSet := range crSets {
		nsWg.Add(1)
		go func(ns *corev1.Namespace) {
			defer nsWg.Done()
			if err := client.CreateNamespace(ctx, ns); err != nil {
				if k8serrors.IsAlreadyExists(err) {
					// Namespace exists - update its labels to ensure infra-test-id is set
					if updateErr := client.UpdateNamespaceLabels(ctx, ns.Name, ns.Labels); updateErr != nil {
						errorsMu.Lock()
						errDetail := formatK8sError("Namespace (update labels)", ns.Name, updateErr)
						errors = append(errors, errDetail)
						errorsMu.Unlock()
					}
					atomic.AddInt32(&nsSkipped, 1)
				} else {
					errorsMu.Lock()
					errDetail := formatK8sError("Namespace", ns.Name, err)
					errors = append(errors, errDetail)
					errorsMu.Unlock()
				}
			}
			nsBar.Update(int(atomic.AddInt32(&nsCount, 1)))
		}(crSet.Namespace)
	}
	nsWg.Wait()
	nsCreated := total - len(errors) - int(nsSkipped)
	if nsSkipped > 0 {
		nsBar.Complete(fmt.Sprintf("Created %d Namespaces (%d already existed, labels updated)", nsCreated, nsSkipped))
	} else {
		nsBar.Complete(fmt.Sprintf("Created %d Namespaces", nsCreated))
	}

	// Phase 2: Create ManagedClusters
	fmt.Printf("\nCreating ManagedClusters...\n")
	mcBar := progress.New(total, "Creating ManagedClusters")
	var mcCount int32
	var mcSkipped int32
	mcErrorsBefore := len(errors)
	var mcWg sync.WaitGroup
	for _, crSet := range crSets {
		mcWg.Add(1)
		go func(mc *unstructured.Unstructured) {
			defer mcWg.Done()
			if err := client.CreateResource(ctx, mc); err != nil {
				if k8serrors.IsAlreadyExists(err) {
					atomic.AddInt32(&mcSkipped, 1)
				} else {
					errorsMu.Lock()
					errDetail := formatK8sError("ManagedCluster", mc.GetName(), err)
					errors = append(errors, errDetail)
					errorsMu.Unlock()
				}
			}
			mcBar.Update(int(atomic.AddInt32(&mcCount, 1)))
		}(crSet.ManagedCluster)
	}
	mcWg.Wait()
	mcCreated := total - (len(errors) - mcErrorsBefore) - int(mcSkipped)
	if mcSkipped > 0 {
		mcBar.Complete(fmt.Sprintf("Created %d ManagedClusters (%d already existed)", mcCreated, mcSkipped))
	} else {
		mcBar.Complete(fmt.Sprintf("Created %d ManagedClusters", mcCreated))
	}

	// Note: ClusterCurators are NOT created here - they should be created by the
	// ClusterCuratorRollout controller when an upgrade is initiated

	if len(errors) > 0 {
		// Write errors to log file with detailed info
		logFileName := fmt.Sprintf("simulate-errors-%s.log", time.Now().Format("20060102-150405"))
		logFile, err := os.Create(logFileName)
		if err != nil {
			fmt.Printf("\n⚠️  Failed to create error log file: %v\n", err)
		} else {
			defer logFile.Close()
			fmt.Fprintf(logFile, "=== Simulate Init Error Log ===\n")
			fmt.Fprintf(logFile, "Test ID: %s\n", cfg.TestID)
			fmt.Fprintf(logFile, "Config File: %s\n", configFile)
			fmt.Fprintf(logFile, "Total Clusters: %d\n", total)
			fmt.Fprintf(logFile, "Total Errors: %d\n", len(errors))
			fmt.Fprintf(logFile, "Timestamp: %s\n", time.Now().Format(time.RFC3339))
			fmt.Fprintf(logFile, "\n=== Error Details ===\n\n")
			for i, e := range errors {
				fmt.Fprintf(logFile, "[%d] %+v\n\n", i+1, e)
			}
			fmt.Fprintf(logFile, "\n=== Debug Info ===\n")
			fmt.Fprintf(logFile, "Kubeconfig: %s\n", initKubeconfig)
			fmt.Fprintf(logFile, "Resume Command: ./simulate init --config %s --config-id %s\n", configFile, cfg.TestID)
		}

		fmt.Printf("\n⚠️  Encountered %d errors during creation (see %s for details)\n", len(errors), logFileName)
		fmt.Printf("\n⚠️  RESUME AVAILABLE: Run the following to continue:\n")
		fmt.Printf("  ./simulate init --config %s --config-id %s\n", configFile, cfg.TestID)
		return fmt.Errorf("creation completed with errors - use --config-id flag to resume")
	}

	fmt.Printf("\n✓ Successfully created all resources for %d clusters\n", len(clusters))

	// Note: Test ID was already saved to config file before creation started
	if initConfigID == "" {
		fmt.Printf("✓ Test ID %s is tracked in config file\n", cfg.TestID)
	} else {
		fmt.Printf("✓ Resumed rollout completed successfully for test ID: %s\n", cfg.TestID)
	}

	return nil
}

// formatK8sError extracts detailed information from Kubernetes API errors
func formatK8sError(resourceType, resourceName string, err error) error {
	if statusErr, ok := err.(*k8serrors.StatusError); ok {
		status := statusErr.ErrStatus
		return fmt.Errorf("%s %s: [%s] %s (Reason: %s, Code: %d)",
			resourceType,
			resourceName,
			status.Status,
			status.Message,
			status.Reason,
			status.Code)
	}
	return fmt.Errorf("%s %s: %v", resourceType, resourceName, err)
}
