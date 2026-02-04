package cmd

import (
	"context"
	"fmt"
	"sync"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/config"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/generator"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/kubernetes"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var (
	dryRun     bool
	kubeconfig string
)

// CreateCmd represents the create command
var CreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create synthetic test infrastructure",
	Long:  "Creates per-cluster Namespace, ManagedCluster, and ClusterCurator CRs based on configuration",
	RunE:  runCreate,
}

func init() {
	CreateCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview changes without applying")
	CreateCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
}

func runCreate(cmd *cobra.Command, args []string) error {
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

	// Distribute labels
	fmt.Printf("Generating %d synthetic clusters...\n", cfg.TotalClusters)
	distributor := generator.NewDistributor(cfg.TotalClusters)
	if err := distributor.Distribute(cfg.Labels); err != nil {
		return err
	}

	clusters := distributor.GetClusters()

	// Display summary with test ID
	fmt.Printf("\nSynthetic Test ID: %s\n", cfg.TestID)
	fmt.Printf("\nCluster Label Distribution:\n")
	
	// For dry-run, show full output; otherwise show abbreviated
	if dryRun {
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

	if dryRun {
		fmt.Println("\n[DRY RUN] Would create the above resources")
		return nil
	}

	// Create Kubernetes client
	fmt.Printf("\nConnecting to Kubernetes cluster...\n")
	client, err := kubernetes.NewClient(kubeconfig)
	if err != nil {
		return err
	}

	// Build CRs
	crBuilder := generator.NewCRBuilder(cfg.TestID)
	crSets := make([]*generator.CRSet, len(clusters))
	for i, cluster := range clusters {
		crSets[i] = crBuilder.BuildCRSet(&cluster)
	}

	// Create resources with parallelism
	ctx := context.Background()
	errChan := make(chan error, len(crSets)*3)
	wg := sync.WaitGroup{}

	fmt.Printf("Creating resources...\n")

	for _, crSet := range crSets {
		wg.Add(3)

		// Create namespace
		go func(ns *corev1.Namespace) {
			defer wg.Done()
			if err := client.CreateNamespace(ctx, ns); err != nil {
				errChan <- fmt.Errorf("failed to create namespace %s: %w", ns.Name, err)
			} else {
				fmt.Printf("✓ Created Namespace: %s\n", ns.Name)
			}
		}(crSet.Namespace)

		// Create ManagedCluster
		go func(mc *unstructured.Unstructured) {
			defer wg.Done()
			if err := client.CreateResource(ctx, mc); err != nil {
				errChan <- fmt.Errorf("failed to create ManagedCluster %s: %w", mc.GetName(), err)
			} else {
				fmt.Printf("✓ Created ManagedCluster: %s\n", mc.GetName())
			}
		}(crSet.ManagedCluster)

		// Create ClusterCurator
		go func(cc *unstructured.Unstructured) {
			defer wg.Done()
			if err := client.CreateResource(ctx, cc); err != nil {
				errChan <- fmt.Errorf("failed to create ClusterCurator %s: %w", cc.GetName(), err)
			} else {
				fmt.Printf("✓ Created ClusterCurator: %s\n", cc.GetName())
			}
		}(crSet.ClusterCurator)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		fmt.Printf("\nEncountered %d errors:\n", len(errors))
		for _, err := range errors {
			fmt.Printf("  ✗ %v\n", err)
		}
		return fmt.Errorf("creation completed with errors")
	}

	fmt.Printf("\n✓ Successfully created all resources for %d synthetic clusters\n", len(clusters))
	return nil
}

