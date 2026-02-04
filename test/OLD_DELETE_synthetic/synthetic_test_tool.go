package main

import (
	"fmt"
	"os"

	"github.com/jpacker/cluster-curator-synthetic-test/cmd"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "synthetic-test-tool",
	Short: "Synthetic ClusterCuratorRollout Test Infrastructure",
	Long: `A tool to generate synthetic test infrastructure for ClusterCuratorRollout testing.
		
Creates and manages Namespace, ManagedCluster, and ClusterCurator resources
based on hierarchical label configurations.`,
}

func init() {
	rootCmd.AddCommand(cmd.CreateCmd)
	rootCmd.AddCommand(cmd.RemoveCmd)
	rootCmd.AddCommand(cmd.QueryCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

