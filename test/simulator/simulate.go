package main

import (
	"fmt"
	"os"

	"github.com/jpacker/cluster-curator-synthetic-test/cmd"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "simulate",
	Short: "Simulation Environment Manager",
	Long: `Simulation Environment Manager - Manage synthetic test infrastructure for ClusterCuratorRollout testing.
		
Initializes, tracks, and removes Namespace, ManagedCluster, and ClusterCurator resources
based on hierarchical label configurations.`,
}

func init() {
	rootCmd.AddCommand(cmd.InitCmd)
	rootCmd.AddCommand(cmd.RemoveCmd)
	rootCmd.AddCommand(cmd.QueryCmd)
	rootCmd.AddCommand(cmd.RunCmd)
	rootCmd.AddCommand(cmd.ResetCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

