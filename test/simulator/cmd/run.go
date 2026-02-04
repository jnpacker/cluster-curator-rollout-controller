package cmd

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/config"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/constants"
	"github.com/jpacker/cluster-curator-synthetic-test/pkg/kubernetes"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	runKubeconfig       string
	runConfigFile       string
	upgradeTimeSeconds  int
	randomFactorPercent float64
	upliftPercent       float64
	displayInterval     int
	pollInterval        int
	logFilePath         string
	failurePercent      int
)

// RunCmd represents the run command
var RunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the upgrade simulator",
	Long: `Run the upgrade simulator to watch ClusterCurator resources and simulate upgrades.

The simulator watches for ClusterCurator resources with spec.desiredCuration set to "upgrade",
simulates the upgrade duration based on configuration, then marks the ClusterCurator as upgraded.

Configuration can be specified via:
  1. --config flag
  2. SIMULATE_CONFIG environment variable
  3. Positional argument (deprecated)

Example:
  ./simulate run --config config/simple_clusters.yaml
  ./simulate run --config config/simple_clusters.yaml --upgrade-time 60 --uplift 15
  SIMULATE_CONFIG=config/infra_clusters.json ./simulate run`,
	RunE: runSimulator,
}

func init() {
	RunCmd.Flags().StringVar(&runConfigFile, "config", "", "Path to config file (can also use SIMULATE_CONFIG env var)")
	RunCmd.Flags().StringVar(&runKubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	RunCmd.Flags().IntVar(&upgradeTimeSeconds, "upgrade-time", 0, "Base time for upgrade simulation in seconds (overrides config file)")
	RunCmd.Flags().Float64Var(&randomFactorPercent, "random-factor", 25.0, "Random variance percentage (0-100)")
	RunCmd.Flags().Float64Var(&upliftPercent, "uplift", 10.0, "Fixed uplift percentage added to upgrade time")
	RunCmd.Flags().IntVar(&displayInterval, "display-interval", 5, "Display metrics every N seconds")
	RunCmd.Flags().IntVar(&pollInterval, "poll-interval", 10, "Poll ClusterCurators every N seconds")
	RunCmd.Flags().StringVar(&logFilePath, "log-file", "simulator.log", "Path to log file for watch events and polls")
	RunCmd.Flags().IntVar(&failurePercent, "failure-percent", 0, "Percentage of upgrades that should randomly fail (0-100)")
}


// UpgradeCompletion represents a scheduled upgrade completion
type UpgradeCompletion struct {
	ClusterName   string
	TargetVersion string
	Failed        bool // true if this upgrade should be marked as failed
}

// SimulatorState holds the state of the upgrade simulator
type SimulatorState struct {
	client            *kubernetes.Client
	testID            string
	startTime         time.Time
	metrics           *ClusterMetrics
	failurePercentage int // Percentage of upgrades that should fail (0-100)
	configuredTotal   int // Total clusters expected from config file

	// Allowed label keys from config (for grouping)
	labelKeys map[string]bool

	// Target version from ClusterCuratorRollout (for skip detection)
	targetVersionMu sync.RWMutex
	targetVersion   string

	// Observed version from ClusterCuratorRollout status (to detect resets)
	observedVersion string

	// CCR phase tracking (Processing, Soaking, etc.)
	ccrPhase          string
	ccrActiveStep     string
	ccrSoakEndsAt     string
	ccrFailedClusters []string // Names of clusters that failed (from CCR status.failedClusters)

	// Track ClusterCurators by namespace/name -> state
	curatorsMu sync.RWMutex
	curators   map[string]*CuratorState

	// Track clusters in progress
	inProgressMu sync.RWMutex
	inProgress   map[string]bool

	// Channel for upgrade completions
	completionCh chan UpgradeCompletion

	// Log file for watch events and polls
	logFile   *os.File
	logFileMu sync.Mutex
}

// CuratorState tracks the state of a ClusterCurator
type CuratorState struct {
	Namespace         string
	Name              string
	DesiredCuration   string
	DesiredVersion    string
	CurrentVersion    string
	State             string // constants.StateIdle, StateUpgrading, StateCompleted
	LastObservedState string
}

// ClusterMetrics holds metrics grouped by labels
type ClusterMetrics struct {
	mu     sync.RWMutex
	groups map[string]*ClusterGroup
}

// ClusterGroup represents clusters with the same label set
type ClusterGroup struct {
	Labels              map[string]string
	Total               int
	Pending             int // Not at target version
	InProgress          int // Currently upgrading
	Completed           int // At target version
	Failed              int
	Skipped             int // Already at target version (no upgrade needed)
	upgradingClustersMu sync.Mutex
	upgradingClusters   map[string]*UpgradeState
	skippedClusters     map[string]bool // Track which clusters are skipped
}

// UpgradeState tracks an in-progress upgrade
type UpgradeState struct {
	ClusterName   string
	StartTime     time.Time
	EstimatedEnd  time.Time
	TargetVersion string
}

func runSimulator(cmd *cobra.Command, args []string) error {
	// Determine config file: --config flag > SIMULATE_CONFIG env > positional arg
	configFile := runConfigFile
	if configFile == "" {
		configFile = os.Getenv("SIMULATE_CONFIG")
	}
	if configFile == "" && len(args) > 0 {
		configFile = args[0]
	}
	if configFile == "" {
		return fmt.Errorf("config file required: use --config flag, SIMULATE_CONFIG env var, or positional argument")
	}

	// Load configuration
	fmt.Printf("Loading configuration from %s...\n", configFile)
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if cfg.TestID == "" {
		return fmt.Errorf("no active test ID found - run 'simulate init' first to create test infrastructure")
	}

	// Determine upgrade time: command-line flag overrides config file, default to 300 if neither set
	effectiveUpgradeTime := upgradeTimeSeconds
	if effectiveUpgradeTime == 0 {
		if cfg.UpgradeTimeSeconds > 0 {
			effectiveUpgradeTime = cfg.UpgradeTimeSeconds
		} else {
			effectiveUpgradeTime = 300 // default
		}
	}
	upgradeTimeSeconds = effectiveUpgradeTime

	// Determine failure percentage: command-line flag overrides config file
	if failurePercent > 0 {
		cfg.FailurePercentage = failurePercent
	}

	fmt.Printf("Using test ID: %s\n", cfg.TestID)
	fmt.Printf("Upgrade time: %ds (±%.0f%% variance, +%.0f%% uplift)\n",
		upgradeTimeSeconds, randomFactorPercent, upliftPercent)
	if cfg.FailurePercentage > 0 {
		fmt.Printf("Failure simulation: %d%% of upgrades will fail\n", cfg.FailurePercentage)
	}

	// Create Kubernetes client
	fmt.Printf("Connecting to Kubernetes cluster...\n")
	client, err := kubernetes.NewClient(runKubeconfig)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Build label keys map from config
	labelKeys := make(map[string]bool)
	for _, key := range cfg.GetLabelKeys() {
		labelKeys[key] = true
	}

	// Open log file
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file %s: %w", logFilePath, err)
	}
	defer logFile.Close()

	// Initialize state
	state := &SimulatorState{
		client:            client,
		testID:            cfg.TestID,
		startTime:         time.Now(),
		labelKeys:         labelKeys,
		failurePercentage: cfg.FailurePercentage,
		configuredTotal:   cfg.TotalClusters,
		curators:          make(map[string]*CuratorState),
		inProgress:        make(map[string]bool),
		completionCh:      make(chan UpgradeCompletion, 100),
		logFile:           logFile,
		metrics: &ClusterMetrics{
			groups: make(map[string]*ClusterGroup),
		},
	}

	// Log startup configuration
	state.log("CONFIG", "Loading configuration from %s", configFile)
	state.log("CONFIG", "Using test ID: %s", cfg.TestID)
	state.log("CONFIG", "Upgrade time: %ds (±%.0f%% variance, +%.0f%% uplift)", upgradeTimeSeconds, randomFactorPercent, upliftPercent)
	if cfg.FailurePercentage > 0 {
		state.log("CONFIG", "Failure simulation: %d%% of upgrades will fail", cfg.FailurePercentage)
	}
	state.log("CONFIG", "Connecting to Kubernetes cluster...")
	state.log("STARTUP", "Simulator started with test ID: %s, poll interval: %ds", cfg.TestID, pollInterval)

	ctx := context.Background()

	// Fetch CCR target version BEFORE initializing metrics
	// This is required for correct completion detection - we need to know if
	// a ClusterCurator's desiredUpdate matches the current CCR target
	state.fetchTargetVersionFromRollout(ctx)

	// Initialize metrics by listing existing ClusterCurators
	if err := state.initializeMetrics(ctx); err != nil {
		return fmt.Errorf("failed to initialize metrics: %w", err)
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nReceived shutdown signal, stopping...")
		cancel()
	}()

	// Start background goroutines
	var wg sync.WaitGroup

	// Periodic poll for ClusterCurator changes
	wg.Add(1)
	go func() {
		defer wg.Done()
		state.runPeriodicPoll(ctx)
	}()

	// Periodic check for upgrades ready to complete (based on annotations)
	wg.Add(1)
	go func() {
		defer wg.Done()
		state.runUpgradeCompletionCheck(ctx)
	}()

	// Periodic check for skipped clusters (already at target version)
	wg.Add(1)
	go func() {
		defer wg.Done()
		state.runSkippedClusterCheck(ctx)
	}()

	// Upgrade completion handler - processes scheduled completions
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case completion := <-state.completionCh:
				state.handleUpgradeCompletion(ctx, completion)
			}
		}
	}()

	// Display metrics loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Show initial display
		state.displayMetrics()

		ticker := time.NewTicker(time.Duration(displayInterval) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state.displayMetrics()
			}
		}
	}()

	fmt.Printf("\n✓ Simulator running (Ctrl+C to stop)\n")
	fmt.Printf("  Polling for ClusterCurators with test-id: %s\n", cfg.TestID)
	fmt.Printf("  Display updates every %d seconds\n", displayInterval)
	fmt.Printf("  Poll interval: %d seconds\n", pollInterval)
	fmt.Printf("  Log file: %s\n\n", logFilePath)

	// Wait for shutdown
	<-ctx.Done()
	wg.Wait()

	fmt.Println("Simulator stopped")
	return nil
}

// initializeMetrics initializes metrics by listing ManagedClusters with our test ID.
// We use ManagedClusters (not ClusterCurators) because clusters exist before upgrades are requested,
// and ClusterCurators created by the controller don't have the infra-test-id label.
func (s *SimulatorState) initializeMetrics(ctx context.Context) error {
	labelSelector := fmt.Sprintf("%s=%s", constants.LabelInfraTestID, s.testID)
	managedClusters, err := s.client.GetManagedClustersByLabel(ctx, labelSelector)
	if err != nil {
		return fmt.Errorf("failed to list ManagedClusters: %w", err)
	}

	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	for _, mc := range managedClusters.Items {
		clusterName := mc.GetName()

		// Get labels from the namespace (which should have the cluster labels)
		ns, err := s.client.GetNamespace(ctx, clusterName)
		if err != nil {
			continue
		}

		labels := s.filterGroupingLabels(ns.GetLabels())
		labelKey := labelsToKey(labels)

		group, exists := s.metrics.groups[labelKey]
		if !exists {
			group = &ClusterGroup{
				Labels:            labels,
				upgradingClusters: make(map[string]*UpgradeState),
				skippedClusters:   make(map[string]bool),
			}
			s.metrics.groups[labelKey] = group
		}

		group.Total++

		// Get current version from ManagedCluster labels
		mcLabels := mc.GetLabels()
		currentVersion := mcLabels[constants.LabelOpenShiftVersion]

		// Check if there's a ClusterCurator for this cluster
		curator, err := s.client.GetClusterCurator(ctx, clusterName, clusterName)
		desiredCuration := ""
		initialState := constants.StateIdle
		isCompleted := false
		isFailed := false

		if err == nil {
			desiredCuration, _, _ = unstructured.NestedString(curator.Object, "spec", "desiredCuration")

			// Get the ClusterCurator's target version for completion verification
			curatorTargetVersion, _, _ := unstructured.NestedString(curator.Object, "spec", "upgrade", "desiredUpdate")

			// Get CCR's target version to ensure we only mark as Completed if
			// the ClusterCurator is for the current rollout
			s.targetVersionMu.RLock()
			ccrTargetVersion := s.targetVersion
			s.targetVersionMu.RUnlock()

			// Check if this curator has already completed or failed an upgrade by looking at its status conditions
			conditions, _, _ := unstructured.NestedSlice(curator.Object, "status", "conditions")
			if conditions != nil {
				for _, condInterface := range conditions {
					if cond, ok := condInterface.(map[string]interface{}); ok {
						condType, ok := cond["type"].(string)
						status, ok2 := cond["status"].(string)
						reason, ok3 := cond["reason"].(string)
						if !ok || !ok2 || !ok3 {
							continue
						}

						// Check for failures first - any Job_failed means the upgrade failed
						if reason == "Job_failed" {
							isFailed = true
							initialState = constants.StateFailed
							break
						}

						// Check for successful completion
						if condType == constants.ConditionCuratorJob &&
							status == "True" &&
							reason == "Job_has_finished" {
							// ClusterCurator shows completion, but we need to verify:
							// 1. The curator's desiredUpdate matches the CCR's target version
							// 2. The actual cluster version is at or above the target
							// If either check fails, the completion is stale (from a previous rollout)

							// First check: curator's target must match CCR's target
							if ccrTargetVersion != "" && curatorTargetVersion != "" && curatorTargetVersion != ccrTargetVersion {
								// ClusterCurator is for a different version - treat as Pending
								break
							}

							// Second check: cluster version must be at or above target
							if curatorTargetVersion != "" && currentVersion != "" {
								if compareVersions(currentVersion, curatorTargetVersion) >= 0 {
									// Cluster is at or above the curator's target - truly completed
									isCompleted = true
									initialState = constants.StateCompleted
								}
								// If cluster version < curator target, completion is stale
								// (from a previous upgrade), treat as pending
							} else if ccrTargetVersion == "" {
								// No CCR target to verify against, trust the completion status
								isCompleted = true
								initialState = constants.StateCompleted
							}
							break
						}
					}
				}
			}
		}

		// Track curator state
		curatorKey := fmt.Sprintf("%s/%s", clusterName, clusterName)
		s.curators[curatorKey] = &CuratorState{
			Namespace:         clusterName,
			Name:              clusterName,
			DesiredCuration:   desiredCuration,
			CurrentVersion:    currentVersion,
			State:             initialState,
			LastObservedState: desiredCuration, // Set to current so we don't re-trigger completed upgrades
		}

		// Update metrics based on completion/failure status
		// Note: Skip detection is handled separately by checkSkippedClusters which runs periodically
		// and uses the CCR's target version. Here we only track completed vs failed vs pending based on
		// what the ClusterCurator shows, allowing comparison between simulator and controller.
		if isFailed {
			group.Failed++
		} else if isCompleted {
			group.Completed++
		} else {
			// Only count as Pending if not completed or failed
			group.Pending++
		}
	}

	fmt.Printf("Initialized metrics: %d label groups, %d total clusters\n",
		len(s.metrics.groups), s.getTotalClusters())

	return nil
}

func (s *SimulatorState) getTotalClusters() int {
	total := 0
	for _, g := range s.metrics.groups {
		total += g.Total
	}
	return total
}

// startUpgradeSimulation starts tracking an upgrade and schedules completion
func (s *SimulatorState) startUpgradeSimulation(ctx context.Context, namespace, name, targetVersion string) {
	clusterName := namespace

	// Check and set inProgress atomically
	s.inProgressMu.Lock()
	if s.inProgress[clusterName] {
		s.inProgressMu.Unlock()
		return
	}
	s.inProgress[clusterName] = true
	s.inProgressMu.Unlock()

	// Get labels for this cluster
	ns, err := s.client.GetNamespace(ctx, namespace)
	if err != nil {
		s.log("WARNING", "Could not get namespace %s: %v", namespace, err)
		return
	}

	labels := s.filterGroupingLabels(ns.GetLabels())
	labelKey := labelsToKey(labels)

	upgradeTime := calculateUpgradeTime()

	// Determine if this upgrade should fail based on failure percentage
	willFail := s.failurePercentage > 0 && rand.Intn(100) < s.failurePercentage

	if willFail {
		s.log("UPGRADE_START", "Starting upgrade (will FAIL): %s → %s (duration: %ds)", clusterName, targetVersion, upgradeTime)
	} else {
		s.log("UPGRADE_START", "Starting upgrade: %s → %s (duration: %ds)", clusterName, targetVersion, upgradeTime)
	}

	// Update ClusterCurator status to show upgrade is in progress
	// This stores the completion time and failure flag in annotations
	if err := s.markClusterCuratorUpgrading(ctx, clusterName, targetVersion, upgradeTime, willFail); err != nil {
		s.log("UPGRADE_WARNING", "Failed to mark %s as upgrading: %v", clusterName, err)
	}

	// Add to metrics
	s.metrics.mu.Lock()
	group, exists := s.metrics.groups[labelKey]
	if !exists {
		// Create group if it doesn't exist yet (shouldn't happen, but be safe)
		group = &ClusterGroup{
			Labels:            labels,
			upgradingClusters: make(map[string]*UpgradeState),
			skippedClusters:   make(map[string]bool),
			Total:             1,
			Pending:           1,
		}
		s.metrics.groups[labelKey] = group
	}

	group.upgradingClustersMu.Lock()
	group.upgradingClusters[clusterName] = &UpgradeState{
		ClusterName:   clusterName,
		StartTime:     time.Now(),
		EstimatedEnd:  time.Now().Add(time.Duration(upgradeTime) * time.Second),
		TargetVersion: targetVersion,
	}
	group.Pending--
	group.InProgress++
	group.upgradingClustersMu.Unlock()
	s.metrics.mu.Unlock()

	// Note: Upgrade completion is now handled by checkUpgradeCompletions which polls
	// ClusterCurators and checks the simulator.test/upgrade-complete-at annotation
}

// handleUpgradeCompletion processes a completed upgrade
func (s *SimulatorState) handleUpgradeCompletion(ctx context.Context, completion UpgradeCompletion) {
	clusterName := completion.ClusterName
	targetVersion := completion.TargetVersion

	// Set curator state FIRST to prevent race condition with pollClusterCurators
	// If poll runs concurrently, it will see the state as already completed/failed and skip
	curatorKey := fmt.Sprintf("%s/%s", clusterName, clusterName)
	s.curatorsMu.Lock()
	curatorState, exists := s.curators[curatorKey]
	if exists {
		// Check if already processed (by poll or previous completion)
		if curatorState.State == constants.StateCompleted || curatorState.State == constants.StateFailed {
			s.curatorsMu.Unlock()
			s.log("COMPLETION_SKIP", "Cluster %s already in terminal state %s, skipping", clusterName, curatorState.State)
			return
		}
		if completion.Failed {
			curatorState.State = constants.StateFailed
		} else {
			curatorState.State = constants.StateCompleted
		}
		curatorState.LastObservedState = ""
	}
	s.curatorsMu.Unlock()

	if completion.Failed {
		// Mark ClusterCurator as failed
		if err := s.markClusterCuratorFailed(ctx, clusterName, targetVersion); err != nil {
			s.log("UPGRADE_ERROR", "Failed to mark %s as failed: %v", clusterName, err)
			return
		}
		s.log("UPGRADE_FAILED", "Upgrade failed: %s → %s", clusterName, targetVersion)
	} else {
		// Mark ClusterCurator as upgraded
		if err := s.markClusterCuratorUpgraded(ctx, clusterName, targetVersion); err != nil {
			s.log("UPGRADE_ERROR", "Failed to mark %s as upgraded: %v", clusterName, err)
			return
		}
		s.log("UPGRADE_COMPLETE", "Upgrade complete: %s → %s", clusterName, targetVersion)
	}

	// Update metrics - find the group for this cluster
	ns, err := s.client.GetNamespace(ctx, clusterName)
	if err != nil {
		s.log("WARNING", "Could not get namespace %s: %v", clusterName, err)
		return
	}

	labels := s.filterGroupingLabels(ns.GetLabels())
	labelKey := labelsToKey(labels)

	s.metrics.mu.Lock()
	if group, ok := s.metrics.groups[labelKey]; ok {
		group.upgradingClustersMu.Lock()
		delete(group.upgradingClusters, clusterName)
		group.InProgress--
		if completion.Failed {
			group.Failed++
		} else {
			group.Completed++
		}
		group.upgradingClustersMu.Unlock()
	}
	s.metrics.mu.Unlock()

	s.inProgressMu.Lock()
	delete(s.inProgress, clusterName)
	s.inProgressMu.Unlock()
}

// markClusterCuratorUpgraded updates the ClusterCurator and ManagedCluster after simulated upgrade
// Status pattern based on real successful upgrade from sub02 ClusterCurator
func (s *SimulatorState) markClusterCuratorUpgraded(ctx context.Context, clusterName, version string) error {
	// Update ManagedCluster version labels
	mc, err := s.client.GetManagedCluster(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("failed to get ManagedCluster %s: %w", clusterName, err)
	}

	mcLabels := mc.GetLabels()
	if mcLabels == nil {
		mcLabels = make(map[string]string)
	}
	for k, v := range constants.ParseVersionLabels(version) {
		mcLabels[k] = v
	}
	mc.SetLabels(mcLabels)

	if err := s.client.UpdateManagedCluster(ctx, mc); err != nil {
		return fmt.Errorf("failed to update ManagedCluster %s: %w", clusterName, err)
	}

	// Update ClusterCurator
	curatorList := &unstructured.UnstructuredList{}
	curatorList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   constants.ClusterCuratorGVK.Group,
		Version: constants.ClusterCuratorGVK.Version,
		Kind:    constants.ClusterCuratorGVK.Kind + "List",
	})

	if err := s.client.ListResourcesInNamespace(ctx, curatorList, clusterName); err != nil {
		return fmt.Errorf("failed to list ClusterCurators: %w", err)
	}

	if len(curatorList.Items) == 0 {
		return nil
	}

	curator := curatorList.Items[0].DeepCopy()

	// Remove simulator annotations now that upgrade is complete
	annotations := curator.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.AnnotationUpgradeCompleteAt)
		delete(annotations, constants.AnnotationUpgradeWillFail)
		curator.SetAnnotations(annotations)
	}

	// Keep spec with desiredCuration and desiredUpdate set (matches real successful upgrade pattern)
	curator.Object["spec"] = map[string]interface{}{
		"desiredCuration": constants.CurationUpgrade,
		"install": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
		"upgrade": map[string]interface{}{
			"desiredUpdate":  version,
			"monitorTimeout": int64(120),
		},
		"scale": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
		"destroy": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
	}

	// Set status condition matching real ClusterCurator successful upgrade pattern (from sub02)
	// Only clustercurator-job condition with Job_has_finished
	now := metav1.Now()
	conditions := []interface{}{
		map[string]interface{}{
			"type":               constants.ConditionCuratorJob,
			"status":             "True",
			"reason":             "Job_has_finished",
			"message":            fmt.Sprintf("curator-job-simulated DesiredCuration: upgrade Version (%s;;)", version),
			"lastTransitionTime": now.Format(time.RFC3339),
		},
	}
	unstructured.SetNestedSlice(curator.Object, conditions, "status", "conditions")

	return s.client.UpdateResource(ctx, curator)
}

// markClusterCuratorFailed updates the ClusterCurator to indicate a failed upgrade
// This mimics the failure pattern seen in real ClusterCurators (e.g., timeout waiting for monitor upgrade job)
func (s *SimulatorState) markClusterCuratorFailed(ctx context.Context, clusterName, version string) error {
	// Update ClusterCurator with failure status
	curatorList := &unstructured.UnstructuredList{}
	curatorList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   constants.ClusterCuratorGVK.Group,
		Version: constants.ClusterCuratorGVK.Version,
		Kind:    constants.ClusterCuratorGVK.Kind + "List",
	})

	if err := s.client.ListResourcesInNamespace(ctx, curatorList, clusterName); err != nil {
		return fmt.Errorf("failed to list ClusterCurators: %w", err)
	}

	if len(curatorList.Items) == 0 {
		return nil
	}

	curator := curatorList.Items[0].DeepCopy()
	curatorName := curator.GetName()

	// Remove simulator annotations now that upgrade is complete (failed)
	annotations := curator.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.AnnotationUpgradeCompleteAt)
		delete(annotations, constants.AnnotationUpgradeWillFail)
		curator.SetAnnotations(annotations)
	}

	// Keep the desiredCuration set to "upgrade" to indicate the upgrade was attempted but failed
	// This matches real failure behavior where the spec remains with the upgrade request
	spec := map[string]interface{}{
		"desiredCuration": constants.CurationUpgrade,
		"install": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
		"upgrade": map[string]interface{}{
			"desiredUpdate":  version,
			"monitorTimeout": int64(120),
		},
		"scale": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
		"destroy": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
	}
	curator.Object["spec"] = spec

	// Set status conditions matching real ClusterCurator failure pattern
	// Based on jnp-mc01 example: clustercurator-job and monitor-upgrade show Job_failed
	now := metav1.Now()
	failureMessage := fmt.Sprintf("curator-job-simulated DesiredCuration: upgrade Version (%s;;) Failed - Timed out waiting for monitor upgrade job", version)
	conditions := []interface{}{
		map[string]interface{}{
			"type":               constants.ConditionCuratorJob,
			"status":             "True",
			"reason":             "Job_failed",
			"message":            failureMessage,
			"lastTransitionTime": now.Format(time.RFC3339),
		},
		map[string]interface{}{
			"type":               constants.ConditionUpgradeCluster,
			"status":             "True",
			"reason":             "Job_has_finished",
			"message":            "Completed executing init container",
			"lastTransitionTime": now.Format(time.RFC3339),
		},
		map[string]interface{}{
			"type":               constants.ConditionMonitorUpgrade,
			"status":             "True",
			"reason":             "Job_failed",
			"message":            "Timed out waiting for monitor upgrade job",
			"lastTransitionTime": now.Format(time.RFC3339),
		},
	}
	unstructured.SetNestedSlice(curator.Object, conditions, "status", "conditions")

	_ = curatorName // Used for potential logging
	return s.client.UpdateResource(ctx, curator)
}

// isClusterCuratorFailed checks if a ClusterCurator has a failed status based on its conditions
// This is used to detect failures that occurred before simulator restart
func (s *SimulatorState) isClusterCuratorFailed(curator *unstructured.Unstructured) bool {
	conditions, found, err := unstructured.NestedSlice(curator.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}

	for _, condRaw := range conditions {
		cond, ok := condRaw.(map[string]interface{})
		if !ok {
			continue
		}

		reason, _, _ := unstructured.NestedString(cond, "reason")
		if reason == "Job_failed" {
			return true
		}
	}

	return false
}

// markClusterCuratorUpgrading updates the ClusterCurator to indicate an upgrade is in progress
// This simulates the real ClusterCurator behavior when an upgrade job is running.
// It stores the expected completion time and failure flag in annotations so the simulator
// can determine when to complete the upgrade by checking the ClusterCurator state.
func (s *SimulatorState) markClusterCuratorUpgrading(ctx context.Context, clusterName, version string, durationSecs int, willFail bool) error {
	// Get ClusterCurator
	curatorList := &unstructured.UnstructuredList{}
	curatorList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   constants.ClusterCuratorGVK.Group,
		Version: constants.ClusterCuratorGVK.Version,
		Kind:    constants.ClusterCuratorGVK.Kind + "List",
	})

	if err := s.client.ListResourcesInNamespace(ctx, curatorList, clusterName); err != nil {
		return fmt.Errorf("failed to list ClusterCurators: %w", err)
	}

	if len(curatorList.Items) == 0 {
		return nil
	}

	curator := curatorList.Items[0].DeepCopy()

	// Set annotations with completion time and failure flag
	annotations := curator.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	completeAt := time.Now().Add(time.Duration(durationSecs) * time.Second)
	annotations[constants.AnnotationUpgradeCompleteAt] = completeAt.Format(time.RFC3339)
	if willFail {
		annotations[constants.AnnotationUpgradeWillFail] = "true"
	} else {
		annotations[constants.AnnotationUpgradeWillFail] = "false"
	}
	curator.SetAnnotations(annotations)

	// Set spec with upgrade configuration
	curator.Object["spec"] = map[string]interface{}{
		"desiredCuration": constants.CurationUpgrade,
		"install": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
		"upgrade": map[string]interface{}{
			"desiredUpdate":  version,
			"monitorTimeout": int64(120),
		},
		"scale": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
		"destroy": map[string]interface{}{
			"jobMonitorTimeout": int64(5),
		},
	}

	// Set status conditions indicating upgrade is in progress
	// This matches real ClusterCurator behavior during an active upgrade
	now := metav1.Now()
	conditions := []interface{}{
		map[string]interface{}{
			"type":               constants.ConditionCuratorJob,
			"status":             "False",
			"reason":             "Job_running",
			"message":            fmt.Sprintf("Upgrading cluster to version %s - estimated %ds remaining", version, durationSecs),
			"lastTransitionTime": now.Format(time.RFC3339),
		},
		map[string]interface{}{
			"type":               constants.ConditionUpgradeCluster,
			"status":             "True",
			"reason":             "Job_has_finished",
			"message":            "Completed executing init container",
			"lastTransitionTime": now.Format(time.RFC3339),
		},
		map[string]interface{}{
			"type":               constants.ConditionMonitorUpgrade,
			"status":             "False",
			"reason":             "Job_running",
			"message":            fmt.Sprintf("Monitoring upgrade to %s - 1 object remaining", version),
			"lastTransitionTime": now.Format(time.RFC3339),
		},
	}
	unstructured.SetNestedSlice(curator.Object, conditions, "status", "conditions")

	return s.client.UpdateResource(ctx, curator)
}

// displayMetrics shows current status (updates in place using ANSI escape codes)
func (s *SimulatorState) displayMetrics() {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()

	// Clear screen and move cursor to top-left
	fmt.Print("\033[2J\033[H")

	elapsed := time.Since(s.startTime)
	elapsedStr := formatDuration(elapsed)

	fmt.Println(strings.Repeat("=", 100))
	fmt.Printf("Cluster Upgrade Simulation Status - %s (Running: %s)\n", time.Now().Format("2006-01-02 15:04:05"), elapsedStr)
	fmt.Printf("Started: %s | Test ID: %s\n", s.startTime.Format("2006-01-02 15:04:05"), s.testID)
	fmt.Println(strings.Repeat("=", 100))

	// Display CCR phase with color
	s.targetVersionMu.RLock()
	phase := s.ccrPhase
	activeStep := s.ccrActiveStep
	soakEnds := s.ccrSoakEndsAt
	s.targetVersionMu.RUnlock()

	switch phase {
	case "Processing":
		fmt.Printf("\033[34m● PROCESSING\033[0m - Step: %s\n", activeStep)
	case "Soaking":
		if soakEnds != "" {
			fmt.Printf("\033[33m● SOAKING\033[0m - Step: %s (ends: %s)\n", activeStep, soakEnds)
		} else {
			fmt.Printf("\033[33m● SOAKING\033[0m - Step: %s\n", activeStep)
		}
	case "Paused":
		fmt.Printf("\033[31m● PAUSED ON FAILURE\033[0m - Delete failed ClusterCurator(s) to resume\n")
		// Read failed clusters and display them
		s.targetVersionMu.RLock()
		failedClusters := s.ccrFailedClusters
		s.targetVersionMu.RUnlock()
		if len(failedClusters) > 0 {
			fmt.Printf("  Failed clusters: \033[31m%s\033[0m\n", strings.Join(failedClusters, ", "))
		}
	case "Completed":
		fmt.Printf("\033[32m● COMPLETED\033[0m\n")
	case "CompletedWithFailures":
		fmt.Printf("\033[33m● COMPLETED WITH FAILURES\033[0m\n")
		// Read failed clusters and display them
		s.targetVersionMu.RLock()
		failedClusters := s.ccrFailedClusters
		s.targetVersionMu.RUnlock()
		if len(failedClusters) > 0 {
			fmt.Printf("  Failed clusters: \033[31m%s\033[0m\n", strings.Join(failedClusters, ", "))
		}
	default:
		fmt.Printf("● IDLE\n")
	}

	groupKeys := make([]string, 0, len(s.metrics.groups))
	for key := range s.metrics.groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	totalClusters := 0
	totalPending := 0
	totalInProgress := 0
	totalUpgraded := 0
	totalSkipped := 0
	totalFailed := 0

	// Extract label keys from ALL groups for header
	// (different groups may have different labels due to conditional parent requirements)
	labelKeysSet := make(map[string]bool)
	for _, key := range groupKeys {
		group := s.metrics.groups[key]
		for labelKey := range group.Labels {
			labelKeysSet[labelKey] = true
		}
	}
	var labelKeys []string
	for k := range labelKeysSet {
		labelKeys = append(labelKeys, k)
	}
	sort.Strings(labelKeys)

	// Build header with label key names
	headerLine := ""
	if len(labelKeys) > 0 {
		headerLine = strings.Join(labelKeys, ", ")
	} else {
		headerLine = "LABELS"
	}
	if len(headerLine) > 48 {
		headerLine = headerLine[:45] + "..."
	}

	fmt.Printf("\n%-44s %8s %10s %10s %8s %8s\n", headerLine, "PENDING", "UPGRADING", "UPGRADED", "SKIPPED", "FAILED")
	fmt.Println(strings.Repeat("-", 100))

	for _, groupKey := range groupKeys {
		group := s.metrics.groups[groupKey]
		group.upgradingClustersMu.Lock()

		// Build values string from labels
		var values []string
		if len(labelKeys) > 0 {
			for _, key := range labelKeys {
				values = append(values, group.Labels[key])
			}
		}
		labelStr := strings.Join(values, ", ")
		if labelStr == "" {
			labelStr = "(no labels)"
		}
		if len(labelStr) > 48 {
			labelStr = labelStr[:45] + "..."
		}

		// Format FAILED column - red if there are failures
		failedStr := fmt.Sprintf("%4d/%d", group.Failed, group.Total)
		if group.Failed > 0 {
			failedStr = fmt.Sprintf("\033[31m%4d/%d\033[0m", group.Failed, group.Total)
		}

		fmt.Printf("%-44s %4d/%d %6d/%d %6d/%d %4d/%d %s\n",
			labelStr,
			group.Pending, group.Total,
			group.InProgress, group.Total,
			group.Completed, group.Total,
			group.Skipped, group.Total,
			failedStr)

		// Progress bar with colors
		barWidth := 40
		if group.Total > 0 {
			pendingBar := int(float64(group.Pending) / float64(group.Total) * float64(barWidth))
			upgradingBar := int(float64(group.InProgress) / float64(group.Total) * float64(barWidth))
			upgradedBar := int(float64(group.Completed) / float64(group.Total) * float64(barWidth))
			skippedBar := int(float64(group.Skipped) / float64(group.Total) * float64(barWidth))
			failedBar := int(float64(group.Failed) / float64(group.Total) * float64(barWidth))

			// Build bar with ANSI colors
			// Gray for pending, Orange for upgrading, Green for completed, Cyan for skipped, Red for failed
			bar := "\033[90m" + strings.Repeat("░", pendingBar) + "\033[0m" + // Gray pending
				"\033[38;5;208m" + strings.Repeat("▓", upgradingBar) + "\033[0m" + // Orange upgrading block
				"\033[32m" + strings.Repeat("█", upgradedBar) + "\033[0m" + // Green completed
				"\033[36m" + strings.Repeat("▢", skippedBar) + "\033[0m" + // Cyan skipped outlined block
				"\033[31m" + strings.Repeat("✗", failedBar) + "\033[0m" // Red failed

			// Calculate visible length (without ANSI codes)
			visibleLen := pendingBar + upgradingBar + upgradedBar + skippedBar + failedBar
			padding := ""
			if visibleLen < barWidth {
				padding = strings.Repeat(" ", barWidth-visibleLen)
			}
			fmt.Printf("%-44s [%s%s]\n", "", bar, padding)
		}

		// Show upgrading clusters
		if group.InProgress > 0 {
			count := 0
			for _, state := range group.upgradingClusters {
				remaining := time.Until(state.EstimatedEnd)
				if remaining < 0 {
					remaining = 0
				}
				fmt.Printf("%-50s   ↳ %s → %s (%.0fs remaining)\n",
					"", state.ClusterName, state.TargetVersion, remaining.Seconds())
				count++
				if count >= 3 {
					if len(group.upgradingClusters) > 3 {
						fmt.Printf("%-50s   ... and %d more\n", "", len(group.upgradingClusters)-3)
					}
					break
				}
			}
		}

		group.upgradingClustersMu.Unlock()

		totalClusters += group.Total
		totalPending += group.Pending
		totalInProgress += group.InProgress
		totalUpgraded += group.Completed
		totalSkipped += group.Skipped
		totalFailed += group.Failed
	}

	// Summary - use configured total from config file if available
	displayTotal := totalClusters
	if s.configuredTotal > 0 {
		displayTotal = s.configuredTotal
	}

	// Format total FAILED - red if there are failures
	totalFailedStr := fmt.Sprintf("%4d/%d", totalFailed, displayTotal)
	if totalFailed > 0 {
		totalFailedStr = fmt.Sprintf("\033[31m%4d/%d\033[0m", totalFailed, displayTotal)
	}

	fmt.Println(strings.Repeat("=", 100))
	fmt.Printf("%-44s %4d/%d %6d/%d %6d/%d %4d/%d %s\n",
		"TOTAL",
		totalPending, displayTotal,
		totalInProgress, displayTotal,
		totalUpgraded, displayTotal,
		totalSkipped, displayTotal,
		totalFailedStr)

	if displayTotal > 0 {
		barWidth := 40
		pendingBar := int(float64(totalPending) / float64(displayTotal) * float64(barWidth))
		upgradingBar := int(float64(totalInProgress) / float64(displayTotal) * float64(barWidth))
		upgradedBar := int(float64(totalUpgraded) / float64(displayTotal) * float64(barWidth))
		skippedBar := int(float64(totalSkipped) / float64(displayTotal) * float64(barWidth))
		failedBar := int(float64(totalFailed) / float64(displayTotal) * float64(barWidth))

		// Build bar with ANSI colors
		bar := "\033[90m" + strings.Repeat("░", pendingBar) + "\033[0m" +
			"\033[38;5;208m" + strings.Repeat("▓", upgradingBar) + "\033[0m" +
			"\033[32m" + strings.Repeat("█", upgradedBar) + "\033[0m" +
			"\033[36m" + strings.Repeat("▢", skippedBar) + "\033[0m" +
			"\033[31m" + strings.Repeat("✗", failedBar) + "\033[0m"

		visibleLen := pendingBar + upgradingBar + upgradedBar + skippedBar + failedBar
		padding := ""
		if visibleLen < barWidth {
			padding = strings.Repeat(" ", barWidth-visibleLen)
		}
		fmt.Printf("%-44s [%s%s]\n", "", bar, padding)

		fmt.Printf("\nLegend: \033[90m░\033[0m Pending  \033[38;5;208m▓\033[0m Upgrading  \033[32m█\033[0m Upgraded  \033[36m▢\033[0m Skipped  \033[31m✗\033[0m Failed\n")
		doneCount := totalUpgraded + totalSkipped
		fmt.Printf("Progress: %.1f%% complete (%d upgraded + %d skipped = %d/%d clusters)\n",
			float64(doneCount)/float64(displayTotal)*100, totalUpgraded, totalSkipped, doneCount, displayTotal)
	}

	fmt.Println(strings.Repeat("=", 100))
	os.Stdout.Sync()
}

// calculateUpgradeTime calculates upgrade duration with variance and uplift
func calculateUpgradeTime() int {
	baseTime := float64(upgradeTimeSeconds)
	randomFactor := randomFactorPercent / 100.0
	uplift := upliftPercent / 100.0

	// Random variance: -randomFactor to +randomFactor
	randomVariance := (rand.Float64() * 2 * randomFactor) - randomFactor
	variance := baseTime * randomVariance

	timeWithVariance := baseTime + variance
	finalTime := timeWithVariance * (1 + uplift)

	if finalTime < 1 {
		finalTime = 1
	}

	return int(math.Round(finalTime))
}

// filterGroupingLabels keeps only labels that are defined in the config
func (s *SimulatorState) filterGroupingLabels(labels map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range labels {
		// Only include labels that are defined in the config
		if s.labelKeys[k] {
			result[k] = v
		}
	}
	return result
}

// labelsToKey creates a consistent key from labels
func labelsToKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, labels[k]))
	}

	return strings.Join(parts, "|")
}

// formatDuration formats a duration in a human-readable format (e.g., "1h 23m 45s")
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// log writes a timestamped log entry to the log file
func (s *SimulatorState) log(eventType, format string, args ...interface{}) {
	s.logFileMu.Lock()
	defer s.logFileMu.Unlock()

	if s.logFile == nil {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	logLine := fmt.Sprintf("[%s] [%s] %s\n", timestamp, eventType, message)
	s.logFile.WriteString(logLine)
	s.logFile.Sync()
}

// runPeriodicPoll runs a periodic poll to check ClusterCurators and catch missed watch events
func (s *SimulatorState) runPeriodicPoll(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(pollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pollClusterCurators(ctx)
			s.checkDeletedClusterCurators(ctx)
		}
	}
}

// checkDeletedClusterCurators detects when failed ClusterCurators have been deleted
// and resets their state to pending so the rollout can resume
func (s *SimulatorState) checkDeletedClusterCurators(ctx context.Context) {
	// Collect failed curators to check (avoid holding lock during API calls)
	type failedCurator struct {
		key       string
		name      string
		namespace string
	}
	var failedCurators []failedCurator

	s.curatorsMu.RLock()
	for curatorKey, state := range s.curators {
		if state.State == constants.StateFailed {
			failedCurators = append(failedCurators, failedCurator{
				key:       curatorKey,
				name:      state.Name,
				namespace: state.Namespace,
			})
		}
	}
	s.curatorsMu.RUnlock()

	// Check each failed curator without holding curatorsMu
	for _, fc := range failedCurators {
		// Check if the ClusterCurator still exists
		curator, err := s.client.GetClusterCurator(ctx, fc.name, fc.namespace)
		if err != nil || curator == nil {
			// ClusterCurator was deleted - reset to idle so it can be retried
			s.log("CURATOR_DELETED", "Failed ClusterCurator %s/%s was deleted, resetting to idle for retry",
				fc.namespace, fc.name)

			// Update metrics first (before curatorsMu to avoid deadlock with markClusterSkipped)
			clusterName := fc.namespace
			ns, err := s.client.GetNamespace(ctx, clusterName)
			if err == nil {
				labels := s.filterGroupingLabels(ns.GetLabels())
				labelKey := labelsToKey(labels)

				s.metrics.mu.Lock()
				if group, ok := s.metrics.groups[labelKey]; ok {
					group.upgradingClustersMu.Lock()
					if group.Failed > 0 {
						group.Failed--
						group.Pending++
					}
					group.upgradingClustersMu.Unlock()
				}
				s.metrics.mu.Unlock()
			}

			// Now update curator state
			s.curatorsMu.Lock()
			if state, exists := s.curators[fc.key]; exists {
				state.State = constants.StateIdle
				state.LastObservedState = ""
				// Remove from curators map so it gets re-detected as new
				delete(s.curators, fc.key)
			}
			s.curatorsMu.Unlock()
		}
	}
}

// pollClusterCurators lists all ClusterCurators and checks for missed upgrade requests
// It also resumes tracking of in-progress upgrades (from annotations) after simulator restart
func (s *SimulatorState) pollClusterCurators(ctx context.Context) {
	s.log("POLL", "Starting periodic poll of ClusterCurators")

	// Find ClusterCurators by looking up ManagedClusters with our test ID first,
	// then getting ClusterCurators from their namespaces. This is necessary because
	// ClusterCurators created by the controller don't have the infra-test-id label.
	labelSelector := fmt.Sprintf("%s=%s", constants.LabelInfraTestID, s.testID)
	curators, err := s.client.GetClusterCuratorsForManagedClusters(ctx, labelSelector)
	if err != nil {
		s.log("POLL_ERROR", "Failed to list ClusterCurators: %v", err)
		return
	}

	s.log("POLL", "Found %d ClusterCurators", len(curators))

	checkedCount := 0
	triggeredCount := 0
	resumedCount := 0

	for _, curator := range curators {
		namespace := curator.GetNamespace()
		curatorKey := fmt.Sprintf("%s/%s", namespace, curator.GetName())
		clusterName := namespace

		desiredCuration, _, _ := unstructured.NestedString(curator.Object, "spec", "desiredCuration")
		desiredVersion, _, _ := unstructured.NestedString(curator.Object, "spec", "upgrade", "desiredUpdate")
		annotations := curator.GetAnnotations()

		checkedCount++

		// Get the CCR's target version to ensure we only process ClusterCurators
		// that match the current rollout target
		s.targetVersionMu.RLock()
		ccrTargetVersion := s.targetVersion
		s.targetVersionMu.RUnlock()

		// Don't trigger upgrade if ClusterCurator's desiredUpdate doesn't match CCR's target version
		// The cluster remains in Pending state until the controller patches the ClusterCurator
		// with the new target version
		if ccrTargetVersion != "" && desiredVersion != "" && desiredVersion != ccrTargetVersion {
			continue
		}

		// Check current curator state - skip if already completed or failed
		s.curatorsMu.RLock()
		curatorState, stateExists := s.curators[curatorKey]
		isCompleted := stateExists && curatorState.State == constants.StateCompleted
		isFailed := stateExists && curatorState.State == constants.StateFailed
		s.curatorsMu.RUnlock()

		if isCompleted || isFailed {
			// This curator has already been completed or failed, skip it entirely
			continue
		}

		// Check if this ClusterCurator shows a failed state from its status conditions
		// This detects failures that occurred before simulator restart
		if s.isClusterCuratorFailed(curator) {
			s.curatorsMu.Lock()
			if !stateExists {
				curatorState = &CuratorState{
					Namespace:         namespace,
					Name:              curator.GetName(),
					DesiredCuration:   desiredCuration,
					DesiredVersion:    desiredVersion,
					State:             constants.StateFailed,
					LastObservedState: desiredCuration,
				}
				s.curators[curatorKey] = curatorState
			} else {
				curatorState.State = constants.StateFailed
			}
			s.curatorsMu.Unlock()

			// Update metrics group counters for the failure
			ns, err := s.client.GetNamespace(ctx, namespace)
			if err == nil {
				labels := s.filterGroupingLabels(ns.GetLabels())
				labelKey := labelsToKey(labels)

				s.metrics.mu.Lock()
				if group, ok := s.metrics.groups[labelKey]; ok {
					group.upgradingClustersMu.Lock()
					// Only count as failed if not already counted
					if _, alreadyUpgrading := group.upgradingClusters[clusterName]; !alreadyUpgrading {
						group.Pending--
						group.Failed++
					} else {
						// Was upgrading, now failed
						delete(group.upgradingClusters, clusterName)
						group.InProgress--
						group.Failed++
					}
					group.upgradingClustersMu.Unlock()
				}
				s.metrics.mu.Unlock()
			}

			s.log("POLL_FAILED", "Detected failed ClusterCurator on poll: %s", clusterName)
			continue
		}

		// First, check if this is an in-progress upgrade that we need to resume tracking
		// (e.g., after simulator restart)
		if annotations != nil {
			if _, hasCompleteAt := annotations[constants.AnnotationUpgradeCompleteAt]; hasCompleteAt {

				s.inProgressMu.RLock()
				alreadyTracked := s.inProgress[clusterName]
				s.inProgressMu.RUnlock()

				if !alreadyTracked {
					// Resume tracking this in-progress upgrade
					s.inProgressMu.Lock()
					s.inProgress[clusterName] = true
					s.inProgressMu.Unlock()

					s.curatorsMu.Lock()
					curatorState, exists := s.curators[curatorKey]
					if !exists {
						curatorState = &CuratorState{
							Namespace:         namespace,
							Name:              curator.GetName(),
							DesiredCuration:   desiredCuration,
							DesiredVersion:    desiredVersion,
							State:             constants.StateUpgrading,
							LastObservedState: desiredCuration,
						}
						s.curators[curatorKey] = curatorState
					} else {
						curatorState.State = constants.StateUpgrading
					}
					s.curatorsMu.Unlock()

					// Update metrics group counters
					ns, err := s.client.GetNamespace(ctx, namespace)
					if err == nil {
						labels := s.filterGroupingLabels(ns.GetLabels())
						labelKey := labelsToKey(labels)

						s.metrics.mu.Lock()
						if group, ok := s.metrics.groups[labelKey]; ok {
							group.upgradingClustersMu.Lock()
							// Only update if this cluster isn't already being tracked as upgrading
							if _, alreadyUpgrading := group.upgradingClusters[clusterName]; !alreadyUpgrading {
								// Parse completion time from annotation for display
								completeAtStr := annotations[constants.AnnotationUpgradeCompleteAt]
								completeAt, _ := time.Parse(time.RFC3339, completeAtStr)
								group.upgradingClusters[clusterName] = &UpgradeState{
									ClusterName:   clusterName,
									StartTime:     time.Now(),
									EstimatedEnd:  completeAt,
									TargetVersion: desiredVersion,
								}
								group.Pending--
								group.InProgress++
							}
							group.upgradingClustersMu.Unlock()
						}
						s.metrics.mu.Unlock()
					}

					s.log("POLL_RESUME", "Resumed tracking in-progress upgrade for %s → %s", clusterName, desiredVersion)
					resumedCount++
					continue
				}
			}
		}

		// Check if this is an upgrade request that we haven't processed yet
		if desiredCuration == constants.CurationUpgrade {
			s.curatorsMu.Lock()
			curatorState, exists := s.curators[curatorKey]

			// Check if we need to start an upgrade
			needsUpgrade := false
			if !exists {
				// New curator we haven't seen
				curatorState = &CuratorState{
					Namespace:         namespace,
					Name:              curator.GetName(),
					DesiredCuration:   desiredCuration,
					DesiredVersion:    desiredVersion,
					State:             constants.StateIdle,
					LastObservedState: "",
				}
				s.curators[curatorKey] = curatorState
				needsUpgrade = true
			} else if curatorState.State != constants.StateUpgrading &&
				curatorState.State != constants.StateCompleted {
				// Existing curator that's not already upgrading or completed
				needsUpgrade = true
			}

			if needsUpgrade {
				curatorState.State = constants.StateUpgrading
				curatorState.LastObservedState = desiredCuration
			}
			s.curatorsMu.Unlock()

			if needsUpgrade {
				s.log("POLL_TRIGGER", "Triggering missed upgrade for %s → %s", namespace, desiredVersion)
				triggeredCount++
				s.startUpgradeSimulation(ctx, namespace, curator.GetName(), desiredVersion)
			}
		}
	}

	s.log("POLL_COMPLETE", "Checked %d curators, triggered %d new upgrades, resumed %d in-progress", checkedCount, triggeredCount, resumedCount)
}

// runSkippedClusterCheck runs a periodic check for clusters that should be skipped
func (s *SimulatorState) runSkippedClusterCheck(ctx context.Context) {
	// Check immediately, then periodically
	s.checkSkippedClusters(ctx)

	ticker := time.NewTicker(time.Duration(pollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkSkippedClusters(ctx)
		}
	}
}

// runUpgradeCompletionCheck runs a periodic check for upgrades that are ready to complete
func (s *SimulatorState) runUpgradeCompletionCheck(ctx context.Context) {
	// Check more frequently than the poll interval to complete upgrades promptly
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkUpgradeCompletions(ctx)
		}
	}
}

// compareVersions compares two version strings (e.g., "4.15.5" vs "4.16.0")
// Returns: -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func compareVersions(v1, v2 string) int {
	parts1 := strings.Split(v1, ".")
	parts2 := strings.Split(v2, ".")

	maxLen := len(parts1)
	if len(parts2) > maxLen {
		maxLen = len(parts2)
	}

	for i := 0; i < maxLen; i++ {
		var n1, n2 int

		if i < len(parts1) {
			fmt.Sscanf(parts1[i], "%d", &n1)
		}
		if i < len(parts2) {
			fmt.Sscanf(parts2[i], "%d", &n2)
		}

		if n1 < n2 {
			return -1
		}
		if n1 > n2 {
			return 1
		}
	}

	return 0
}

// checkSkippedClusters checks for clusters that should be skipped because
// they are already at or above the target version. This moves them from
// Pending to Skipped state and logs the transition.
func (s *SimulatorState) checkSkippedClusters(ctx context.Context) {
	// Always check for version changes from ClusterCuratorRollout
	// This also handles fetching the initial target version
	s.fetchTargetVersionFromRollout(ctx)

	// Get the current target version
	s.targetVersionMu.RLock()
	targetVersion := s.targetVersion
	s.targetVersionMu.RUnlock()

	if targetVersion == "" {
		// No target version known yet
		return
	}

	// List ManagedClusters with our test ID
	labelSelector := fmt.Sprintf("%s=%s", constants.LabelInfraTestID, s.testID)
	managedClusters, err := s.client.GetManagedClustersByLabel(ctx, labelSelector)
	if err != nil {
		return
	}

	for _, mc := range managedClusters.Items {
		clusterName := mc.GetName()
		mcLabels := mc.GetLabels()
		currentVersion := mcLabels[constants.LabelOpenShiftVersion]

		if currentVersion == "" {
			continue
		}

		// Check if cluster version is >= target version
		if compareVersions(currentVersion, targetVersion) >= 0 {
			// This cluster should be skipped - check if we've already processed it
			s.markClusterSkipped(ctx, clusterName, currentVersion, targetVersion)
		}
	}
}

// fetchTargetVersionFromRollout fetches the target version from ClusterCuratorRollout
// and detects when the rollout status has been reset (indicating a version change)
func (s *SimulatorState) fetchTargetVersionFromRollout(ctx context.Context) {
	// List ClusterCuratorRollout resources to find the target version
	rolloutGVR := schema.GroupVersionResource{
		Group:    "rollout.open-cluster-management.io",
		Version:  "v1alpha1",
		Resource: "clustercuratorrollouts",
	}

	rollouts, err := s.client.ListResourcesByGVR(ctx, rolloutGVR)
	if err != nil {
		return
	}

	for _, rollout := range rollouts.Items {
		// Get the spec.openShiftVersion
		version, found, _ := unstructured.NestedString(rollout.Object, "spec", "openShiftVersion")
		if !found || version == "" {
			continue
		}

		// Get the status.observedOpenShiftVersion to detect resets
		observedVersion, _, _ := unstructured.NestedString(rollout.Object, "status", "observedOpenShiftVersion")

		// Check if the rollout status has been reset (rolloutPlan will be nil/empty)
		rolloutPlan, _, _ := unstructured.NestedSlice(rollout.Object, "status", "rolloutPlan")

		// Get the status.phase to detect Paused state
		statusPhase, _, _ := unstructured.NestedString(rollout.Object, "status", "phase")

		s.targetVersionMu.Lock()

		// Detect version change reset:
		// - We have a previous observed version
		// - The new observed version is different
		// - The rolloutPlan is empty (status was reset)
		if s.observedVersion != "" && observedVersion != "" &&
			s.observedVersion != observedVersion && len(rolloutPlan) == 0 {
			s.targetVersionMu.Unlock()
			s.log("VERSION_RESET", "Detected ClusterCuratorRollout version change from %s to %s - resetting simulator state",
				s.observedVersion, observedVersion)
			s.resetSimulatorState(ctx, observedVersion, version)
			return
		}

		// Update observed version
		if observedVersion != "" {
			s.observedVersion = observedVersion
		}

		// Update target version if changed
		if s.targetVersion != version {
			s.targetVersion = version
			s.log("TARGET_VERSION", "Detected target version from ClusterCuratorRollout: %s", version)
		}

		// Extract CCR phase from status.phase first, then from rollout plan
		s.ccrPhase = "Idle"
		s.ccrActiveStep = ""
		s.ccrSoakEndsAt = ""
		s.ccrFailedClusters = nil

		// Read failed clusters from status.failedClusters
		failedClustersRaw, _, _ := unstructured.NestedSlice(rollout.Object, "status", "failedClusters")
		for _, fcRaw := range failedClustersRaw {
			if fc, ok := fcRaw.(map[string]interface{}); ok {
				if name, found, _ := unstructured.NestedString(fc, "name"); found && name != "" {
					s.ccrFailedClusters = append(s.ccrFailedClusters, name)
				}
			}
		}

		// Use status.phase from the CCR as the source of truth for the phase
		// Then find the active step from the rollout plan for display
		switch statusPhase {
		case "Processing":
			s.ccrPhase = "Processing"
		case "Soaking":
			s.ccrPhase = "Soaking"
		case "Paused":
			s.ccrPhase = "Paused"
		case "Completed":
			s.ccrPhase = "Completed"
		case "CompletedWithFailures":
			s.ccrPhase = "CompletedWithFailures"
		}

		// Find the active step from the rollout plan for display purposes
		for _, stepInterface := range rolloutPlan {
			step, ok := stepInterface.(map[string]interface{})
			if !ok {
				continue
			}
			stepState, _, _ := unstructured.NestedString(step, "state")
			placementName, _, _ := unstructured.NestedString(step, "placementName")

			switch stepState {
			case "Active":
				s.ccrActiveStep = placementName
			case "Soaking", "BatchSoaking":
				s.ccrActiveStep = placementName
				// Try to get soak end time
				if soakEndsAt, found, _ := unstructured.NestedString(step, "soakEndsAt"); found {
					s.ccrSoakEndsAt = soakEndsAt
				} else if batchSoakEndsAt, found, _ := unstructured.NestedString(step, "batchSoakEndsAt"); found {
					s.ccrSoakEndsAt = batchSoakEndsAt
				}
			}

			// Stop at first active/soaking step found
			if s.ccrActiveStep != "" {
				break
			}
		}

		// Check if all steps are completed (Completed or Failed are terminal)
		if s.ccrPhase == "Idle" && len(rolloutPlan) > 0 {
			allCompleted := true
			hasFailures := false
			for _, stepInterface := range rolloutPlan {
				step, ok := stepInterface.(map[string]interface{})
				if !ok {
					continue
				}
				stepState, _, _ := unstructured.NestedString(step, "state")
				if stepState != "Completed" && stepState != "Failed" {
					allCompleted = false
					break
				}
				if stepState == "Failed" {
					hasFailures = true
					// Collect failed cluster names from this step
					if failedClustersRaw, found, _ := unstructured.NestedStringSlice(step, "failedClusters"); found {
						s.ccrFailedClusters = append(s.ccrFailedClusters, failedClustersRaw...)
					}
				}
			}
			if allCompleted {
				if hasFailures {
					s.ccrPhase = "CompletedWithFailures"
				} else {
					s.ccrPhase = "Completed"
				}
			}
		}

		s.targetVersionMu.Unlock()
		return
	}
}

// resetSimulatorState clears all simulator state when the ClusterCuratorRollout
// version changes and its status is reset. This allows the simulator to track
// a fresh rollout cycle.
func (s *SimulatorState) resetSimulatorState(ctx context.Context, newObservedVersion, newTargetVersion string) {
	// Clear curators state
	s.curatorsMu.Lock()
	s.curators = make(map[string]*CuratorState)
	s.curatorsMu.Unlock()

	// Clear in-progress tracking
	s.inProgressMu.Lock()
	s.inProgress = make(map[string]bool)
	s.inProgressMu.Unlock()

	// Re-initialize metrics - need to rebuild group structure with fresh counts
	s.metrics.mu.Lock()
	for _, group := range s.metrics.groups {
		group.upgradingClustersMu.Lock()
		// Reset all counts
		group.Pending = group.Total // All clusters go back to pending
		group.InProgress = 0
		group.Completed = 0
		group.Failed = 0
		group.Skipped = 0
		// Clear maps
		group.upgradingClusters = make(map[string]*UpgradeState)
		group.skippedClusters = make(map[string]bool)
		group.upgradingClustersMu.Unlock()
	}
	s.metrics.mu.Unlock()

	// Update version tracking
	s.targetVersionMu.Lock()
	s.observedVersion = newObservedVersion
	s.targetVersion = newTargetVersion
	s.targetVersionMu.Unlock()

	s.log("RESET", "Simulator state reset complete - ready for new rollout cycle with target version %s", newTargetVersion)
}

// markClusterSkipped moves a cluster from Pending to Skipped state
func (s *SimulatorState) markClusterSkipped(ctx context.Context, clusterName, currentVersion, targetVersion string) {
	// Check curator state first (before acquiring other locks to avoid deadlock)
	s.curatorsMu.RLock()
	curatorKey := fmt.Sprintf("%s/%s", clusterName, clusterName)
	curatorState, exists := s.curators[curatorKey]
	if exists && (curatorState.State == constants.StateCompleted || curatorState.State == constants.StateUpgrading) {
		s.curatorsMu.RUnlock()
		return // Already completed or upgrading
	}
	s.curatorsMu.RUnlock()

	// Get namespace labels for group lookup
	ns, err := s.client.GetNamespace(ctx, clusterName)
	if err != nil {
		return
	}

	labels := s.filterGroupingLabels(ns.GetLabels())
	labelKey := labelsToKey(labels)

	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	group, ok := s.metrics.groups[labelKey]
	if !ok {
		return
	}

	group.upgradingClustersMu.Lock()
	defer group.upgradingClustersMu.Unlock()

	// Check if this cluster is already skipped or in another terminal state
	if group.skippedClusters[clusterName] {
		return // Already skipped
	}

	// Check if cluster is in progress (being upgraded)
	if _, inProgress := group.upgradingClusters[clusterName]; inProgress {
		return // In progress, don't skip
	}

	// Move from Pending to Skipped
	if group.Pending > 0 {
		group.Pending--
		group.Skipped++
		group.skippedClusters[clusterName] = true

		s.log("SKIPPED", "Cluster %s skipped: already at version %s (target: %s)", clusterName, currentVersion, targetVersion)
	}
}

// checkUpgradeCompletions checks for ClusterCurators with upgrades ready to complete
// based on the simulator.test/upgrade-complete-at annotation
func (s *SimulatorState) checkUpgradeCompletions(ctx context.Context) {
	// Find ClusterCurators by looking up ManagedClusters with our test ID first,
	// then getting ClusterCurators from their namespaces
	labelSelector := fmt.Sprintf("%s=%s", constants.LabelInfraTestID, s.testID)
	curators, err := s.client.GetClusterCuratorsForManagedClusters(ctx, labelSelector)
	if err != nil {
		return
	}

	now := time.Now()

	for _, curator := range curators {
		annotations := curator.GetAnnotations()
		if annotations == nil {
			continue
		}

		// Check if this curator has a pending upgrade completion
		completeAtStr, hasCompleteAt := annotations[constants.AnnotationUpgradeCompleteAt]
		if !hasCompleteAt {
			continue
		}

		completeAt, err := time.Parse(time.RFC3339, completeAtStr)
		if err != nil {
			continue
		}

		// Check if it's time to complete this upgrade
		if now.Before(completeAt) {
			continue
		}

		clusterName := curator.GetNamespace()

		// Check if we're already processing this completion
		s.inProgressMu.RLock()
		inProgress := s.inProgress[clusterName]
		s.inProgressMu.RUnlock()

		if !inProgress {
			// This cluster isn't tracked as in progress, skip it
			// (might be a leftover annotation from a previous run)
			continue
		}

		// Get the failure flag and target version
		willFail := annotations[constants.AnnotationUpgradeWillFail] == "true"
		targetVersion, _, _ := unstructured.NestedString(curator.Object, "spec", "upgrade", "desiredUpdate")

		s.log("COMPLETION_CHECK", "Upgrade ready to complete: %s → %s (willFail=%v)", clusterName, targetVersion, willFail)

		// Send completion to the handler
		s.completionCh <- UpgradeCompletion{
			ClusterName:   clusterName,
			TargetVersion: targetVersion,
			Failed:        willFail,
		}
	}
}
