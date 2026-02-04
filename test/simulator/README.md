# Cluster Upgrade Simulation Environment

The simulator folder contains the `simulate` tool, a comprehensive command-line utility for managing and testing ClusterCuratorRollout with synthetic test infrastructure.

## Table of Contents

- [Quick Start](#quick-start)
- [Commands Overview](#commands-overview)
- [Initialize Test Infrastructure (`init`)](#initialize-test-infrastructure-init)
- [Run the Simulator (`run`)](#run-the-simulator-run)
- [Query Infrastructure (`query`)](#query-infrastructure-query)
- [Remove Resources (`remove`)](#remove-resources-remove)
- [Reset for Testing (`reset`)](#reset-for-testing-reset)
- [Configuration Guide](#configuration-guide)
- [Integration with ClusterCuratorRollout](#integration-with-clustercuratorrollout)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

Complete workflow to test ClusterCuratorRollout:

```bash
cd /home/jpacker/workspace_git/cluster-curator-rollout-controller/test/simulator

# Build the simulator
make build

# Terminal 1: Initialize test infrastructure
./bin/simulate init --config ./config/infra_clusters.json

# Terminal 2: Start the simulator monitor
./bin/simulate run --config ./config/infra_clusters.json

# Terminal 3: Query active test infrastructure (optional)
./bin/simulate query --config ./config/infra_clusters.json

# When done: Remove resources
./bin/simulate remove --config ./config/infra_clusters.json
```

---

## Commands Overview

The `simulate` tool provides five main commands:

| Command | Purpose |
|---------|---------|
| `init` | Create synthetic test infrastructure (Namespaces, ManagedClusters) |
| `run` | Monitor and simulate cluster upgrades in real-time |
| `query` | List active test IDs and cluster counts |
| `remove` | Delete test infrastructure and clean up resources |
| `reset` | Reset ClusterCurators for controller restart |

---

## Initialize Test Infrastructure (`init`)

Creates synthetic Namespace and ManagedCluster resources based on your configuration.

### Usage

```bash
./bin/simulate init <config-file> [--dry-run] [--kubeconfig <path>] [--config-id <id>]
```

### Flags

- `<config-file>` - Path to JSON/YAML cluster config (required)
- `--dry-run` - Preview changes without applying
- `--kubeconfig <path>` - Path to kubeconfig file (uses default if omitted)
- `--config-id <id>` - Resume incomplete deployment with existing ID
- `--config` - Alternative flag format for config file

### Examples

```bash
# Create fresh test infrastructure
./bin/simulate init ./config/infra_clusters.json

# Preview creation without applying
./bin/simulate init ./config/infra_clusters.json --dry-run

# Resume incomplete deployment
./bin/simulate init ./config/infra_clusters.json --config-id infra-abc123-456789

# With custom kubeconfig
./bin/simulate init ./config/infra_clusters.json --kubeconfig ~/.kube/config
```

### What It Creates

For each cluster defined in your config:
- **Namespace**: `synthetic-cluster-N` containing all cluster resources
- **ManagedCluster**: `synthetic-cluster-N` in its dedicated namespace
- **Labels**: All resources tagged with `infra-test-id` for easy tracking

**Note**: ClusterCurator resources are NOT created by `init`. They are created by the ClusterCuratorRollout controller when an upgrade is initiated.

### Output

```
======================================================================
SIMULATION ENVIRONMENT ID (Config ID): infra-abc123-456789
Mode: NEW (fresh rollout)
Use this ID to manage/cleanup resources later
======================================================================

Cluster Label Distribution:
  synthetic-cluster-0: {environment: production, region: us-east}
  synthetic-cluster-1: {environment: staging, region: us-west}
  ... (23 more clusters)

✓ Successfully created all resources for 25 clusters
✓ Test ID infra-abc123-456789 is tracked in config file
```

---

## Run the Simulator (`run`)

Watches for ClusterCurator resources and simulates cluster upgrades in real-time.

### Usage

```bash
./bin/simulate run <config-file> [--upgrade-time <seconds>] [--random-factor <percent>] [--uplift <percent>]
```

### Flags

- `<config-file>` - Path to JSON/YAML cluster config (required)
- `--kubeconfig <path>` - Path to kubeconfig file
- `--upgrade-time <secs>` - Base upgrade duration in seconds (overrides config)
- `--random-factor <percent>` - Random variance percentage (default: 25.0)
- `--uplift <percent>` - Fixed uplift percentage (default: 10.0)
- `--display-interval <secs>` - Update display every N seconds (default: 5)
- `--poll-interval <secs>` - Poll ClusterCurators every N seconds (default: 10)
- `--log-file <path>` - Path to log file (default: simulator.log)
- `--config` - Alternative flag format for config file

### Examples

```bash
# Basic usage
./bin/simulate run --config ./config/infra_clusters.json

# Fast testing (quick validation)
./bin/simulate run --config ./config/infra_clusters.json --upgrade-time 10 --random-factor 10.0

# Large scale testing
./bin/simulate run --config ./config/infra_clusters.json --upgrade-time 120 --random-factor 20.0

# Debug mode with verbose logging
./bin/simulate run --config ./config/infra_clusters.json -v=2 --log-file debug.log
```

### Features

**Real-Time Metrics**
- Cluster grouping by label combinations
- State tracking (Pending, Upgrading, Upgraded, Failed)
- Per-group and total upgrade concurrency
- Progress bars showing upgrade status
- Independent metrics collection (decoupled from watchers)

**Upgrade Simulation**
- Watches for upgrade signals from controller
- Configurable upgrade duration with variance
- Per-cluster random variance
- Automatic state transitions
- Failure simulation (configurable percentage)

**Display Output** (updates every 30 seconds)

```
====================================================================================
Cluster Upgrade Simulation Status - 2026-01-20 14:45:30 (Running: 5m 23s)
Started: 2026-01-20 14:40:07 | Test ID: infra-abc123-456789
====================================================================================

LABELS                                  PENDING  UPGRADING  UPGRADED FAILED
----------------------------------------------------------------------------------
production, us-east                        15/25    6/25       4/25   0/25
[░░░░░░░░░░▓▓▓▓▓▓████████████████░░░░░░░░░░░░]
  ↳ synthetic-cluster-0 → 4.15.3 (45.0s remaining)
  ↳ synthetic-cluster-1 → 4.15.3 (78.0s remaining)
  ↳ synthetic-cluster-2 → 4.15.3 (52.0s remaining)

staging, us-west                         10/20    5/20       5/20   0/20
[░░░░░░░░░▓▓▓▓▓████████████░░░░░░░░░░░░░░░░░░░░]

====================================================================================
TOTAL                                     25/100  11/100      9/100   0/100
[░░░░░░░▓▓▓▓▓████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░]

Legend: ░ Pending  ▓ Upgrading  █ Upgraded
Progress: 9.0% complete (9/100 clusters)
====================================================================================
```

---

## Query Infrastructure (`query`)

Lists all active test IDs and their associated cluster counts in Kubernetes.

### Usage

```bash
./bin/simulate query <config-file> [--kubeconfig <path>]
```

### Examples

```bash
# Query test infrastructure
./bin/simulate query ./config/infra_clusters.json

# With custom kubeconfig
./bin/simulate query ./config/infra_clusters.json --kubeconfig ~/.kube/config
```

### Output

```
Loading configuration from ./config/infra_clusters.json...
Connecting to Kubernetes cluster...

Active Test IDs from Config:
===========================

Test ID: infra-abc123-456789
  Clusters: 25

Test ID: infra-def456-123456
  Clusters: 25

===========================
Total Test IDs: 2
Total Clusters: 50

⚠️  Orphaned Test IDs (in cluster but not in config):
===========================
Test ID: infra-xyz789-999999
  Clusters: 10

You can remove orphaned test IDs manually:
  ./simulate remove --config-id infra-xyz789-999999
```

---

## Remove Resources (`remove`)

Deletes all test infrastructure associated with a test ID.

### Usage

```bash
./bin/simulate remove <config-file> [--config-id <id>] [--dry-run] [--kubeconfig <path>]
```

### Flags

- `<config-file>` - Path to config file (optional if `--config-id` provided)
- `--config-id <id>` - Specific test ID to remove (overrides config file)
- `--dry-run` - Preview deletion without applying
- `--kubeconfig <path>` - Path to kubeconfig file

### Examples

```bash
# Remove resources from config file
./bin/simulate remove ./config/infra_clusters.json

# Remove specific test ID
./bin/simulate remove ./config/infra_clusters.json --config-id infra-abc123-456789

# Preview deletion first
./bin/simulate remove ./config/infra_clusters.json --dry-run

# Remove orphaned test ID (config file optional)
./bin/simulate remove --config-id infra-xyz789-999999
```

### Cleanup Process

1. Removes finalizers from ClusterCurators
2. Removes finalizers from ManagedClusters
3. Deletes ClusterCurators
4. Deletes ManagedClusters
5. Deletes Namespaces
6. Removes test ID from config file (if all deletions succeed)

### Output

```
======================================================================
REMOVING RESOURCES FOR TEST ID: infra-abc123-456789
======================================================================

Connecting to Kubernetes cluster...
Deleting resources by test ID: infra-abc123-456789
  Using label selector: infra-test-id=infra-abc123-456789

Removing finalizers from ClusterCurators (via ManagedCluster lookup)...
✓ Removed finalizers from ClusterCurators

Removing finalizers from ManagedClusters...
✓ Removed finalizers from ManagedClusters

Deleting ClusterCurators (via ManagedCluster lookup)...
✓ Deleted ClusterCurators

Deleting ManagedClusters...
✓ Deleted ManagedClusters

Deleting Namespaces...
✓ Deleted Namespaces

✓ Successfully removed all resources for test ID: infra-abc123-456789
```

---

## Reset for Testing (`reset`)

Resets ClusterCurators to allow the ClusterCuratorRollout controller to restart processing.

### Usage

```bash
./bin/simulate reset <config-file> [--config-id <id>] [--dry-run] [--kubeconfig <path>]
```

### Flags

- `<config-file>` - Path to config file (required to get base versions)
- `--config-id <id>` - Specific test ID to reset (overrides config file)
- `--dry-run` - Preview reset without applying
- `--kubeconfig <path>` - Path to kubeconfig file

### What It Resets

For each ClusterCurator:
- Clears `spec.desiredCuration` (removes upgrade trigger)
- Clears `spec.upgrade.desiredUpdate` (removes target version)
- Clears all status conditions (resets upgrade status)
- Removes management labels
- Resets ManagedCluster version labels to base version

### Examples

```bash
# Reset all clusters in config
./bin/simulate reset --config ./config/infra_clusters.json

# Reset specific test ID
./bin/simulate reset --config ./config/infra_clusters.json --config-id infra-abc123-456789

# Preview reset first
./bin/simulate reset --config ./config/infra_clusters.json --dry-run
```

### Use Cases

1. **Controller Restart**: Reset after controller crash or restart to re-process all clusters
2. **Test Retry**: Reset after a test run to run another test with same infrastructure
3. **Clean Slate**: Reset without deleting infrastructure (faster than remove/init cycle)

---

## Configuration Guide

### Configuration File Formats

#### JSON Format

```json
{
  "totalClusters": 25,
  "baseVersions": ["4.14.0", "4.14.1"],
  "upgradeTimeSeconds": 300,
  "failurePercentage": 0,
  "activeTestIDs": ["infra-abc123-456789"],
  "labels": [
    {
      "key": "environment",
      "values": [
        {"value": "production", "percentage": 60},
        {"value": "staging", "percentage": 30},
        {"value": "dev"}
      ]
    },
    {
      "key": "region",
      "values": [
        {"value": "us-east", "percentage": 50},
        {"value": "us-west"}
      ]
    }
  ]
}
```

#### YAML Format

```yaml
totalClusters: 25
baseVersions:
  - 4.14.0
  - 4.14.1
upgradeTimeSeconds: 300
failurePercentage: 0
activeTestIDs:
  - infra-abc123-456789
labels:
  - key: environment
    values:
      - value: production
        percentage: 60
      - value: staging
        percentage: 30
      - value: dev
  - key: region
    values:
      - value: us-east
        percentage: 50
      - value: us-west
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `totalClusters` | int | Yes | Number of synthetic clusters to create |
| `baseVersions` | string[] | Yes | Base OpenShift versions for clusters |
| `upgradeTimeSeconds` | int | No | Default upgrade duration in seconds |
| `failurePercentage` | int | No | Percentage of upgrades to simulate as failures (0-100) |
| `activeTestIDs` | string[] | No | Active test IDs (auto-managed by tool) |
| `labels` | object[] | Yes | Label definitions for clusters |

### Label Distribution Logic

- If percentages sum to less than 100, remaining percentage is distributed equally
- If no percentages specified, equal distribution applies
- Each value can specify `percentage` (0-100) to control distribution

### Resource Organization

Each cluster deployment creates:
- **Namespace**: `synthetic-cluster-N` containing all resources
- **ManagedCluster**: `synthetic-cluster-N` in its namespace
- **Labels**: All resources tagged with `infra-test-id` for tracking

### Test ID Format

Test IDs follow format: `infra-{config-hash}-{timestamp}`
- `config-hash`: First 12 characters of config file SHA256 hash
- `timestamp`: Last 6 digits of Unix timestamp

Example: `infra-522d2effa219-807882`

---

## Integration with ClusterCuratorRollout

### Complete Testing Workflow

```bash
# Terminal 1: Initialize infrastructure
./bin/simulate init --config ./config/infra_clusters.json

# Terminal 2: Start simulator
./bin/simulate run --config ./config/infra_clusters.json

# Terminal 3: Create rollout resource
kubectl apply -f config/samples/rollout_v1alpha1_clustercuratorrollout.yaml

# Terminal 4: Watch controller processing
kubectl get ccr -w

# Terminal 5: Monitor controller logs
kubectl logs -f deployment/cluster-curator-rollout-controller-manager

# Terminal 6 (optional): Watch cluster upgrade progress
watch -n 1 './bin/simulate query --config ./test/simulator/config/infra_clusters.json'
```

### How It Works

1. **Setup**: `simulate init` creates synthetic Kubernetes resources
2. **Controller**: Controller detects clusters and creates ClusterCurator resources
3. **Simulator**: `simulate run` watches for upgrade signals in ClusterCurator
4. **Simulation**: Simulator marks clusters as upgrading with specified duration
5. **Completion**: After duration expires, simulator marks upgrade as complete
6. **Controller**: Controller detects completion and moves to next batch
7. **Metrics**: Simulator continuously displays concurrency and progress

### Validation Points

- ✅ Simulator displays metrics every 30 seconds
- ✅ Metrics show clusters grouped by label combinations
- ✅ Concurrency matches upgrade configuration
- ✅ State transitions happen smoothly
- ✅ Total cluster count remains stable
- ✅ No clusters stuck in "Upgrading"
- ✅ Completed percentage increases over time

---

## Troubleshooting

### Configuration File Issues

**Problem**: JSON/YAML parsing error
```bash
# Verify JSON
jq . < your-config.json

# Verify YAML
yamllint your-config.yaml
```

**Problem**: "no active test ID found"
```bash
# Solution: Run init first
./bin/simulate init --config ./config/your-config.json
```

**Problem**: Multiple test IDs in config
```bash
# Solution: Specify which one to operate on
./bin/simulate remove --config ./config/your-config.json --config-id infra-abc123-456789
```

### Resources Not Appearing

**Problem**: Resources not visible in Kubernetes

```bash
# Check kubeconfig
kubectl cluster-info

# Verify namespaces created
kubectl get namespaces -l infra-test-id=<your-id>

# Check ManagedClusters
kubectl get managedclusters -l infra-test-id=<your-id> -A

# Check ClusterCurators
kubectl get clustercurators -A
```

### Cleanup Issues

**Problem**: Resources won't delete / stuck with finalizers

```bash
# Try dry-run first
./bin/simulate remove --config ./config/your-config.json --dry-run

# Check finalizers
kubectl get namespaces -o yaml | grep finalizers

# Manual cleanup if needed
kubectl patch ns <namespace> -p '{"metadata":{"finalizers":null}}'
```

### Simulator Not Running

**Problem**: No clusters detected

```bash
# Check simulator logs
tail -f simulator.log

# Verify test ID in config
./bin/simulate query --config ./config/your-config.json

# Ensure controller has created ClusterCurators
kubectl get clustercurators -A
```

**Problem**: Simulator exits with errors

```bash
# Check for permission issues
kubectl auth can-i get managedclusters --as system:serviceaccount:default:default

# Verify kubeconfig
cat ~/.kube/config | grep current-context
```

---

## Environment Variables

- `SIMULATE_CONFIG` - Default config file path (alternative to `--config` flag)

### Usage

```bash
export SIMULATE_CONFIG=./config/infra_clusters.json
./bin/simulate init
./bin/simulate run
```

---

## Performance Characteristics

| Scale | Startup | Memory | CPU | Full Upgrade |
|-------|---------|--------|-----|--------------|
| 10 clusters | <1s | <5MB | <1% | 5-10m |
| 25 clusters | <1s | <10MB | <2% | 5-10m |
| 100 clusters | <2s | <50MB | <5% | 5-10m |

---

## Building from Source

```bash
# From the simulator directory
cd /home/jpacker/workspace_git/cluster-curator-rollout-controller/test/simulator

# Build simulator binary to ./bin/simulate
make build

# Binary location: ./bin/simulate
```

---

## Directory Structure

```
test/simulator/
├── simulate                    # Main executable
├── simulate.go                 # Entry point (root command setup)
├── cmd/                        # Command implementations
│   ├── init.go                # Initialize infrastructure
│   ├── run.go                 # Run simulator monitor
│   ├── query.go               # Query test IDs and counts
│   ├── remove.go              # Remove resources
│   └── reset.go               # Reset for testing
├── pkg/                       # Packages
│   ├── config/                # Configuration loading/saving
│   ├── kubernetes/            # Kubernetes client operations
│   ├── models/                # Data structures
│   ├── generator/             # Resource generation
│   ├── constants/             # Constants and labels
│   └── progress/              # Progress bar display
├── config/                    # Example configurations
│   ├── infra_clusters.json    # Production-like example
│   ├── synthetic_clusters.yaml# Complex example
│   └── simple_clusters.yaml   # Simple example
└── README.md                  # This file
```

---

## Best Practices

### Infrastructure Management

1. **Use Dry-Run**: Always test with `--dry-run` before deployment
2. **Monitor Config**: Review `activeTestIDs` to understand active deployments
3. **Regular Cleanup**: Use `query` to identify orphaned resources
4. **Version Control**: Keep config files in version control

### Simulator Integration

1. **Start Infrastructure First**: Run `init` before `run`
2. **Verify Connectivity**: Use `query` to confirm resources created
3. **Monitor Logs**: Check simulator.log for unexpected behavior
4. **Collect Evidence**: Save logs for troubleshooting failures

### Testing Workflows

1. **Fresh Start**: Use `init` → `run` for each test run
2. **Quick Retry**: Use `reset` to re-test without recreating resources
3. **Long Running**: Use `query` to verify resources persist
4. **Cleanup**: Use `remove` after testing completes

---

## Shared Configuration

The config file serves as source of truth:
- **Manager** (`init`): Reads config, writes `activeTestIDs` after creation
- **Monitor** (`run`): Reads config to get base versions and upgrade settings
- **Query** (`query`): Reads config to verify active test IDs
- **Remove** (`remove`): Reads config to find resources (optional with `--config-id`)
- **Reset** (`reset`): Reads config to get base versions for reset

---

## Error Handling

- **Graceful Degradation**: Tool continues with remaining resources if some fail
- **Error Logging**: Detailed errors written to error log files with timestamps
- **Resume Capability**: Many operations support `--config-id` to resume incomplete tasks
- **Finalizer Handling**: Automatically removes Kubernetes finalizers before deletion

---

**Status**: Production Ready | **Version**: 1.0

