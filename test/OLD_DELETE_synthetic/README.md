# Synthetic ClusterCuratorRollout Test Tool

A Go-based tool for generating and managing synthetic Kubernetes infrastructure to test ClusterCuratorRollout performance and behavior.

## Features

- **Unique Test ID**: Automatic generation of unique test ID for safe resource tracking
- **Hierarchical Label Distribution**: Define labels with optional parent-child relationships
- **Flexible Percentage Logic**: Automatic distribution of remaining percentages
- **Parallel Resource Creation**: Fast parallel creation/deletion of Kubernetes resources
- **Dry-run Mode**: Preview changes before applying to cluster
- **Simple YAML Configuration**: Define your test scenario in a single config file
- **Query Command**: Helper command to generate kubectl queries by test ID

## Architecture

```
./test/synthetic/
├── synthetic_test_tool.go    # Main CLI entry point
├── cmd/
│   ├── create.go            # Create command implementation
│   └── remove.go            # Remove command implementation
├── pkg/
│   ├── config/
│   │   └── loader.go        # YAML config parsing & validation
│   ├── generator/
│   │   ├── distributor.go   # Label distribution logic
│   │   └── cr_builder.go    # Kubernetes resource builders
│   ├── kubernetes/
│   │   └── client.go        # K8s API client wrapper
│   └── models/
│       └── types.go         # Data structures
├── config/
│   └── synthetic_clusters.yaml   # Example configuration
└── go.mod
```

## Installation

### Requirements
- Go 1.21+
- kubectl configured with access to your cluster
- Kubernetes 1.24+

### Build

```bash
cd test/synthetic
go mod download
go build -o synthetic-test-tool ./
```

## Usage

### Create Test Infrastructure

```bash
# Basic usage
./synthetic-test-tool create config/synthetic_clusters.yaml

# With specific namespace for ManagedClusters
./synthetic-test-tool create config/synthetic_clusters.yaml --namespace acm-tests

# Dry-run to preview changes
./synthetic-test-tool create config/synthetic_clusters.yaml --dry-run

# With custom kubeconfig
./synthetic-test-tool create config/synthetic_clusters.yaml --kubeconfig ~/.kube/custom-config
```

### Remove Test Infrastructure

```bash
# Basic usage
./synthetic-test-tool remove config/synthetic_clusters.yaml

# With specific namespace
./synthetic-test-tool remove config/synthetic_clusters.yaml --namespace acm-tests
```

## Configuration

### YAML Schema

```yaml
totalClusters: 100

labels:
  - key: "environment"           # Label key name
    values:
      - value: "production"      # Label value
        percentage: 60           # Optional: percentage distribution
      - value: "staging"
        percentage: 30
      - value: "dev"             # No percentage: gets remaining 10%
  
  - key: "region"
    parentKey: "environment"     # Optional: apply only to clusters with this key
    parentValue: "production"    # Optional: apply only to clusters with key=value
    values:
      - value: "us-east"
        percentage: 50
      - value: "us-west"
        percentage: 50
```

### Configuration Rules

1. **No Parent** (root level):
   - Label applied to all clusters
   - Uses percentage logic

2. **Parent Key Only**:
   - Label applied to all clusters that have the parent key
   - Percentage is calculated from those filtered clusters

3. **Parent Key + Value**:
   - Label applied only to clusters matching parent key=value
   - Percentage is calculated from those filtered clusters

### Percentage Logic

- **Explicit percentages**: Values with specified percentages are allocated accordingly
- **No percentages**: Values without percentages receive equal distribution of remaining percentage
- **Partial percentages**: If all values have percentages totaling < 100:
  - Only those values get the label (remaining clusters skip this label)
- **Example**: 
  - 60% prod, 30% staging, dev unspecified → dev gets 10%
  - 40% high, 40% medium, no others → only 80 clusters get tier label

## Resources Created

For each synthetic cluster, the tool creates:

### 1. Namespace
- Name: `curator-{cluster-name}`
- Purpose: Isolated environment for cluster curator resources

### 2. ManagedCluster
- Name: `synthetic-cluster-{N}`
- Namespace: `default` (configurable)
- Labels: All distributed labels from configuration
- Example labels: environment=production, region=us-east, tier=premium

### 3. ClusterCurator
- Name: `curator-synthetic-cluster-{N}`
- Namespace: `curator-{cluster-name}`
- Owner: References ManagedCluster for cleanup
- Purpose: Defines rollout strategy for the cluster

## Example Scenarios

### Scenario 1: Simple Environment Distribution
```yaml
totalClusters: 100

labels:
  - key: "environment"
    values:
      - value: "prod"
        percentage: 60
      - value: "staging"
        percentage: 30
      - value: "dev"    # 10% remaining
```

Result: 60 prod, 30 staging, 10 dev clusters

### Scenario 2: Regional Distribution Within Environment
```yaml
totalClusters: 100

labels:
  - key: "environment"
    values:
      - value: "prod"
        percentage: 60

  - key: "region"
    parentKey: "environment"
    parentValue: "prod"
    values:
      - value: "us-east"
        percentage: 60
      - value: "us-west"
        percentage: 40
```

Result: 36 prod+us-east (60% of 60), 24 prod+us-west (40% of 60)

### Scenario 3: Multi-Level Hierarchy
```yaml
totalClusters: 100

labels:
  - key: "environment"
    values:
      - value: "prod"
        percentage: 60

  - key: "tier"
    parentKey: "environment"
    parentValue: "prod"
    values:
      - value: "premium"
        percentage: 40

  - key: "gpu"
    parentKey: "tier"
    parentValue: "premium"
    values:
      - value: "enabled"
        percentage: 100
```

Result: 24 clusters (40% of 60 prod) with all three labels

## Performance

- **Parallel Creation**: Uses goroutines for 3x concurrent resource creation
- **Typical Performance**:
  - 100 clusters: ~5-10 seconds
  - 1000 clusters: ~30-60 seconds
  - Depends on cluster API latency

## Troubleshooting

### Connection Errors
```
Error: failed to build kubeconfig
```
- Ensure kubectl is configured: `kubectl cluster-info`
- Specify kubeconfig: `--kubeconfig ~/.kube/config`

### Validation Errors
```
Error: parentKey 'environment' not found in previous labels
```
- Parent key must be defined in a previous label
- Check label order in YAML configuration

### Resource Not Found During Removal
- Non-fatal: tool continues removal of other resources
- Check if resources were already deleted manually

## Development

### Running Tests
```bash
go test ./...
```

### Building a Binary
```bash
go build -o synthetic-test-tool ./
```

### Code Structure

- `models/types.go`: Data structures for config and clusters
- `config/loader.go`: YAML parsing and validation
- `generator/distributor.go`: Label distribution algorithm
- `generator/cr_builder.go`: Kubernetes resource generation
- `kubernetes/client.go`: K8s API operations

## Contributing

To add features:

1. Update data structures in `models/types.go`
2. Modify distribution logic in `distributor.go`
3. Update CR builders in `cr_builder.go`
4. Add commands in `cmd/` directory
5. Update example config

## License

Part of cluster-curator-rollout-controller project

## Related Documentation

- [Kubernetes Client-Go](https://github.com/kubernetes/client-go)
- [Cobra CLI Framework](https://cobra.dev/)
- [Open Cluster Management](https://open-cluster-management.io/)

