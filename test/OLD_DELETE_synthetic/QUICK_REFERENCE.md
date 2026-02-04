# Synthetic Test Tool - Quick Reference

## Location
```
/home/jpacker/workspace_git/cluster-curator-rollout-controller/test/synthetic/
```

## Quick Start

### 1. Build
```bash
cd test/synthetic
make build
```

### 2. Test (No cluster required)
```bash
./synthetic-test-tool create config/simple_clusters.yaml --dry-run
```

### 3. Create Resources (On your cluster)
```bash
./synthetic-test-tool create config/synthetic_clusters.yaml
```

### 4. Cleanup
```bash
./synthetic-test-tool remove config/synthetic_clusters.yaml
```

## Commands

### Create
```bash
./synthetic-test-tool create <config.yaml> [options]

Options:
  --namespace string      Namespace for ManagedCluster (default "default")
  --kubeconfig string     Path to kubeconfig file
  --dry-run              Preview without applying (default false)
```

### Remove
```bash
./synthetic-test-tool remove <config.yaml> [options]

Options:
  --namespace string      Namespace for ManagedCluster (default "default")
  --kubeconfig string     Path to kubeconfig file
```

## YAML Configuration

### Basic Structure
```yaml
totalClusters: 100

labels:
  - key: "label-key"
    parentKey: "parent-key"      # Optional
    parentValue: "parent-value"  # Optional
    values:
      - value: "value1"
        percentage: 60           # Optional
      - value: "value2"
        percentage: 40           # Optional
      - value: "value3"          # No percentage: gets remaining
```

### Parent Key Rules

**No parent** (root level):
```yaml
- key: "environment"
  values:
    - value: "prod"
      percentage: 60
```
→ Applied to all clusters

**Parent key only** (filter by key):
```yaml
- key: "tier"
  parentKey: "environment"
  values:
    - value: "premium"
      percentage: 50
```
→ Applied to all clusters with environment key

**Parent key + value** (filter by key=value):
```yaml
- key: "region"
  parentKey: "environment"
  parentValue: "production"
  values:
    - value: "us-east"
      percentage: 50
```
→ Applied only to production clusters

## Configuration Examples

### Example 1: Simple (10 clusters)
```bash
cat config/simple_clusters.yaml
```

### Example 2: Complex (100 clusters, 7 labels)
```bash
cat config/synthetic_clusters.yaml
```

## Percentage Logic

| Scenario | Example | Result |
|----------|---------|--------|
| Explicit | 60%, 30%, unspecified | 60%, 30%, 10% |
| No percentage | Both unspecified | Equal split (50%, 50%) |
| Partial | 30%, 20% total | Only 30 and 20 clusters get label |

## Output Files

### Resources Created Per Cluster
1. **Namespace**: `curator-synthetic-cluster-N`
2. **ManagedCluster**: `synthetic-cluster-N` (with all labels)
3. **ClusterCurator**: `curator-synthetic-cluster-N`

## Common Tasks

### Preview 100 clusters
```bash
./synthetic-test-tool create config/synthetic_clusters.yaml --dry-run
```

### Create 100 clusters in custom namespace
```bash
./synthetic-test-tool create config/synthetic_clusters.yaml --namespace testing
```

### Create simple 10-cluster test
```bash
./synthetic-test-tool create config/simple_clusters.yaml --namespace test
```

### Remove all test clusters
```bash
./synthetic-test-tool remove config/synthetic_clusters.yaml --namespace testing
```

## Troubleshooting

| Error | Solution |
|-------|----------|
| `parentKey not found` | Parent key must be defined in previous labels |
| `connection refused` | Check kubeconfig: `kubectl cluster-info` |
| `permission denied` | Check RBAC permissions for creating resources |
| `not found` (on remove) | Resources already deleted, can ignore |

## File Structure
```
test/synthetic/
├── synthetic-test-tool         # Executable binary
├── config/
│   ├── simple_clusters.yaml    # 10 clusters
│   └── synthetic_clusters.yaml # 100 clusters
├── README.md                   # Full documentation
├── IMPLEMENTATION_SUMMARY.md   # Implementation details
└── pkg/...                     # Source code
```

## Performance Notes
- 10 clusters: ~1-2 seconds
- 100 clusters: ~5-10 seconds
- 1000+ clusters: ~30-60 seconds
- Speed depends on cluster API latency

## Next Steps
1. Review `config/synthetic_clusters.yaml`
2. Modify for your test scenario
3. Run `--dry-run` to preview
4. Create resources without `--dry-run`
5. Test ClusterCuratorRollout
6. Run `remove` to cleanup



