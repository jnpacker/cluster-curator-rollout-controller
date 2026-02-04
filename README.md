# cluster-curator-rollout-controller

A Kubernetes controller that orchestrates hierarchical, controlled rollouts of OpenShift cluster upgrades across large fleets using ClusterCurator resources.

## Description

The ClusterCuratorRollout controller manages the orchestration of cluster upgrades by:
- Defining hierarchical selection strategies (group by labels, filter by selectors)
- Creating and managing Placement resources for cluster selection
- Creating ClusterCurator resources to trigger upgrades on selected clusters
- Controlling concurrency (percentage or count-based limits)
- Managing soak periods between rollout steps
- Tracking upgrade state across all clusters

## Hierarchical Selection Model

The CCR uses a hierarchical tree structure to define how clusters are selected and the order in which they're upgraded. There are three selection types:

| Type | Purpose |
|------|---------|
| `group` | Branches the rollout by a label key (e.g., region, environment) |
| `filter` | Adds label predicates to narrow cluster selection; defines rollout order |
| `placement` | References an existing Placement resource directly |

### How It Works

1. **Groups** branch the rollout tree by unique values of a label (e.g., `groupByLabelKey: region` creates branches for us-east, us-west, eu-west, etc.)

2. **Filters** add label predicates and define the rollout order. The order of filter children determines priority (first filter = first to upgrade)

3. **Leaf filters** (filters with no children) create Placement resources that select the matching clusters

### Selection Tree Example

```
ClusterCuratorRollout
└── group (by: environment)
    ├── filter (environment=staging)     ← 1st: Upgrade staging canaries
    │   └── filter (tier=canary)         ← Leaf: Creates Placement
    │       soakDuration: 24h            ← Wait 24h after staging completes
    │
    ├── filter (environment=dev)         ← 2nd: Upgrade dev
    │   └── filter (tier=standard)
    │       soakDuration: 12h            ← Wait 12h after dev completes
    │
    └── filter (environment=production)  ← 3rd: Upgrade production (last)
        ├── filter (tier=canary)         ← 3a: Production canaries first
        │   soakDuration: 48h            ← Wait 48h after canaries complete
        │
        └── group (by: region)           ← 3b: Then by region
            ├── filter (region=us-east)
            │   soakDuration: 48h
            ├── filter (region=us-west)
            │   soakDuration: 48h
            └── filter (region=eu-west)
```

## Soak Durations

Soak durations provide validation time between upgrade phases. There are two types:

### Step Soak (`soakDuration` on filter)

Wait time **after all clusters in a filter complete** before moving to the next sibling filter. Use this for validation periods between environments or regions.

```yaml
children:
  - type: filter
    labelSelector:
      matchLabels:
        environment: staging
    soakDuration: 24h  # Wait 24 hours after ALL staging clusters complete
    children:
      - type: filter
        labelSelector:
          matchLabels:
            tier: standard

  - type: filter
    labelSelector:
      matchLabels:
        environment: production
    # Production starts 24h after staging completes
```

### Batch Soak (`concurrency.soakDuration`)

Wait time **between batches of clusters within a single filter**. Use this for gradual rollouts within a group.

```yaml
- type: filter
  labelSelector:
    matchLabels:
      environment: production
  concurrency:
    type: percent
    value: 10           # Upgrade 10% of clusters at a time
    soakDuration: 2h    # Wait 2 hours between each 10% batch
```

## Concurrency Control

Control how many clusters upgrade simultaneously within a filter:

| Type | Description |
|------|-------------|
| `percent` | Percentage of total clusters (e.g., 25 = 25% at a time) |
| `count` | Absolute number of clusters (e.g., 5 = 5 clusters at a time) |

### Rolling vs Batch Mode

- **Without `concurrency.soakDuration`**: Rolling mode - as clusters complete, new ones start immediately (maintains concurrency level)
- **With `concurrency.soakDuration`**: Batch mode - wait for entire batch to complete, soak, then start next batch

## Complete Example: Enterprise Rollout Strategy

This example demonstrates a realistic enterprise upgrade strategy with:
- Staging canary validation (24h soak)
- Dev environment validation (12h soak)
- Production canaries first (48h soak)
- Production regional phasing (48h between regions)
- Gradual rollout within regions (25% at a time with 1h batch soak)

```yaml
apiVersion: rollout.open-cluster-management.io/v1alpha1
kind: ClusterCuratorRollout
metadata:
  name: enterprise-upgrade-4-16
  namespace: open-cluster-management
spec:
  openShiftVersion: "4.16.0"
  onlyUpgradeFromOpenShiftVersions:
    - "4.15"  # Only upgrade 4.15.x clusters
  onFailure: pause  # Stop on any failure

  # Top level: group by environment
  type: group
  groupByLabelKey: environment

  children:
    #############################################
    # PHASE 1: Staging Canaries (first validation)
    #############################################
    - type: filter
      labelSelector:
        matchLabels:
          environment: staging
      soakDuration: 24h  # 24-hour validation after staging completes
      children:
        - type: filter
          labelSelector:
            matchLabels:
              tier: canary
          concurrency:
            type: count
            value: 1  # One cluster at a time in staging

    #############################################
    # PHASE 2: Development (broader validation)
    #############################################
    - type: filter
      labelSelector:
        matchLabels:
          environment: dev
      soakDuration: 12h  # 12-hour validation after dev completes
      children:
        - type: filter
          labelSelector:
            matchLabels:
              tier: standard
          concurrency:
            type: percent
            value: 50  # 50% of dev clusters at a time

    #############################################
    # PHASE 3: Production
    #############################################
    - type: filter
      labelSelector:
        matchLabels:
          environment: production
      children:
        ###########################################
        # PHASE 3a: Production Canaries First
        ###########################################
        - type: filter
          labelSelector:
            matchLabels:
              tier: canary
          soakDuration: 48h  # 48-hour validation after prod canaries
          concurrency:
            type: count
            value: 1  # One canary at a time

        ###########################################
        # PHASE 3b: Production by Region
        ###########################################
        - type: group
          groupByLabelKey: region
          children:
            # Region 1: US East (primary)
            - type: filter
              labelSelector:
                matchLabels:
                  region: us-east
              soakDuration: 48h  # 48-hour soak after US East
              children:
                - type: filter
                  labelSelector:
                    matchLabels:
                      tier: standard
                  concurrency:
                    type: percent
                    value: 25
                    soakDuration: 1h  # 1-hour soak between 25% batches

            # Region 2: US West
            - type: filter
              labelSelector:
                matchLabels:
                  region: us-west
              soakDuration: 48h  # 48-hour soak after US West
              children:
                - type: filter
                  labelSelector:
                    matchLabels:
                      tier: standard
                  concurrency:
                    type: percent
                    value: 25
                    soakDuration: 1h

            # Region 3: EU West (last region, no soak after)
            - type: filter
              labelSelector:
                matchLabels:
                  region: eu-west
              children:
                - type: filter
                  labelSelector:
                    matchLabels:
                      tier: standard
                  concurrency:
                    type: percent
                    value: 25
                    soakDuration: 1h
```

### Timeline for 100 Production Clusters per Region (+ 3 Canaries)

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Staging canaries (2 clusters) | ~2 hours | 2 hours |
| Staging soak | 24 hours | 26 hours |
| Dev (50% batches) | ~2 hours | 28 hours |
| Dev soak | 12 hours | 40 hours |
| Production canaries (3 clusters) | ~3 hours | 43 hours |
| Production canary soak | 48 hours | 91 hours |
| US East (4 batches × 25%, 1h soak each) | ~7 hours | 98 hours |
| US East soak | 48 hours | 146 hours |
| US West (4 batches × 25%, 1h soak each) | ~7 hours | 153 hours |
| US West soak | 48 hours | 201 hours |
| EU West (4 batches × 25%, 1h soak each) | ~7 hours | 208 hours |
| **Total** | | **~8.7 days** |

## Simple Examples

### Placement Reference

Use an existing Placement resource:

```yaml
apiVersion: rollout.open-cluster-management.io/v1alpha1
kind: ClusterCuratorRollout
metadata:
  name: simple-rollout
  namespace: open-cluster-management
spec:
  openShiftVersion: "4.16.0"
  type: placement
  placementRef:
    name: my-existing-placement
    namespace: open-cluster-management
```

### Single Filter with Concurrency

Upgrade all production clusters, 10% at a time:

```yaml
apiVersion: rollout.open-cluster-management.io/v1alpha1
kind: ClusterCuratorRollout
metadata:
  name: production-rollout
  namespace: open-cluster-management
spec:
  openShiftVersion: "4.16.0"
  type: filter
  labelSelector:
    matchLabels:
      environment: production
  concurrency:
    type: percent
    value: 10
```

### One Cluster at a Time with Soak

Ultra-conservative rollout for critical infrastructure:

```yaml
apiVersion: rollout.open-cluster-management.io/v1alpha1
kind: ClusterCuratorRollout
metadata:
  name: critical-infra-rollout
  namespace: open-cluster-management
spec:
  openShiftVersion: "4.16.0"
  onFailure: pause
  type: filter
  labelSelector:
    matchLabels:
      tier: critical
  concurrency:
    type: count
    value: 1           # One cluster at a time
    soakDuration: 4h   # 4-hour validation after each cluster
```

## Failure Handling

### OnFailure Policy

The `spec.onFailure` field controls how the rollout behaves when a cluster upgrade fails:

| Value | Behavior |
|-------|----------|
| `continue` (default) | Failures are recorded but the rollout continues with other clusters. Failed clusters remain in their failed state. |
| `pause` | The rollout pauses immediately when a failure is detected. The status phase is set to `Paused` and no further upgrades are initiated until manual intervention. |

### Example Configuration

```yaml
apiVersion: rollout.open-cluster-management.io/v1alpha1
kind: ClusterCuratorRollout
metadata:
  name: production-upgrade
spec:
  openShiftVersion: "4.16.0"
  onFailure: pause  # Stop on first failure
  selection:
    # ... selection configuration
```

### Handling Failed Upgrades

When a cluster upgrade fails, the ClusterCurator resource for that cluster will show a failed status. The ClusterCurator controller handles its own retry logic internally.

**To manually retry a failed upgrade:**

1. Investigate the failure by checking the ClusterCurator status:
   ```sh
   kubectl get clustercurator <cluster-name> -n <cluster-name> -o yaml
   ```

2. Fix the underlying issue (if any) on the cluster

3. Delete the ClusterCurator to trigger a retry:
   ```sh
   kubectl delete clustercurator <cluster-name> -n <cluster-name>
   ```

4. The ClusterCuratorRollout controller will automatically recreate the ClusterCurator on its next reconcile, initiating a fresh upgrade attempt.

### Resuming a Paused Rollout

When `onFailure: pause` is configured and a failure occurs:

1. The rollout status phase will be set to `Paused`
2. The status message will indicate how many clusters failed
3. No new upgrades will be initiated

**Automatic Resume:**

The rollout will automatically resume when all failed ClusterCurators are deleted. Simply delete the failed ClusterCurator(s) to trigger a retry:

```sh
kubectl delete clustercurator <cluster-name> -n <cluster-name>
```

The controller will detect that no failures remain and:
1. Change the phase from `Paused` to `Processing`
2. Recreate the deleted ClusterCurator(s) to retry the upgrade
3. Continue processing remaining clusters

If the retry fails again, the rollout will pause once more.

**Manual Resume (alternative):**

If you want to resume without retrying failed clusters:

1. Change `onFailure` to `continue`:
   ```sh
   kubectl patch clustercuratorrollout <name> --type=merge -p '{"spec":{"onFailure":"continue"}}'
   ```

2. Clear the paused phase:
   ```sh
   kubectl patch clustercuratorrollout <name> --type=merge --subresource=status -p '{"status":{"phase":"Processing"}}'
   ```

**Note:** You can switch back to `onFailure: pause` after resuming if you want to pause on the next failure.

## Getting Started

### Prerequisites
- go version v1.23.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/cluster-curator-rollout-controller:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/cluster-curator-rollout-controller:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/cluster-curator-rollout-controller:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/cluster-curator-rollout-controller/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v1-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

