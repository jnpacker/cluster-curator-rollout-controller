package generator

import (
	"fmt"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/models"
)

// Distributor handles the distribution of labels across synthetic clusters
type Distributor struct {
	totalClusters int
	clusters      []models.SyntheticCluster
}

// NewDistributor creates a new Distributor instance
func NewDistributor(totalClusters int) *Distributor {
	clusters := make([]models.SyntheticCluster, totalClusters)
	for i := 0; i < totalClusters; i++ {
		clusters[i] = models.SyntheticCluster{
			Name:   fmt.Sprintf("synthetic-cluster-%d", i+1),
			Labels: make(map[string]string),
		}
	}
	return &Distributor{
		totalClusters: totalClusters,
		clusters:      clusters,
	}
}

// Distribute applies all label definitions to clusters
func (d *Distributor) Distribute(labels []models.LabelDef) error {
	for i, labelDef := range labels {
		// Determine target cluster indices
		targetIndices := d.getTargetClusters(labelDef, i)

		if len(targetIndices) == 0 {
			// Skip if no targets (shouldn't happen for first label)
			continue
		}

		// Apply label distribution to target clusters
		if err := d.applyLabel(labelDef, targetIndices); err != nil {
			return fmt.Errorf("failed to apply label '%s': %w", labelDef.Key, err)
		}
	}

	return nil
}

// getTargetClusters determines which cluster indices should receive a label
func (d *Distributor) getTargetClusters(labelDef models.LabelDef, labelIndex int) []int {
	if labelDef.ParentKey == "" {
		// No parent: apply to all clusters
		indices := make([]int, d.totalClusters)
		for i := 0; i < d.totalClusters; i++ {
			indices[i] = i
		}
		return indices
	}

	// Filter to clusters with parent key (optionally matching parent value)
	var targetIndices []int
	for i := 0; i < d.totalClusters; i++ {
		clusterLabel, exists := d.clusters[i].Labels[labelDef.ParentKey]
		if !exists {
			continue
		}

		// If parentValue specified, must match
		if labelDef.ParentValue != "" && clusterLabel != labelDef.ParentValue {
			continue
		}

		targetIndices = append(targetIndices, i)
	}

	return targetIndices
}

// applyLabel applies a label definition to target clusters
func (d *Distributor) applyLabel(labelDef models.LabelDef, targetIndices []int) error {
	targetCount := len(targetIndices)

	// Calculate distribution
	distribution := calculateDistribution(labelDef.Values, targetCount)

	// Apply labels to clusters
	clusterIdx := 0
	for valueIdx, count := range distribution {
		value := labelDef.Values[valueIdx]
		for i := 0; i < count; i++ {
			d.clusters[targetIndices[clusterIdx]].Labels[labelDef.Key] = value.Value
			clusterIdx++
		}
	}

	return nil
}

// calculateDistribution calculates how many clusters should get each value
func calculateDistribution(values []models.LabelValue, totalClusters int) []int {
	distribution := make([]int, len(values))

	// Separate values with and without percentages
	specifiedIndices := make([]int, 0)
	unspecifiedIndices := make([]int, 0)
	specifiedTotal := 0

	for i, val := range values {
		if val.Percentage > 0 {
			specifiedIndices = append(specifiedIndices, i)
			specifiedTotal += val.Percentage
		} else {
			unspecifiedIndices = append(unspecifiedIndices, i)
		}
	}

	// Assign specified percentages
	clustersAssigned := 0
	for _, i := range specifiedIndices {
		count := (totalClusters * values[i].Percentage) / 100
		distribution[i] = count
		clustersAssigned += count
	}

	remaining := totalClusters - clustersAssigned

	if len(unspecifiedIndices) > 0 {
		// Distribute remaining equally among unspecified values
		perValue := remaining / len(unspecifiedIndices)
		extra := remaining % len(unspecifiedIndices)

		for j, i := range unspecifiedIndices {
			distribution[i] = perValue
			if j < extra {
				distribution[i]++
			}
		}
	} else if specifiedTotal < 100 {
		// All specified but sum < 100: only use the specified percentages
		// Don't distribute the remaining to anyone
		// (clusters simply won't get this label)
	} else {
		// All specified and sum >= 100: shouldn't have remaining
		// This is edge case, but we've already assigned based on percentages
	}

	return distribution
}

// GetClusters returns the generated clusters
func (d *Distributor) GetClusters() []models.SyntheticCluster {
	return d.clusters
}



