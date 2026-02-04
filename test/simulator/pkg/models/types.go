package models

import "fmt"

// InfraTestConfig represents the top-level configuration for infra test manager
type InfraTestConfig struct {
	TotalClusters      int          `json:"totalClusters" yaml:"totalClusters"`
	Labels             []LabelDef   `json:"labels" yaml:"labels"`
	BaseVersions       []VersionDef `json:"baseVersions,omitempty" yaml:"baseVersions,omitempty"`
	FailurePercentage  int          `json:"failurePercentage,omitempty" yaml:"failurePercentage,omitempty"`   // Percentage of upgrades that should fail (0-100)
	UpgradeTimeSeconds int          `json:"upgradeTimeSeconds,omitempty" yaml:"upgradeTimeSeconds,omitempty"` // Base time for upgrade simulation in seconds
	ActiveTestIDs      []string     `json:"activeTestIDs,omitempty" yaml:"activeTestIDs,omitempty"`
	TestID             string       `json:"-" yaml:"-"` // Auto-generated unique ID (not in file)
}

// LabelDef defines a label with optional parent key/value constraints
type LabelDef struct {
	Key         string       `json:"key" yaml:"key"`
	ParentKey   string       `json:"parentKey,omitempty" yaml:"parentKey,omitempty"`
	ParentValue string       `json:"parentValue,omitempty" yaml:"parentValue,omitempty"`
	Values      []LabelValue `json:"values" yaml:"values"`
}

// LabelValue defines a single label value with optional percentage
type LabelValue struct {
	Value      string `json:"value" yaml:"value"`
	Percentage int    `json:"percentage,omitempty" yaml:"percentage,omitempty"`
}

// VersionDef defines an OpenShift version with distribution percentage
type VersionDef struct {
	Version    string `json:"version" yaml:"version"`
	Percentage int    `json:"percentage,omitempty" yaml:"percentage,omitempty"`
}

// InfraTestCluster represents a generated cluster with its labels
type InfraTestCluster struct {
	Name        string
	Labels      map[string]string
	BaseVersion string // OpenShift base version for this cluster
}

// ValidationError is returned when config validation fails
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// GetLabelKeys returns all unique label keys defined in the config
func (c *InfraTestConfig) GetLabelKeys() []string {
	keys := make([]string, 0, len(c.Labels))
	for _, label := range c.Labels {
		keys = append(keys, label.Key)
	}
	return keys
}

// SyntheticConfig is an alias for backwards compatibility
type SyntheticConfig = InfraTestConfig

// SyntheticCluster is an alias for backwards compatibility
type SyntheticCluster = InfraTestCluster

// PrintLabelKeyTree prints a tree visualization of label key relationships
func (c *InfraTestConfig) PrintLabelKeyTree() string {
	if len(c.Labels) == 0 {
		return ""
	}

	var result string
	result = "\nLabel Key Hierarchy:\n"
	result += "====================\n"

	// Build parent-child relationships for keys
	// Map: parentKey -> list of child LabelDefs
	children := make(map[string][]LabelDef)
	roots := []LabelDef{}

	for _, label := range c.Labels {
		if label.ParentKey == "" {
			roots = append(roots, label)
		} else {
			children[label.ParentKey] = append(children[label.ParentKey], label)
		}
	}

	// Recursive function to print tree
	var printNode func(label LabelDef, prefix string, isLast bool, isRoot bool)
	printNode = func(label LabelDef, prefix string, isLast bool, isRoot bool) {
		// Build values string with percentages
		valuesStr := ""
		for i, v := range label.Values {
			if i > 0 {
				valuesStr += ", "
			}
			if v.Percentage > 0 {
				valuesStr += fmt.Sprintf("%s (%d%%)", v.Value, v.Percentage)
			} else {
				valuesStr += v.Value
			}
		}

		// Add constraint info if this has a parent value filter
		constraintStr := ""
		if label.ParentValue != "" {
			constraintStr = fmt.Sprintf(" [only when %s=%s]", label.ParentKey, label.ParentValue)
		}

		if isRoot {
			// Root node - no connector
			result += fmt.Sprintf("%s: %s\n", label.Key, valuesStr)
		} else {
			// Child node with tree connector
			connector := "├── "
			if isLast {
				connector = "└── "
			}
			result += fmt.Sprintf("%s%s%s: %s%s\n", prefix, connector, label.Key, valuesStr, constraintStr)
		}

		// Get children of this key
		childLabels := children[label.Key]
		if len(childLabels) == 0 {
			return
		}

		// New prefix for children
		var newPrefix string
		if isRoot {
			newPrefix = ""
		} else if isLast {
			newPrefix = prefix + "    "
		} else {
			newPrefix = prefix + "│   "
		}

		// Group children by parentValue for better visualization
		// First, children with no parentValue filter (apply to all)
		// Then, children with specific parentValue filters
		noFilter := []LabelDef{}
		withFilter := make(map[string][]LabelDef) // parentValue -> labels

		for _, child := range childLabels {
			if child.ParentValue == "" {
				noFilter = append(noFilter, child)
			} else {
				withFilter[child.ParentValue] = append(withFilter[child.ParentValue], child)
			}
		}

		// Count total children for determining isLast
		totalChildren := len(noFilter)
		for _, v := range withFilter {
			totalChildren += len(v)
		}

		childIdx := 0

		// Print children without filter first
		for _, child := range noFilter {
			childIdx++
			isLastChild := childIdx == totalChildren
			printNode(child, newPrefix, isLastChild, false)
		}

		// Print children with filters, grouped by parent value
		filterKeys := make([]string, 0, len(withFilter))
		for k := range withFilter {
			filterKeys = append(filterKeys, k)
		}

		for _, parentVal := range filterKeys {
			filteredChildren := withFilter[parentVal]
			for _, child := range filteredChildren {
				childIdx++
				isLastChild := childIdx == totalChildren
				printNode(child, newPrefix, isLastChild, false)
			}
		}
	}

	// Print each root
	for _, root := range roots {
		printNode(root, "", true, true)
	}

	return result
}
