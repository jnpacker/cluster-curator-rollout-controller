package models

// SyntheticConfig represents the top-level configuration for synthetic cluster generation
type SyntheticConfig struct {
	TotalClusters int         `yaml:"totalClusters"`
	Labels        []LabelDef  `yaml:"labels"`
	TestID        string      `yaml:"-"` // Auto-generated unique ID (not in YAML)
}

// LabelDef defines a label with optional parent key/value constraints
type LabelDef struct {
	Key         string       `yaml:"key"`
	ParentKey   string       `yaml:"parentKey,omitempty"`
	ParentValue string       `yaml:"parentValue,omitempty"`
	Values      []LabelValue `yaml:"values"`
}

// LabelValue defines a single label value with optional percentage
type LabelValue struct {
	Value      string `yaml:"value"`
	Percentage int    `yaml:"percentage,omitempty"`
}

// SyntheticCluster represents a generated cluster with its labels
type SyntheticCluster struct {
	Name   string
	Labels map[string]string
}

// ValidationError is returned when config validation fails
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

