package config

import (
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/models"
	"gopkg.in/yaml.v3"
)

// LoadConfig loads and validates the YAML configuration file
func LoadConfig(filepath string) (*models.SyntheticConfig, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg models.SyntheticConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Validate config
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	// Generate unique test ID based on config hash
	cfg.TestID = generateTestID(data)

	return &cfg, nil
}

// generateTestID creates a unique ID based on the config content and timestamp
// Format: synth-{sha256-first-12-chars}-{timestamp-short}
func generateTestID(configData []byte) string {
	// Hash the config content for reproducibility
	hash := sha256.Sum256(configData)
	configHash := fmt.Sprintf("%x", hash)[:12]

	// Add timestamp for uniqueness if config is same
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	timestampShort := timestamp[len(timestamp)-6:] // Last 6 digits

	return fmt.Sprintf("synth-%s-%s", configHash, timestampShort)
}

// validateConfig performs basic validation on the configuration
func validateConfig(cfg *models.SyntheticConfig) error {
	if cfg.TotalClusters <= 0 {
		return &models.ValidationError{Message: "totalClusters must be greater than 0"}
	}

	if len(cfg.Labels) == 0 {
		return &models.ValidationError{Message: "at least one label definition must be provided"}
	}

	// Track which keys exist (for parent validation)
	keyExists := make(map[string]bool)

	for i, labelDef := range cfg.Labels {
		if labelDef.Key == "" {
			return &models.ValidationError{Message: fmt.Sprintf("label[%d]: key is required", i)}
		}

		if len(labelDef.Values) == 0 {
			return &models.ValidationError{Message: fmt.Sprintf("label[%d] (%s): at least one value must be provided", i, labelDef.Key)}
		}

		// Validate values
		for j, val := range labelDef.Values {
			if val.Value == "" {
				return &models.ValidationError{Message: fmt.Sprintf("label[%d].values[%d]: value is required", i, j)}
			}
			if val.Percentage < 0 || val.Percentage > 100 {
				return &models.ValidationError{Message: fmt.Sprintf("label[%d].values[%d] (%s): percentage must be between 0 and 100", i, j, val.Value)}
			}
		}

		// Mark key as existing
		keyExists[labelDef.Key] = true

		// Validate parent references
		if labelDef.ParentKey != "" {
			// Parent key must be defined in previous labels
			if !keyExists[labelDef.ParentKey] {
				return &models.ValidationError{Message: fmt.Sprintf("label[%d]: parentKey '%s' not found in previous labels", i, labelDef.ParentKey)}
			}
		}
	}

	return nil
}

