package config

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jpacker/cluster-curator-synthetic-test/pkg/models"
	"gopkg.in/yaml.v3"
)

// LoadConfig loads and validates the configuration file (supports JSON and YAML)
func LoadConfig(configPath string) (*models.InfraTestConfig, error) {
	return loadConfigInternal(configPath, true)
}

// LoadConfigMinimal loads the configuration file without validation.
// This is used when --config-id is provided and the config file values are not needed.
// Only activeTestIDs are extracted; other fields may be empty/zero.
func LoadConfigMinimal(configPath string) (*models.InfraTestConfig, error) {
	return loadConfigInternal(configPath, false)
}

// loadConfigInternal is the shared implementation for loading config
func loadConfigInternal(configPath string, validate bool) (*models.InfraTestConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg models.InfraTestConfig

	// Determine format by file extension
	ext := filepath.Ext(configPath)
	if ext == ".json" {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
	} else {
		// Default to YAML
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse YAML: %w", err)
		}
	}

	// Validate config only if requested
	if validate {
		if err := validateConfig(&cfg); err != nil {
			return nil, err
		}
	}

	// Use existing test ID from activeTestIDs if available
	// NOTE: Do NOT generate new IDs here - only the init command should generate new IDs
	if len(cfg.ActiveTestIDs) > 0 {
		// Use the most recent (last) active test ID as the current one
		cfg.TestID = cfg.ActiveTestIDs[len(cfg.ActiveTestIDs)-1]
	}
	// If no activeTestIDs, TestID remains empty - caller must handle this appropriately

	return &cfg, nil
}

// GenerateNewTestID generates a new unique test ID for fresh deployments
// This should be called by the init command when starting a new deployment
func GenerateNewTestID(configPath string) (string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("failed to read config file: %w", err)
	}
	return generateTestID(data), nil
}

// SaveConfig saves the configuration back to file with updated test IDs
func SaveConfig(configPath string, cfg *models.InfraTestConfig) error {
	ext := filepath.Ext(configPath)
	var data []byte
	var err error

	if ext == ".json" {
		data, err = json.MarshalIndent(cfg, "", "  ")
	} else {
		data, err = yaml.Marshal(cfg)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// AddTestID adds a test ID to the config's active list
func AddTestID(cfg *models.InfraTestConfig, testID string) {
	// Check if already exists
	for _, id := range cfg.ActiveTestIDs {
		if id == testID {
			return // Already exists
		}
	}
	cfg.ActiveTestIDs = append(cfg.ActiveTestIDs, testID)
}

// RemoveTestID removes a test ID from the config's active list
func RemoveTestID(cfg *models.InfraTestConfig, testID string) bool {
	for i, id := range cfg.ActiveTestIDs {
		if id == testID {
			// Remove by swapping with last and truncating
			cfg.ActiveTestIDs[i] = cfg.ActiveTestIDs[len(cfg.ActiveTestIDs)-1]
			cfg.ActiveTestIDs = cfg.ActiveTestIDs[:len(cfg.ActiveTestIDs)-1]
			return true
		}
	}
	return false
}

// generateTestID creates a unique ID based on the config content and timestamp
// Format: infra-{sha256-first-12-chars}-{timestamp-short}
func generateTestID(configData []byte) string {
	// Hash the config content for reproducibility
	hash := sha256.Sum256(configData)
	configHash := fmt.Sprintf("%x", hash)[:12]

	// Add timestamp for uniqueness if config is same
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	timestampShort := timestamp[len(timestamp)-6:] // Last 6 digits

	return fmt.Sprintf("infra-%s-%s", configHash, timestampShort)
}

// validateConfig performs basic validation on the configuration
func validateConfig(cfg *models.InfraTestConfig) error {
	if cfg.TotalClusters <= 0 {
		return &models.ValidationError{Message: "totalClusters must be greater than 0"}
	}

	if len(cfg.Labels) == 0 {
		return &models.ValidationError{Message: "at least one label definition must be provided"}
	}

	// Validate baseVersions if provided
	if len(cfg.BaseVersions) > 0 {
		totalPercentage := 0
		for i, ver := range cfg.BaseVersions {
			if ver.Version == "" {
				return &models.ValidationError{Message: fmt.Sprintf("baseVersions[%d]: version is required", i)}
			}
			if ver.Percentage < 0 || ver.Percentage > 100 {
				return &models.ValidationError{Message: fmt.Sprintf("baseVersions[%d] (%s): percentage must be between 0 and 100", i, ver.Version)}
			}
			totalPercentage += ver.Percentage
		}
		if totalPercentage > 100 {
			return &models.ValidationError{Message: fmt.Sprintf("baseVersions: total percentage (%d) exceeds 100", totalPercentage)}
		}
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
