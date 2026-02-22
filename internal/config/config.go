// Package config manages WVC configuration and the .wvc directory structure.
// It handles loading, saving, and initializing the repository configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

const (
	WVCDir       = ".wvc"
	ConfigFile   = "config"
	DatabaseFile = "wvc.db"
	SnapshotsDir = "snapshots"
)

// Config represents the WVC configuration
type Config struct {
	WeaviateURL   string `toml:"weaviate_url"`
	ServerVersion string `toml:"server_version"` // Detected Weaviate server version on init
	path          string // path to .wvc directory
}

// FindWVCRoot finds the .wvc directory by walking up from current directory
func FindWVCRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		wvcPath := filepath.Join(dir, WVCDir)
		if info, err := os.Stat(wvcPath); err == nil && info.IsDir() {
			return wvcPath, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("not a wvc repository (or any parent up to root)")
		}
		dir = parent
	}
}

// Load loads the configuration from the .wvc directory
func Load() (*Config, error) {
	wvcPath, err := FindWVCRoot()
	if err != nil {
		return nil, err
	}

	configPath := filepath.Join(wvcPath, ConfigFile)
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	cfg.path = wvcPath
	return &cfg, nil
}

// Save saves the configuration to disk
func (c *Config) Save() error {
	configPath := filepath.Join(c.path, ConfigFile)
	data, err := toml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	return os.WriteFile(configPath, data, 0644)
}

// WVCPath returns the path to the .wvc directory
func (c *Config) WVCPath() string {
	return c.path
}

// DatabasePath returns the path to the bbolt database
func (c *Config) DatabasePath() string {
	return filepath.Join(c.path, DatabaseFile)
}

// SnapshotsPath returns the path to the snapshots directory
func (c *Config) SnapshotsPath() string {
	return filepath.Join(c.path, SnapshotsDir)
}

// Initialize creates a new .wvc directory with initial configuration
func Initialize(weaviateURL string) (*Config, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	wvcPath := filepath.Join(cwd, WVCDir)

	// Check if already initialized
	if _, err := os.Stat(wvcPath); err == nil {
		return nil, fmt.Errorf("wvc repository already exists")
	}

	// Create directories
	if err := os.MkdirAll(wvcPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create .wvc directory: %w", err)
	}

	snapshotsPath := filepath.Join(wvcPath, SnapshotsDir)
	if err := os.MkdirAll(snapshotsPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshots directory: %w", err)
	}

	cfg := &Config{
		WeaviateURL: weaviateURL,
		path:        wvcPath,
	}

	if err := cfg.Save(); err != nil {
		// Cleanup on failure
		os.RemoveAll(wvcPath)
		return nil, err
	}

	return cfg, nil
}

// SupportsCursorPagination returns true if the server version supports cursor pagination
func (c *Config) SupportsCursorPagination() bool {
	if c.ServerVersion == "" {
		// Default to cursor pagination if version unknown
		return true
	}

	// Parse major.minor.patch version
	var major, minor int
	_, err := fmt.Sscanf(c.ServerVersion, "%d.%d", &major, &minor)
	if err != nil {
		return true // Default to cursor on parse error
	}

	// Cursor pagination (WithAfter) requires Weaviate 1.18+
	return major > 1 || (major == 1 && minor >= 18)
}
