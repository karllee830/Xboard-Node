package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/karllee830/Xboard-Node/internal/config"
	"gopkg.in/yaml.v3"
)

func TestConfigInitEnablesDetailedStatisticsForMachine(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yml")
	credentialsPath := filepath.Join(tempDir, "credentials.env")
	metaPath := filepath.Join(tempDir, "install-meta.json")

	err := runConfigInit([]string{
		"--mode", "machine",
		"--panel-url", "https://panel.example.com",
		"--token", "test-token",
		"--machine-id", "13",
		"--kernel", "singbox",
		"--health-port", "65530",
		"--statistics-enabled", "true",
		"--install-root", "/etc/xboard-node",
		"--output", configPath,
		"--credentials-out", credentialsPath,
		"--meta", metaPath,
		"--version", "dev",
	})
	if err != nil {
		t.Fatal(err)
	}

	root := readGeneratedRoot(t, configPath)
	if len(root.Instances) != 1 {
		t.Fatalf("instances=%d, want 1", len(root.Instances))
	}
	instance := root.Instances[0]
	if !instance.Statistics.Enabled {
		t.Fatal("detailed statistics should be enabled by default")
	}
	if instance.Statistics.MaxHourlyDimensions != 200000 ||
		instance.Statistics.MaxDomainsPerUser != 5000 ||
		instance.Statistics.MaxDestIPsPerUser != 10000 ||
		instance.Statistics.MaxPendingBatches != 168 ||
		instance.Statistics.RequestTimeout != 30 {
		t.Fatalf("unexpected statistics defaults: %+v", instance.Statistics)
	}
	if instance.Statistics.SpoolPath != "" {
		t.Fatalf("spool_path=%q, want empty for per-node machine isolation", instance.Statistics.SpoolPath)
	}
}

func TestConfigInitDisablesDetailedStatisticsForXray(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yml")

	err := runConfigInit([]string{
		"--mode", "node",
		"--panel-url", "https://panel.example.com",
		"--token", "test-token",
		"--node-id", "9",
		"--kernel", "xray",
		"--statistics-enabled", "true",
		"--install-root", "/etc/xboard-node",
		"--output", configPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	root := readGeneratedRoot(t, configPath)
	if len(root.Instances) != 1 {
		t.Fatalf("instances=%d, want 1", len(root.Instances))
	}
	if root.Instances[0].Statistics.Enabled {
		t.Fatal("xray must not enable Sing-box detailed statistics")
	}
}

func readGeneratedRoot(t *testing.T, path string) config.RootConfig {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var root config.RootConfig
	if err := yaml.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	return root
}
