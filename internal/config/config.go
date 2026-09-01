package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

type Local struct {
	RelayURL             string `toml:"relay_url"`
	DeviceID             string `toml:"device_id"`
	DeviceToken          string `toml:"device_token"`
	CodexBinary          string `toml:"codex_binary"`
	QueueRetryMaxSeconds int    `toml:"queue_retry_max_seconds"`
}

func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".codex-wait"), nil
}

func DefaultSocket(dataDir string) string { return filepath.Join(dataDir, "run", "waitd.sock") }

func LoadLocal(path string) (Local, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Local{}, nil
	}
	if err != nil {
		return Local{}, fmt.Errorf("read config: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return Local{}, fmt.Errorf("stat config: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return Local{}, fmt.Errorf("config %s must not be accessible by group or others; run chmod 600", path)
	}
	var c Local
	if err := toml.Unmarshal(b, &c); err != nil {
		return Local{}, fmt.Errorf("parse config: %w", err)
	}
	return c, nil
}
