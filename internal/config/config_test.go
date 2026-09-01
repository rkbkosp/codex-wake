package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadLocalRequiresPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("codex_binary = \"/bin/true\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLocal(path); err == nil {
		t.Fatal("expected public config permissions to be rejected")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadLocal(path)
	if err != nil || got.CodexBinary != "/bin/true" {
		t.Fatalf("config: %+v %v", got, err)
	}
}
