package codex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Queuer interface {
	Queue(context.Context, string, string) error
}

type Binary struct {
	Path    string
	Timeout time.Duration
}

func (b Binary) Queue(ctx context.Context, threadID, message string) error {
	if !isAbsoluteExecutable(b.Path) {
		return errors.New("codex binary must be an absolute executable path")
	}
	if strings.TrimSpace(threadID) == "" || strings.TrimSpace(message) == "" {
		return errors.New("thread id and message are required")
	}
	timeout := b.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// Deliberately use direct argv execution. No event field is interpreted by a shell.
	cmd := exec.CommandContext(ctx, b.Path, "queue", "--thread", threadID, "--message", message)
	output, err := cmd.CombinedOutput()
	if err != nil {
		text := strings.TrimSpace(string(output))
		if len(text) > 2048 {
			text = text[:2048]
		}
		if text == "" {
			text = err.Error()
		}
		return fmt.Errorf("codex queue failed: %s", text)
	}
	return nil
}

func (b Binary) Check(ctx context.Context) (string, error) {
	if !isAbsoluteExecutable(b.Path) {
		return "", errors.New("codex binary must be an absolute executable path")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, b.Path, "queue", "--help")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("codex queue --help failed: %w", err)
	}
	text := string(output)
	if !strings.Contains(text, "--thread") || !strings.Contains(text, "--message") {
		return "", errors.New("codex queue does not expose required --thread and --message options")
	}
	return "available", nil
}

func isAbsoluteExecutable(path string) bool {
	if !strings.HasPrefix(path, "/") {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode().Perm()&0o111 != 0
}
