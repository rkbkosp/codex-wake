package codex

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQueueUsesArgumentBoundaries(t *testing.T) {
	dir := t.TempDir()
	record := filepath.Join(dir, "args")
	script := filepath.Join(dir, "fake-codex")
	body := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CODEX_WAIT_TEST_RECORD\"\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_WAIT_TEST_RECORD", record)
	message := "hello; $(touch /tmp/never-run)\nsecond line"
	if err := (Binary{Path: script}).Queue(context.Background(), "thread-1", message); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, "thread-1\n--message\n"+message+"\n") {
		t.Fatalf("arguments were not preserved: %q", got)
	}
}
