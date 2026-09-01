package ipc

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rkbkosp/codex-wake/internal/protocol"
)

func TestListenDoesNotReplaceLiveDaemonSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "cw-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "waitd.sock")
	handler := func(context.Context, protocol.Request) protocol.Response { return protocol.Response{} }
	first, err := Listen(path, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := Listen(path, handler); err == nil {
		second.Close()
		t.Fatal("second daemon replaced a live socket")
	}
}
