package ipc_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rkbkosp/codex-wake/internal/daemon"
	"github.com/rkbkosp/codex-wake/internal/ipc"
	"github.com/rkbkosp/codex-wake/internal/model"
	"github.com/rkbkosp/codex-wake/internal/protocol"
	"github.com/rkbkosp/codex-wake/internal/storage"
)

type queueRecorder struct {
	mu       sync.Mutex
	messages []string
}

func (q *queueRecorder) Queue(_ context.Context, thread, message string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = append(q.messages, thread+"\n"+message)
	return nil
}

func TestRegisterDebugFireAndDeliverOverIPC(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "cw-ipc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	db, err := storage.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := db.MigrateLocal(ctx); err != nil {
		t.Fatal(err)
	}
	q := &queueRecorder{}
	service := daemon.NewService(db, q)
	socket := filepath.Join(dir, "waitd.sock")
	server, err := ipc.Listen(socket, service.Handle)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()
	workerDone := make(chan error, 1)
	go func() { workerDone <- service.RunDeliveryWorker(ctx) }()
	client := ipc.Client{SocketPath: socket}
	params := protocol.RegisterParams{Event: model.EventPRMerged, RepositoryKey: "example/repo", RepositoryDisplay: "Example/Repo", PRNumber: 5, PRURL: "https://github.com/Example/Repo/pull/5", ThreadID: "01900000-0000-7000-8000-000000000001", Continuation: "continue safely"}
	var registered protocol.RegisterResult
	if err := client.Call(ctx, "wait.register", params, &registered); err != nil {
		t.Fatal(err)
	}
	if registered.Wait.State != model.StateArmed || registered.Relay != "offline" {
		t.Fatalf("register: %+v", registered)
	}
	var ready model.Wait
	if err := client.Call(ctx, "wait.debug_fire", protocol.IDParams{ID: registered.Wait.ID}, &ready); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var got model.Wait
		if err := client.Call(ctx, "wait.status", protocol.IDParams{ID: registered.Wait.ID}, &got); err != nil {
			t.Fatal(err)
		}
		if got.State == model.StateDelivered {
			q.mu.Lock()
			count := len(q.messages)
			q.mu.Unlock()
			if count != 1 {
				t.Fatalf("queue calls: %d", count)
			}
			cancel()
			<-serveDone
			<-workerDone
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("debug wait was not delivered")
}
