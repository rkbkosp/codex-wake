package daemon

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rkbkosp/codex-wake/internal/model"
	"github.com/rkbkosp/codex-wake/internal/relay"
	"github.com/rkbkosp/codex-wake/internal/storage"
)

func TestRelayClientReconnectSyncReplaysOfflineMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relayDB, err := storage.Open(filepath.Join(t.TempDir(), "relay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer relayDB.Close()
	if err := relayDB.MigrateRelay(ctx); err != nil {
		t.Fatal(err)
	}
	fact := storage.MergeFact{RepositoryKey: "example/repo", PRNumber: 77, DeliveryID: "01900000-0000-7000-8000-000000000002", MergeCommitSHA: "abcdef1", MergedAt: "2026-08-31T08:00:00Z"}
	if _, inserted, err := relayDB.PutMergeFact(ctx, fact); err != nil || !inserted {
		t.Fatalf("fact: %v %v", inserted, err)
	}
	hub := relay.NewHub(relayDB)
	relayServer := &relay.Server{DB: relayDB, Hub: hub, WebhookSecret: []byte("secret"), DeviceTokens: map[string]string{"mac": "token"}}
	httpServer := httptest.NewServer(relayServer.Handler())
	defer httpServer.Close()
	localDB, err := storage.Open(filepath.Join(t.TempDir(), "local.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer localDB.Close()
	if err := localDB.MigrateLocal(ctx); err != nil {
		t.Fatal(err)
	}
	w, err := localDB.RegisterWait(ctx, model.Wait{RepositoryKey: "example/repo", RepositoryDisplay: "Example/Repo", PRNumber: 77, PRURL: "https://github.com/Example/Repo/pull/77", CodexThreadID: "01900000-0000-7000-8000-000000000001"})
	if err != nil {
		t.Fatal(err)
	}
	q := &recordingQueuer{}
	service := NewService(localDB, q)
	client := NewRelayClient("ws"+strings.TrimPrefix(httpServer.URL, "http")+"/v1/ws", "mac", "token", service)
	service.Notifier = client
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Run(ctx) }()
	workerDone := make(chan error, 1)
	go func() { workerDone <- service.RunDeliveryWorker(ctx) }()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := localDB.GetWait(ctx, w.ID)
		if got.State == model.StateDelivered {
			if got.ExternalEventID != fact.DeliveryID {
				t.Fatalf("event: %+v", got)
			}
			cancel()
			<-clientDone
			<-workerDone
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("offline terminal fact was not replayed and delivered")
}
