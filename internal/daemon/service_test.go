package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rkbkosp/codex-wake/internal/model"
	"github.com/rkbkosp/codex-wake/internal/storage"
)

type recordingQueuer struct {
	mu       sync.Mutex
	calls    []model.Wait
	messages []string
	fail     int
}

func (q *recordingQueuer) Queue(_ context.Context, thread, message string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = append(q.messages, thread+"\n"+message)
	if q.fail > 0 {
		q.fail--
		return errors.New("app server unavailable")
	}
	return nil
}

func TestDebugEventDeliveryLifecycle(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := db.MigrateLocal(ctx); err != nil {
		t.Fatal(err)
	}
	q := &recordingQueuer{}
	s := NewService(db, q)
	w, err := db.RegisterWait(ctx, model.Wait{RepositoryKey: "a/b", RepositoryDisplay: "a/b", PRNumber: 1, PRURL: "https://github.com/a/b/pull/1", CodexThreadID: "01900000-0000-7000-8000-000000000001", Continuation: "next"})
	if err != nil {
		t.Fatal(err)
	}
	envelope := model.EventEnvelope{Type: "event", Version: 1, SubscriptionID: w.ID, EventID: "event-1", Event: model.ExternalEvent{Provider: model.ProviderGitHub, Type: model.EventPRMerged, Repository: "a/b", PRNumber: 1, MergeCommitSHA: "abcdef1", MergedAt: "2026-08-31T08:00:00Z"}}
	if _, accepted, err := s.AcceptEvent(ctx, envelope); err != nil || !accepted {
		t.Fatalf("accept: %v %v", accepted, err)
	}
	done := make(chan struct{})
	go func() { _ = s.RunDeliveryWorker(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := db.GetWait(ctx, w.ID)
		if got.State == model.StateDelivered {
			q.mu.Lock()
			calls := len(q.messages)
			message := q.messages[0]
			q.mu.Unlock()
			if calls != 1 || !containsAll(message, []string{"wait_id: " + w.ID, "event_id: event-1", "Continuation note", "next"}) {
				t.Fatalf("message: %q", message)
			}
			cancel()
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("wait was not delivered")
}

func TestQueueFailureIsRetriedFromDurableReadyState(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := db.MigrateLocal(ctx); err != nil {
		t.Fatal(err)
	}
	q := &recordingQueuer{fail: 1}
	s := NewService(db, q)
	s.RetryMaxInterval = 10 * time.Millisecond
	w, err := db.RegisterWait(ctx, model.Wait{RepositoryKey: "a/b", RepositoryDisplay: "a/b", PRNumber: 2, PRURL: "https://github.com/a/b/pull/2", CodexThreadID: "01900000-0000-7000-8000-000000000001"})
	if err != nil {
		t.Fatal(err)
	}
	envelope := model.EventEnvelope{Type: "event", Version: 1, SubscriptionID: w.ID, EventID: "event-retry", Event: model.ExternalEvent{Provider: model.ProviderGitHub, Type: model.EventPRMerged, Repository: "a/b", PRNumber: 2, MergedAt: "2026-08-31T08:00:00Z"}}
	if _, accepted, err := s.AcceptEvent(ctx, envelope); err != nil || !accepted {
		t.Fatalf("accept: %v %v", accepted, err)
	}
	done := make(chan struct{})
	go func() { _ = s.RunDeliveryWorker(ctx); close(done) }()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := db.GetWait(ctx, w.ID)
		if got.State == model.StateDelivered {
			q.mu.Lock()
			calls := len(q.messages)
			q.mu.Unlock()
			if calls != 2 || got.DeliveryAttempts != 2 {
				t.Fatalf("calls=%d wait=%+v", calls, got)
			}
			cancel()
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("queue failure was not retried")
}

func containsAll(value string, parts []string) bool {
	for _, p := range parts {
		if !stringsContains(value, p) {
			return false
		}
	}
	return true
}

func stringsContains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return part == ""
}
