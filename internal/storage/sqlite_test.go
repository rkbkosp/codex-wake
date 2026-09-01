package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rkbkosp/codex-wake/internal/model"
)

func localTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.MigrateLocal(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestLocalWaitLifecycleAndEventDedup(t *testing.T) {
	t.Parallel()
	db := localTestDB(t)
	ctx := context.Background()
	w, err := db.RegisterWait(ctx, model.Wait{RepositoryKey: "Example/Repo", RepositoryDisplay: "Example/Repo", PRNumber: 7, PRURL: "https://github.com/Example/Repo/pull/7", CodexThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	envelope := model.EventEnvelope{Type: "event", Version: 1, SubscriptionID: w.ID, EventID: "delivery-1", Event: model.ExternalEvent{Provider: model.ProviderGitHub, Type: model.EventPRMerged, Repository: "example/repo", PRNumber: 7, MergedAt: "2026-08-31T08:00:00Z"}}
	if _, accepted, err := db.AcceptEvent(ctx, envelope); err != nil || !accepted {
		t.Fatalf("accept event: accepted=%v err=%v", accepted, err)
	}
	if _, accepted, err := db.AcceptEvent(ctx, envelope); err != nil || accepted {
		t.Fatalf("duplicate event: accepted=%v err=%v", accepted, err)
	}
	claimed, ok, err := db.ClaimReady(ctx)
	if err != nil || !ok || claimed.State != model.StateDelivering {
		t.Fatalf("claim: %+v %v %v", claimed, ok, err)
	}
	if err := db.DeliverySucceeded(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetWait(ctx, w.ID)
	if err != nil || got.State != model.StateDelivered || got.DeliveryAttempts != 1 {
		t.Fatalf("delivered: %+v %v", got, err)
	}
}

func TestOneMergeEventCanSatisfyMultipleWaits(t *testing.T) {
	t.Parallel()
	db := localTestDB(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		w, err := db.RegisterWait(ctx, model.Wait{RepositoryKey: "example/repo", RepositoryDisplay: "example/repo", PRNumber: 8, PRURL: "https://github.com/example/repo/pull/8", CodexThreadID: "thread"})
		if err != nil {
			t.Fatal(err)
		}
		e := model.EventEnvelope{Type: "event", Version: 1, SubscriptionID: w.ID, EventID: "same-delivery", Event: model.ExternalEvent{Provider: model.ProviderGitHub, Type: model.EventPRMerged, Repository: "example/repo", PRNumber: 8, MergedAt: "2026-08-31T08:00:00Z"}}
		if _, accepted, err := db.AcceptEvent(ctx, e); err != nil || !accepted {
			t.Fatalf("wait %d: %v %v", i, accepted, err)
		}
	}
}

func TestCancelledWaitRejectsLaterMerge(t *testing.T) {
	t.Parallel()
	db := localTestDB(t)
	ctx := context.Background()
	w, err := db.RegisterWait(ctx, model.Wait{RepositoryKey: "a/b", RepositoryDisplay: "a/b", PRNumber: 11, PRURL: "https://github.com/a/b/pull/11", CodexThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CancelWait(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	e := model.EventEnvelope{Type: "event", Version: 1, SubscriptionID: w.ID, EventID: "event-cancel", Event: model.ExternalEvent{Provider: model.ProviderGitHub, Type: model.EventPRMerged, Repository: "a/b", PRNumber: 11, MergedAt: "2026-08-31T08:00:00Z"}}
	if got, accepted, err := db.AcceptEvent(ctx, e); err != nil || accepted || got.State != model.StateCancelled {
		t.Fatalf("cancelled event: %+v %v %v", got, accepted, err)
	}
}

func TestRelayMergeFactIsTerminalAndDeduplicated(t *testing.T) {
	t.Parallel()
	db, err := Open(filepath.Join(t.TempDir(), "relay.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if err := db.MigrateRelay(ctx); err != nil {
		t.Fatal(err)
	}
	fact := MergeFact{RepositoryKey: "Example/Repo", PRNumber: 9, DeliveryID: "d1", MergeCommitSHA: "abc"}
	if _, inserted, err := db.PutMergeFact(ctx, fact); err != nil || !inserted {
		t.Fatalf("insert: %v %v", inserted, err)
	}
	if got, inserted, err := db.PutMergeFact(ctx, fact); err != nil || inserted || got.DeliveryID != "d1" {
		t.Fatalf("duplicate: %+v %v %v", got, inserted, err)
	}
	if got, inserted, err := db.PutMergeFact(ctx, MergeFact{RepositoryKey: "example/repo", PRNumber: 9, DeliveryID: "d2", MergeCommitSHA: "changed"}); err != nil || inserted || got.MergeCommitSHA != "abc" {
		t.Fatalf("terminal fact changed: %+v %v %v", got, inserted, err)
	}
}
