package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rkbkosp/codex-wake/internal/model"
	_ "modernc.org/sqlite"
)

type DB struct{ sql *sql.DB }

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	dsn := (&url.URL{Scheme: "file", Path: path}).String() + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		db.Close()
		return nil, fmt.Errorf("secure sqlite database: %w", err)
	}
	return &DB{sql: db}, nil
}

func (d *DB) Close() error { return d.sql.Close() }

func (d *DB) MigrateLocal(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS waits (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    event_type TEXT NOT NULL,
    repository_key TEXT NOT NULL,
    repository_display TEXT NOT NULL,
    pr_number INTEGER NOT NULL CHECK (pr_number > 0),
    pr_url TEXT NOT NULL,
    codex_thread_id TEXT NOT NULL,
    continuation TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('ARMED','READY','DELIVERING','DELIVERED','CANCELLED')),
    external_event_id TEXT NOT NULL DEFAULT '',
    merge_commit_sha TEXT NOT NULL DEFAULT '',
    merged_at TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    event_received_at INTEGER NOT NULL DEFAULT 0,
    delivered_at INTEGER NOT NULL DEFAULT 0,
    cancelled_at INTEGER NOT NULL DEFAULT 0,
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    last_delivery_error TEXT NOT NULL DEFAULT '',
    next_delivery_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS waits_state_next_idx ON waits(state, next_delivery_at, created_at);
DROP INDEX IF EXISTS waits_external_event_idx;
`)
	return err
}

func (d *DB) MigrateRelay(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS github_pr_merges (
    repository_key TEXT NOT NULL,
    pr_number INTEGER NOT NULL CHECK (pr_number > 0),
    delivery_id TEXT NOT NULL UNIQUE,
    merge_commit_sha TEXT NOT NULL DEFAULT '',
    merged_at TEXT NOT NULL DEFAULT '',
    received_at INTEGER NOT NULL,
    PRIMARY KEY(repository_key, pr_number)
);
`)
	return err
}

func NewWaitID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return "wait_" + id.String(), nil
}

func (d *DB) RegisterWait(ctx context.Context, w model.Wait) (model.Wait, error) {
	if w.ID == "" {
		id, err := NewWaitID()
		if err != nil {
			return model.Wait{}, err
		}
		w.ID = id
	}
	w.Provider = model.ProviderGitHub
	w.EventType = model.EventPRMerged
	w.RepositoryKey = strings.ToLower(w.RepositoryKey)
	w.State = model.StateArmed
	w.CreatedAt = model.UnixNow()
	_, err := d.sql.ExecContext(ctx, `INSERT INTO waits (
id, provider, event_type, repository_key, repository_display, pr_number, pr_url,
codex_thread_id, continuation, state, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, w.ID, w.Provider, w.EventType, w.RepositoryKey, w.RepositoryDisplay, w.PRNumber, w.PRURL, w.CodexThreadID, w.Continuation, w.State, w.CreatedAt)
	if err != nil {
		return model.Wait{}, fmt.Errorf("insert wait: %w", err)
	}
	return w, nil
}

const waitColumns = `id, provider, event_type, repository_key, repository_display, pr_number, pr_url,
codex_thread_id, continuation, state, external_event_id, merge_commit_sha, merged_at,
created_at, event_received_at, delivered_at, cancelled_at, delivery_attempts,
last_delivery_error, next_delivery_at`

type scanner interface{ Scan(...any) error }

func scanWait(s scanner) (model.Wait, error) {
	var w model.Wait
	err := s.Scan(&w.ID, &w.Provider, &w.EventType, &w.RepositoryKey, &w.RepositoryDisplay, &w.PRNumber, &w.PRURL,
		&w.CodexThreadID, &w.Continuation, &w.State, &w.ExternalEventID, &w.MergeCommitSHA, &w.MergedAt,
		&w.CreatedAt, &w.EventReceivedAt, &w.DeliveredAt, &w.CancelledAt, &w.DeliveryAttempts,
		&w.LastDeliveryError, &w.NextDeliveryAt)
	return w, err
}

func (d *DB) GetWait(ctx context.Context, id string) (model.Wait, error) {
	w, err := scanWait(d.sql.QueryRowContext(ctx, `SELECT `+waitColumns+` FROM waits WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return model.Wait{}, fmt.Errorf("wait %q not found", id)
	}
	return w, err
}

func (d *DB) ListWaits(ctx context.Context, state *model.State) ([]model.Wait, error) {
	query := `SELECT ` + waitColumns + ` FROM waits`
	var args []any
	if state != nil {
		query += ` WHERE state = ?`
		args = append(args, *state)
	}
	query += ` ORDER BY created_at DESC, id DESC`
	rows, err := d.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Wait
	for rows.Next() {
		w, err := scanWait(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (d *DB) CancelWait(ctx context.Context, id string) (model.Wait, error) {
	res, err := d.sql.ExecContext(ctx, `UPDATE waits SET state = ?, cancelled_at = ? WHERE id = ? AND state = ?`, model.StateCancelled, model.UnixNow(), id, model.StateArmed)
	if err != nil {
		return model.Wait{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return model.Wait{}, err
	}
	if n == 0 {
		w, getErr := d.GetWait(ctx, id)
		if getErr != nil {
			return model.Wait{}, getErr
		}
		return model.Wait{}, fmt.Errorf("wait %q cannot be cancelled from state %s", id, w.State)
	}
	return d.GetWait(ctx, id)
}

func (d *DB) AcceptEvent(ctx context.Context, envelope model.EventEnvelope) (model.Wait, bool, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return model.Wait{}, false, err
	}
	defer tx.Rollback()
	w, err := scanWait(tx.QueryRowContext(ctx, `SELECT `+waitColumns+` FROM waits WHERE id = ?`, envelope.SubscriptionID))
	if err != nil {
		return model.Wait{}, false, err
	}
	if w.State != model.StateArmed {
		return w, false, nil
	}
	if err := envelope.ValidateAgainst(w); err != nil {
		return model.Wait{}, false, err
	}
	now := model.UnixNow()
	res, err := tx.ExecContext(ctx, `UPDATE waits SET state = ?, external_event_id = ?, merge_commit_sha = ?, merged_at = ?, event_received_at = ?, next_delivery_at = ? WHERE id = ? AND state = ?`,
		model.StateReady, envelope.EventID, envelope.Event.MergeCommitSHA, envelope.Event.MergedAt, now, now, w.ID, model.StateArmed)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return w, false, nil
		}
		return model.Wait{}, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return w, false, err
	}
	if err := tx.Commit(); err != nil {
		return model.Wait{}, false, err
	}
	w.State = model.StateReady
	w.ExternalEventID = envelope.EventID
	w.MergeCommitSHA = envelope.Event.MergeCommitSHA
	w.MergedAt = envelope.Event.MergedAt
	w.EventReceivedAt = now
	w.NextDeliveryAt = now
	return w, true, nil
}

func (d *DB) RecoverDelivering(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx, `UPDATE waits SET state = ?, next_delivery_at = ? WHERE state = ?`, model.StateReady, model.UnixNow(), model.StateDelivering)
	return err
}

func (d *DB) ClaimReady(ctx context.Context) (model.Wait, bool, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return model.Wait{}, false, err
	}
	defer tx.Rollback()
	w, err := scanWait(tx.QueryRowContext(ctx, `SELECT `+waitColumns+` FROM waits WHERE state = ? AND next_delivery_at <= ? ORDER BY next_delivery_at, created_at LIMIT 1`, model.StateReady, model.UnixNow()))
	if errors.Is(err, sql.ErrNoRows) {
		return model.Wait{}, false, nil
	}
	if err != nil {
		return model.Wait{}, false, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE waits SET state = ? WHERE id = ? AND state = ?`, model.StateDelivering, w.ID, model.StateReady)
	if err != nil {
		return model.Wait{}, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return model.Wait{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return model.Wait{}, false, err
	}
	w.State = model.StateDelivering
	return w, true, nil
}

func (d *DB) DeliverySucceeded(ctx context.Context, id string) error {
	res, err := d.sql.ExecContext(ctx, `UPDATE waits SET state = ?, delivered_at = ?, delivery_attempts = delivery_attempts + 1, last_delivery_error = '' WHERE id = ? AND state = ?`, model.StateDelivered, model.UnixNow(), id, model.StateDelivering)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("wait %q is no longer DELIVERING", id)
	}
	return nil
}

func (d *DB) DeliveryFailed(ctx context.Context, id, message string, retryAt time.Time) error {
	res, err := d.sql.ExecContext(ctx, `UPDATE waits SET state = ?, delivery_attempts = delivery_attempts + 1, last_delivery_error = ?, next_delivery_at = ? WHERE id = ? AND state = ?`, model.StateReady, message, retryAt.UTC().Unix(), id, model.StateDelivering)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("wait %q is no longer DELIVERING", id)
	}
	return nil
}

type MergeFact struct {
	RepositoryKey  string
	PRNumber       int64
	DeliveryID     string
	MergeCommitSHA string
	MergedAt       string
	ReceivedAt     int64
}

func (f MergeFact) Event() model.ExternalEvent {
	return model.ExternalEvent{Provider: model.ProviderGitHub, Type: model.EventPRMerged, Repository: f.RepositoryKey, PRNumber: f.PRNumber, MergeCommitSHA: f.MergeCommitSHA, MergedAt: f.MergedAt}
}

func (d *DB) PutMergeFact(ctx context.Context, f MergeFact) (MergeFact, bool, error) {
	f.RepositoryKey = strings.ToLower(f.RepositoryKey)
	if f.ReceivedAt == 0 {
		f.ReceivedAt = model.UnixNow()
	}
	res, err := d.sql.ExecContext(ctx, `INSERT OR IGNORE INTO github_pr_merges (repository_key, pr_number, delivery_id, merge_commit_sha, merged_at, received_at) VALUES (?, ?, ?, ?, ?, ?)`, f.RepositoryKey, f.PRNumber, f.DeliveryID, f.MergeCommitSHA, f.MergedAt, f.ReceivedAt)
	if err != nil {
		return MergeFact{}, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return MergeFact{}, false, err
	}
	if n == 1 {
		return f, true, nil
	}
	existing, ok, err := d.GetMergeFact(ctx, f.RepositoryKey, f.PRNumber)
	if err != nil {
		return MergeFact{}, false, err
	}
	if ok {
		return existing, false, nil
	}
	// A duplicate delivery id for a different PR is invalid rather than a new event.
	return MergeFact{}, false, fmt.Errorf("delivery id %q was already used for another event", f.DeliveryID)
}

func (d *DB) GetMergeFact(ctx context.Context, repository string, prNumber int64) (MergeFact, bool, error) {
	var f MergeFact
	err := d.sql.QueryRowContext(ctx, `SELECT repository_key, pr_number, delivery_id, merge_commit_sha, merged_at, received_at FROM github_pr_merges WHERE repository_key = ? AND pr_number = ?`, strings.ToLower(repository), prNumber).Scan(&f.RepositoryKey, &f.PRNumber, &f.DeliveryID, &f.MergeCommitSHA, &f.MergedAt, &f.ReceivedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return MergeFact{}, false, nil
	}
	return f, err == nil, err
}
