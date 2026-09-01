package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rkbkosp/codex-wake/internal/codex"
	"github.com/rkbkosp/codex-wake/internal/model"
	"github.com/rkbkosp/codex-wake/internal/protocol"
	"github.com/rkbkosp/codex-wake/internal/storage"
	"github.com/rkbkosp/codex-wake/internal/version"
)

type SubscriptionNotifier interface {
	Add(model.Subscription)
	Remove(string)
}

type Service struct {
	DB               *storage.DB
	Queuer           codex.Queuer
	CodexCheck       func(context.Context) (string, error)
	Notifier         SubscriptionNotifier
	RetryMaxInterval time.Duration
	relayConnected   atomic.Bool
	deliveryWake     chan struct{}
}

func NewService(db *storage.DB, queuer codex.Queuer) *Service {
	return &Service{DB: db, Queuer: queuer, RetryMaxInterval: 5 * time.Minute, deliveryWake: make(chan struct{}, 1)}
}

func (s *Service) SetRelayConnected(connected bool) { s.relayConnected.Store(connected) }
func (s *Service) RelayState() string {
	if s.relayConnected.Load() {
		return "connected"
	}
	return "offline"
}

func response(result any) protocol.Response {
	b, err := json.Marshal(result)
	if err != nil {
		return failure("internal", err)
	}
	return protocol.Response{Result: b}
}

func failure(code string, err error) protocol.Response {
	return protocol.Response{Error: &protocol.Error{Code: code, Message: err.Error()}}
}

func decodeParams(raw json.RawMessage, dst any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON values are not allowed")
	}
	return nil
}

func (s *Service) Handle(ctx context.Context, req protocol.Request) protocol.Response {
	if req.Version != protocol.Version {
		return failure("unsupported_version", fmt.Errorf("protocol version %d is not supported", req.Version))
	}
	switch req.Method {
	case "wait.register":
		return s.handleRegister(ctx, req.Params)
	case "wait.list":
		waits, err := s.DB.ListWaits(ctx, nil)
		if err != nil {
			return failure("storage_error", err)
		}
		return response(waits)
	case "wait.status":
		var p protocol.IDParams
		if err := decodeParams(req.Params, &p); err != nil || p.ID == "" {
			if err == nil {
				err = errors.New("wait_id is required")
			}
			return failure("invalid_params", err)
		}
		w, err := s.DB.GetWait(ctx, p.ID)
		if err != nil {
			return failure("not_found", err)
		}
		return response(w)
	case "wait.cancel":
		var p protocol.IDParams
		if err := decodeParams(req.Params, &p); err != nil || p.ID == "" {
			if err == nil {
				err = errors.New("wait_id is required")
			}
			return failure("invalid_params", err)
		}
		w, err := s.DB.CancelWait(ctx, p.ID)
		if err != nil {
			return failure("invalid_state", err)
		}
		if s.Notifier != nil {
			s.Notifier.Remove(p.ID)
		}
		return response(w)
	case "wait.debug_fire":
		return s.handleDebugFire(ctx, req.Params)
	case "daemon.doctor":
		return s.handleDoctor(ctx)
	default:
		return failure("method_not_found", fmt.Errorf("unknown method %q", req.Method))
	}
}

func (s *Service) handleRegister(ctx context.Context, raw json.RawMessage) protocol.Response {
	var p protocol.RegisterParams
	if err := decodeParams(raw, &p); err != nil {
		return failure("invalid_params", err)
	}
	if p.Event != model.EventPRMerged {
		return failure("invalid_params", fmt.Errorf("unsupported event %q", p.Event))
	}
	ref, err := model.ParseGitHubPRURL(p.PRURL)
	if err != nil {
		return failure("invalid_params", err)
	}
	if ref.RepositoryKey != strings.ToLower(p.RepositoryKey) || ref.RepositoryDisplay != p.RepositoryDisplay || ref.Number != p.PRNumber {
		return failure("invalid_params", errors.New("PR URL fields do not match"))
	}
	parsedThread, err := uuid.Parse(p.ThreadID)
	if err != nil || parsedThread.String() != p.ThreadID {
		return failure("invalid_params", errors.New("thread_id must be a canonical UUID"))
	}
	if len(p.Continuation) > 8192 || strings.ContainsRune(p.Continuation, '\x00') {
		return failure("invalid_params", errors.New("continuation must be at most 8192 bytes and contain no NUL"))
	}
	w, err := s.DB.RegisterWait(ctx, model.Wait{Provider: model.ProviderGitHub, EventType: model.EventPRMerged, RepositoryKey: ref.RepositoryKey, RepositoryDisplay: ref.RepositoryDisplay, PRNumber: ref.Number, PRURL: ref.URL, CodexThreadID: p.ThreadID, Continuation: p.Continuation})
	if err != nil {
		return failure("storage_error", err)
	}
	if s.Notifier != nil {
		s.Notifier.Add(w.Subscription())
	}
	return response(protocol.RegisterResult{Wait: w, Relay: s.RelayState()})
}

func (s *Service) handleDebugFire(ctx context.Context, raw json.RawMessage) protocol.Response {
	var p protocol.IDParams
	if err := decodeParams(raw, &p); err != nil || p.ID == "" {
		if err == nil {
			err = errors.New("wait_id is required")
		}
		return failure("invalid_params", err)
	}
	w, err := s.DB.GetWait(ctx, p.ID)
	if err != nil {
		return failure("not_found", err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return failure("internal", err)
	}
	envelope := model.EventEnvelope{Type: "event", Version: 1, SubscriptionID: w.ID, EventID: "debug-" + id.String(), Event: model.ExternalEvent{Provider: w.Provider, Type: w.EventType, Repository: w.RepositoryKey, PRNumber: w.PRNumber, MergeCommitSHA: "0000000", MergedAt: time.Now().UTC().Format(time.RFC3339)}}
	ready, accepted, err := s.AcceptEvent(ctx, envelope)
	if err != nil {
		return failure("invalid_event", err)
	}
	if !accepted {
		return failure("invalid_state", fmt.Errorf("wait %q is %s", w.ID, w.State))
	}
	return response(ready)
}

func (s *Service) handleDoctor(ctx context.Context) protocol.Response {
	checks := map[string]string{"daemon_socket": "available", "daemon_version": version.Current, "relay_connection": s.RelayState()}
	if _, err := s.DB.ListWaits(ctx, nil); err != nil {
		checks["local_database"] = "error: " + err.Error()
	} else {
		checks["local_database"] = "available"
	}
	if s.CodexCheck == nil {
		checks["codex_queue"] = "not configured"
	} else if value, err := s.CodexCheck(ctx); err != nil {
		checks["codex_queue"] = "error: " + err.Error()
	} else {
		checks["codex_queue"] = value
	}
	return response(protocol.DoctorResult{DaemonVersion: version.Current, Relay: s.RelayState(), Checks: checks})
}

func (s *Service) AcceptEvent(ctx context.Context, envelope model.EventEnvelope) (model.Wait, bool, error) {
	w, accepted, err := s.DB.AcceptEvent(ctx, envelope)
	if err == nil && accepted {
		s.wakeDelivery()
	}
	return w, accepted, err
}

func (s *Service) wakeDelivery() {
	select {
	case s.deliveryWake <- struct{}{}:
	default:
	}
}

func (s *Service) RunDeliveryWorker(ctx context.Context) error {
	if err := s.DB.RecoverDelivering(ctx); err != nil {
		return fmt.Errorf("recover deliveries: %w", err)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.deliverAvailable(ctx); err != nil && ctx.Err() == nil {
			// Storage failures are retried by the next tick; a single wait cannot kill the daemon.
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-s.deliveryWake:
		}
	}
}

func (s *Service) deliverAvailable(ctx context.Context) error {
	for {
		w, ok, err := s.DB.ClaimReady(ctx)
		if err != nil || !ok {
			return err
		}
		err = s.Queuer.Queue(ctx, w.CodexThreadID, model.FormatContinuation(w))
		if err == nil {
			if err := s.DB.DeliverySucceeded(ctx, w.ID); err != nil {
				return err
			}
			continue
		}
		delay := retryDelay(w.DeliveryAttempts, s.RetryMaxInterval)
		if dbErr := s.DB.DeliveryFailed(ctx, w.ID, err.Error(), time.Now().Add(delay)); dbErr != nil {
			return dbErr
		}
	}
}

func retryDelay(attempts int, max time.Duration) time.Duration {
	schedule := []time.Duration{5 * time.Second, 15 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute}
	if attempts < 0 {
		attempts = 0
	}
	if attempts >= len(schedule) {
		attempts = len(schedule) - 1
	}
	d := schedule[attempts]
	if max > 0 && d > max {
		return max
	}
	return d
}
