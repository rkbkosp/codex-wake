package relay

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rkbkosp/codex-wake/internal/model"
	"github.com/rkbkosp/codex-wake/internal/storage"
)

type Server struct {
	DB            *storage.DB
	Hub           *Hub
	WebhookSecret []byte
	DeviceTokens  map[string]string
	DebugToken    string
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/ws", WSServer{Hub: s.Hub, Auth: Authenticator{Tokens: s.DeviceTokens}})
	mux.HandleFunc("/v1/webhooks/github", s.githubWebhook)
	mux.HandleFunc("/v1/debug/merge", s.debugMerge)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
	return securityHeaders(mux)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) githubWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if !verifyGitHubSignature(s.WebhookSecret, body, r.Header.Get("X-Hub-Signature-256")) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	if r.Header.Get("X-GitHub-Event") != "pull_request" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	deliveryID := strings.TrimSpace(r.Header.Get("X-GitHub-Delivery"))
	deliveryUUID, err := uuid.Parse(deliveryID)
	if err != nil {
		http.Error(w, "invalid X-GitHub-Delivery", http.StatusBadRequest)
		return
	}
	deliveryID = deliveryUUID.String()
	var payload struct {
		Action     string `json:"action"`
		Number     int64  `json:"number"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
		PullRequest struct {
			Merged         bool   `json:"merged"`
			MergeCommitSHA string `json:"merge_commit_sha"`
			MergedAt       string `json:"merged_at"`
		} `json:"pull_request"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if payload.Action != "closed" || !payload.PullRequest.Merged {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	repository := strings.ToLower(payload.Repository.FullName)
	if err := validateMergeFields(repository, payload.Number, payload.PullRequest.MergeCommitSHA, payload.PullRequest.MergedAt); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fact, inserted, err := s.DB.PutMergeFact(r.Context(), storage.MergeFact{RepositoryKey: repository, PRNumber: payload.Number, DeliveryID: deliveryID, MergeCommitSHA: payload.PullRequest.MergeCommitSHA, MergedAt: payload.PullRequest.MergedAt})
	if err != nil {
		http.Error(w, "persist event", http.StatusInternalServerError)
		return
	}
	if inserted {
		s.Hub.Dispatch(fact)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "duplicate": !inserted})
}

func verifyGitHubSignature(secret, body []byte, signature string) bool {
	if len(secret) == 0 || !strings.HasPrefix(signature, "sha256=") {
		return false
	}
	presented, err := hex.DecodeString(strings.TrimPrefix(signature, "sha256="))
	if err != nil || len(presented) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	expected := mac.Sum(nil)
	return subtle.ConstantTimeCompare(expected, presented) == 1
}

func validateMergeFields(repository string, number int64, sha, mergedAt string) error {
	if number <= 0 {
		return errors.New("invalid PR number")
	}
	ref, err := model.ParseGitHubPRURL(model.ConstructPRURL(repository, number))
	if err != nil || ref.RepositoryKey != repository {
		return errors.New("invalid repository")
	}
	if sha != "" {
		if len(sha) < 7 || len(sha) > 64 {
			return errors.New("invalid merge commit SHA")
		}
		for _, r := range sha {
			if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
				return errors.New("invalid merge commit SHA")
			}
		}
	}
	if mergedAt == "" {
		return errors.New("merged_at is required")
	}
	if _, err := time.Parse(time.RFC3339, mergedAt); err != nil {
		return errors.New("invalid merged_at")
	}
	return nil
}

func (s *Server) debugMerge(w http.ResponseWriter, r *http.Request) {
	if s.DebugToken == "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	presented := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if subtle.ConstantTimeCompare([]byte(presented), []byte(s.DebugToken)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var input struct {
		Repository     string `json:"repository"`
		PRNumber       int64  `json:"pr_number"`
		MergeCommitSHA string `json:"merge_commit_sha"`
		MergedAt       string `json:"merged_at"`
		EventID        string `json:"event_id"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&input); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(w, "trailing JSON values are not allowed", http.StatusBadRequest)
		return
	}
	input.Repository = strings.ToLower(input.Repository)
	if input.MergedAt == "" {
		input.MergedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if input.MergeCommitSHA == "" {
		input.MergeCommitSHA = "0000000"
	}
	if input.EventID == "" {
		input.EventID = "debug-" + uuid.NewString()
	} else if strings.HasPrefix(input.EventID, "debug-") {
		parsed, err := uuid.Parse(strings.TrimPrefix(input.EventID, "debug-"))
		if err != nil {
			http.Error(w, "invalid event_id", http.StatusBadRequest)
			return
		}
		input.EventID = "debug-" + parsed.String()
	} else {
		parsed, err := uuid.Parse(input.EventID)
		if err != nil {
			http.Error(w, "invalid event_id", http.StatusBadRequest)
			return
		}
		input.EventID = parsed.String()
	}
	if err := validateMergeFields(input.Repository, input.PRNumber, input.MergeCommitSHA, input.MergedAt); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fact, inserted, err := s.DB.PutMergeFact(r.Context(), storage.MergeFact{RepositoryKey: input.Repository, PRNumber: input.PRNumber, DeliveryID: input.EventID, MergeCommitSHA: input.MergeCommitSHA, MergedAt: input.MergedAt})
	if err != nil {
		http.Error(w, "persist event", http.StatusInternalServerError)
		return
	}
	if inserted {
		s.Hub.Dispatch(fact)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"event_id": fact.DeliveryID, "repository": fact.RepositoryKey, "pr_number": strconv.FormatInt(fact.PRNumber, 10), "state": map[bool]string{true: "inserted", false: "existing"}[inserted]})
}
