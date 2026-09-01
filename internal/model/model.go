package model

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	ProviderGitHub = "github"
	EventPRMerged  = "github.pull_request.merged"
)

type State string

const (
	StateArmed      State = "ARMED"
	StateReady      State = "READY"
	StateDelivering State = "DELIVERING"
	StateDelivered  State = "DELIVERED"
	StateCancelled  State = "CANCELLED"
)

type Wait struct {
	ID                string `json:"wait_id"`
	Provider          string `json:"provider"`
	EventType         string `json:"event"`
	RepositoryKey     string `json:"repository"`
	RepositoryDisplay string `json:"repository_display"`
	PRNumber          int64  `json:"pr_number"`
	PRURL             string `json:"pr_url"`
	CodexThreadID     string `json:"thread_id"`
	Continuation      string `json:"continuation,omitempty"`
	State             State  `json:"state"`
	ExternalEventID   string `json:"event_id,omitempty"`
	MergeCommitSHA    string `json:"merge_commit_sha,omitempty"`
	MergedAt          string `json:"merged_at,omitempty"`
	CreatedAt         int64  `json:"created_at"`
	EventReceivedAt   int64  `json:"event_received_at,omitempty"`
	DeliveredAt       int64  `json:"delivered_at,omitempty"`
	CancelledAt       int64  `json:"cancelled_at,omitempty"`
	DeliveryAttempts  int    `json:"delivery_attempts"`
	LastDeliveryError string `json:"last_delivery_error,omitempty"`
	NextDeliveryAt    int64  `json:"next_delivery_at,omitempty"`
}

type Subscription struct {
	ID         string `json:"id"`
	Provider   string `json:"provider"`
	Event      string `json:"event"`
	Repository string `json:"repository"`
	PRNumber   int64  `json:"pr_number"`
}

func (w Wait) Subscription() Subscription {
	return Subscription{ID: w.ID, Provider: w.Provider, Event: w.EventType, Repository: w.RepositoryKey, PRNumber: w.PRNumber}
}

type ExternalEvent struct {
	Provider       string `json:"provider"`
	Type           string `json:"type"`
	Repository     string `json:"repository"`
	PRNumber       int64  `json:"pr_number"`
	MergeCommitSHA string `json:"merge_commit_sha,omitempty"`
	MergedAt       string `json:"merged_at,omitempty"`
}

type EventEnvelope struct {
	Type           string        `json:"type"`
	Version        int           `json:"version"`
	SubscriptionID string        `json:"subscription_id"`
	EventID        string        `json:"event_id"`
	Event          ExternalEvent `json:"event"`
}

var githubPart = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
var externalEventID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
var commitSHA = regexp.MustCompile(`^[A-Fa-f0-9]{7,64}$`)

func (e ExternalEvent) Validate() error {
	if e.Provider != ProviderGitHub || e.Type != EventPRMerged {
		return errors.New("unsupported external event")
	}
	if e.PRNumber <= 0 || strings.ToLower(e.Repository) != e.Repository {
		return errors.New("invalid external event target")
	}
	ref, err := ParseGitHubPRURL(ConstructPRURL(e.Repository, e.PRNumber))
	if err != nil || ref.RepositoryKey != e.Repository {
		return errors.New("invalid external event repository")
	}
	if e.MergeCommitSHA != "" && !commitSHA.MatchString(e.MergeCommitSHA) {
		return errors.New("invalid merge commit SHA")
	}
	if e.MergedAt == "" {
		return errors.New("merged_at is required")
	}
	if _, err := time.Parse(time.RFC3339, e.MergedAt); err != nil {
		return errors.New("invalid merged_at")
	}
	return nil
}

func (e EventEnvelope) ValidateAgainst(w Wait) error {
	if e.Type != "event" || e.Version != 1 {
		return errors.New("unsupported event envelope")
	}
	if e.SubscriptionID != w.ID || !externalEventID.MatchString(e.EventID) {
		return errors.New("event identity does not match wait")
	}
	if err := e.Event.Validate(); err != nil {
		return err
	}
	if w.State != StateArmed {
		return fmt.Errorf("wait is %s, not ARMED", w.State)
	}
	if e.Event.Provider != w.Provider || e.Event.Type != w.EventType || strings.ToLower(e.Event.Repository) != w.RepositoryKey || e.Event.PRNumber != w.PRNumber {
		return errors.New("event fields do not match wait")
	}
	return nil
}

type PRRef struct {
	RepositoryKey     string
	RepositoryDisplay string
	Number            int64
	URL               string
}

func ParseGitHubPRURL(raw string) (PRRef, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return PRRef{}, fmt.Errorf("parse PR URL: %w", err)
	}
	if u.Scheme != "https" || !strings.EqualFold(u.Host, "github.com") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return PRRef{}, errors.New("PR URL must be canonical https://github.com/<owner>/<repo>/pull/<number>")
	}
	parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	if len(parts) != 4 || parts[2] != "pull" {
		return PRRef{}, errors.New("PR URL must be canonical https://github.com/<owner>/<repo>/pull/<number>")
	}
	owner, err := url.PathUnescape(parts[0])
	if err != nil {
		return PRRef{}, errors.New("invalid repository owner")
	}
	repo, err := url.PathUnescape(parts[1])
	if err != nil {
		return PRRef{}, errors.New("invalid repository name")
	}
	if !githubPart.MatchString(owner) || !githubPart.MatchString(repo) {
		return PRRef{}, errors.New("invalid GitHub repository")
	}
	n, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != parts[3] {
		return PRRef{}, errors.New("invalid pull request number")
	}
	display := owner + "/" + repo
	canonicalURL := "https://github.com/" + display + "/pull/" + strconv.FormatInt(n, 10)
	if u.Path != "/"+owner+"/"+repo+"/pull/"+strconv.FormatInt(n, 10) {
		return PRRef{}, errors.New("PR URL must not contain a trailing slash or escaped path parts")
	}
	return PRRef{RepositoryKey: strings.ToLower(display), RepositoryDisplay: display, Number: n, URL: canonicalURL}, nil
}

func ConstructPRURL(repository string, number int64) string {
	return "https://github.com/" + strings.ToLower(repository) + "/pull/" + strconv.FormatInt(number, 10)
}

func FormatContinuation(w Wait) string {
	continuation := strings.TrimSpace(w.Continuation)
	if continuation == "" {
		continuation = "No additional continuation note was registered."
	}
	return fmt.Sprintf(`[external-event:v1]

wait_id: %s
event_id: %s
type: %s
repository: %s
pr_number: %d
pr_url: %s
merge_commit_sha: %s
merged_at: %s

The pull request this thread explicitly registered as an external wait has been merged.

Verify the current repository and task state locally before making changes. Do not repeat work that has already been completed.

Continuation note registered by the previous turn:
%s`, w.ID, w.ExternalEventID, w.EventType, w.RepositoryKey, w.PRNumber, ConstructPRURL(w.RepositoryKey, w.PRNumber), w.MergeCommitSHA, w.MergedAt, continuation)
}

func UnixNow() int64 { return time.Now().UTC().Unix() }
