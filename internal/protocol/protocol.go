package protocol

import (
	"encoding/json"

	"github.com/rkbkosp/codex-wake/internal/model"
)

const Version = 1

type Request struct {
	Version int             `json:"version"`
	ID      string          `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	Version int             `json:"version"`
	ID      string          `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type RegisterParams struct {
	Event             string `json:"event"`
	RepositoryKey     string `json:"repository"`
	RepositoryDisplay string `json:"repository_display"`
	PRNumber          int64  `json:"pr_number"`
	PRURL             string `json:"pr_url"`
	ThreadID          string `json:"thread_id"`
	Continuation      string `json:"continuation,omitempty"`
}

type IDParams struct {
	ID string `json:"wait_id"`
}

type RegisterResult struct {
	Wait  model.Wait `json:"wait"`
	Relay string     `json:"relay"`
}

type DoctorResult struct {
	DaemonVersion string            `json:"daemon_version"`
	Relay         string            `json:"relay"`
	Checks        map[string]string `json:"checks"`
}

type WSMessage struct {
	Type          string               `json:"type"`
	Version       int                  `json:"version"`
	Subscriptions []model.Subscription `json:"subscriptions,omitempty"`
	Subscription  *model.Subscription  `json:"subscription,omitempty"`
	ID            string               `json:"id,omitempty"`
}
