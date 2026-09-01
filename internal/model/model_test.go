package model

import (
	"strings"
	"testing"
)

func TestParseGitHubPRURL(t *testing.T) {
	t.Parallel()
	got, err := ParseGitHubPRURL("https://github.com/Example/Project/pull/123")
	if err != nil {
		t.Fatal(err)
	}
	if got.RepositoryKey != "example/project" || got.RepositoryDisplay != "Example/Project" || got.Number != 123 {
		t.Fatalf("unexpected ref: %+v", got)
	}
	bad := []string{
		"http://github.com/a/b/pull/1",
		"https://example.com/a/b/pull/1",
		"https://github.com/a/b/pull/01",
		"https://github.com/a/b/pull/1/",
		"https://github.com/a/b/pull/1?q=x",
		"https://github.com/a/b/issues/1",
	}
	for _, raw := range bad {
		if _, err := ParseGitHubPRURL(raw); err == nil {
			t.Errorf("expected rejection for %q", raw)
		}
	}
}

func TestContinuationExcludesUntrustedGitHubText(t *testing.T) {
	w := Wait{ID: "wait_x", EventType: EventPRMerged, RepositoryKey: "a/b", PRNumber: 1, ExternalEventID: "evt", Continuation: "continue"}
	message := FormatContinuation(w)
	if strings.Contains(message, "title") || strings.Contains(message, "body") {
		t.Fatalf("unexpected untrusted fields: %s", message)
	}
}
