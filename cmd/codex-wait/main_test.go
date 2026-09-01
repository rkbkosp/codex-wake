package main

import "testing"

func TestCurrentThreadID(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "a")
	t.Setenv("CODEX_SESSION_ID", "a")
	if got, err := currentThreadID(); err != nil || got != "a" {
		t.Fatalf("%q %v", got, err)
	}
	t.Setenv("CODEX_SESSION_ID", "b")
	if _, err := currentThreadID(); err == nil {
		t.Fatal("expected mismatch error")
	}
	t.Setenv("CODEX_THREAD_ID", "")
	if got, err := currentThreadID(); err != nil || got != "b" {
		t.Fatalf("fallback: %q %v", got, err)
	}
	t.Setenv("CODEX_SESSION_ID", "")
	if _, err := currentThreadID(); err == nil {
		t.Fatal("expected missing env error")
	}
}

func TestParseArgsAllowsFlagsAfterURL(t *testing.T) {
	got, err := parseArgs([]string{"https://github.com/a/b/pull/1", "--after", "continue", "--json"})
	if err != nil || len(got.positionals) != 1 || got.after != "continue" || !got.json {
		t.Fatalf("%+v %v", got, err)
	}
}
