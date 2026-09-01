# Codex External Wait V1 architecture

## Frozen boundary

```text
registration: Codex task -> codex-wait -> local durable wait
event:        GitHub -> HTTPS webhook -> relay -> WSS -> local daemon
continuation: local daemon -> codex queue -> original Codex App task
```

Only the local database contains `codex_thread_id` and `continuation`. Relay messages contain only versioned structured event fields. PR title, body, comments, review text, and commit messages are neither persisted for delivery nor copied into the Codex message.

## Local durability and states

The daemon owns `~/.codex-wait/state.db`. Registration success means the `ARMED` row committed locally; it does not depend on relay connectivity.

```text
ARMED -> READY -> DELIVERING -> DELIVERED
  |
  +-> CANCELLED
```

- `ARMED`: subscribed locally and eligible for relay synchronization.
- `READY`: a validated event is durable locally.
- `DELIVERING`: atomically claimed by the delivery worker.
- `DELIVERED`: `codex queue` exited zero. This does not mean the continuation work finished.
- `CANCELLED`: cancelled before the terminal event was accepted.

On startup, an interrupted `DELIVERING` row returns to `READY`. Queue failures return it to `READY` with the next retry time. Retry intervals are 5s, 15s, 30s, 1m, then 5m, capped by local configuration.

## Local IPC

The Unix socket defaults to `~/.codex-wait/run/waitd.sock`, mode `0600`, inside a `0700` directory. Paths longer than 96 bytes are rejected early because Darwin Unix-socket paths are limited. `CODEX_WAIT_SOCKET` or `--socket` can select a shorter path.

Each connection carries one newline-delimited JSON request and response with `version: 1`. Methods are:

- `wait.register`
- `wait.list`
- `wait.status`
- `wait.cancel`
- `wait.debug_fire`
- `daemon.doctor`

The daemon re-parses the canonical PR URL, verifies all derived fields, requires a canonical UUID thread ID, and bounds the locally supplied continuation note.

## Relay protocol

The daemon initiates the WebSocket and authenticates with a bearer device token. Production configuration requires `wss://`. After every connection it sends a complete `subscriptions.sync` containing only `id`, `provider`, `event`, `repository`, and `pr_number`. Incremental add/remove messages are an optimization; the local `ARMED` set remains authoritative.

The relay keeps live subscriptions in memory. Terminal merge facts are stored in SQLite by `(repository_key, pr_number)`, with a unique GitHub delivery ID. When a subscription is added or synchronized, the relay checks this store and immediately replays an already-satisfied fact.

Event envelopes are allowlisted:

```json
{
  "type": "event",
  "version": 1,
  "subscription_id": "wait_<uuid>",
  "event_id": "<github-delivery-guid>",
  "event": {
    "provider": "github",
    "type": "github.pull_request.merged",
    "repository": "owner/repo",
    "pr_number": 123,
    "merge_commit_sha": "abcdef1",
    "merged_at": "2026-08-31T08:00:00Z"
  }
}
```

The daemon independently validates every identity, target, SHA, and timestamp before the transactional `ARMED -> READY` transition.

## GitHub webhook

`POST /v1/webhooks/github` follows this order:

1. Read bounded raw request bytes.
2. Verify `X-Hub-Signature-256` HMAC-SHA256 with constant-time comparison.
3. Parse JSON.
4. Accept only `X-GitHub-Event: pull_request`, `action: closed`, and `pull_request.merged: true`.
5. Normalize allowlisted fields and durably insert the terminal fact.
6. Dispatch to online matching subscriptions and return 2xx.

Invalid signatures never reach JSON processing or persistence. Repeated deliveries and a second delivery for an already-known PR do not produce a new logical fact.

## Codex injection

The absolute configured `codex_binary` is executed directly with argv:

```text
codex queue --thread <local-thread-id> --message <fixed-local-template>
```

No shell parses the thread ID, structured event, or message. The template includes stable `wait_id` and `event_id` values plus the local continuation note.

Strict exactly-once delivery is impossible without an idempotency primitive in `codex queue`. The crash window between queue acceptance and the `DELIVERED` commit gives V1 at-least-once delivery with application-level idempotency.

## Acceptance coverage

Automated tests cover:

- local register -> durable DB -> debug event -> fixed message -> delivered;
- duplicate event suppression and one merge satisfying multiple local waits;
- terminal-fact deduplication and late-subscription replay;
- invalid signature rejection and non-merge close filtering;
- environment mismatch/missing behavior and canonical URL parsing;
- direct argv preservation for shell metacharacters/newlines;
- prompt-injection text in non-allowlisted webhook fields not entering event delivery.

Still requiring deployment evidence:

- a real GitHub webhook delivered through the production HTTPS edge;
- daemon disconnect during a real merge followed by WSS reconnect/replay;
- temporary real Codex App unavailability followed by successful retry;
- a real PR merge causing the original Codex App task to continue.
