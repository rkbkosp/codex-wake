# Codex Wake

Codex Wake is a local-first bridge from external terminal events to an existing Codex App task. V1 supports one event: `github.pull_request.merged`.

The public relay never receives a Codex thread ID or continuation note. The local daemon is the wait registry source of truth and is the only component that invokes `codex queue`.

## Maturity

This repository currently contains the P0 implementation and automated host/integration tests. It has **not** yet been activated against a real public relay or accepted with a real GitHub webhook end to end. A successful unit/integration run is not a production-activation claim.

## Components

- `codex-wait`: short-lived CLI used by a Codex turn.
- `codex-waitd`: local daemon with Unix-socket IPC, SQLite persistence, relay reconnect/resync, and retrying Codex delivery.
- `codex-wait-relay`: HTTPS/WSS service with GitHub HMAC verification, terminal-fact persistence, late-subscription replay, and structured event routing.

## Build and test

```bash
make build
make test
```

Binaries are written to `bin/`.

## Local P0-1 flow

Create `~/.codex-wait/config.toml` from [`config.example.toml`](config.example.toml), making sure `codex_binary` is the absolute result of `command -v codex`. For a relay-free local test, leave the relay fields empty.

Start the daemon:

```bash
bin/codex-waitd
```

From a Codex App task whose `CODEX_THREAD_ID`/`CODEX_SESSION_ID` environment is present:

```bash
bin/codex-wait pr-merge \
  https://github.com/example/project/pull/123 \
  --after "PR 已合并。先核对本地仓库状态，再继续下一阶段。" \
  --json
```

Registration exits successfully once the wait is durably committed locally, even when the relay reports `offline`.

To exercise the local delivery path without GitHub:

```bash
bin/codex-wait debug-fire <WAIT_ID> --json
```

`debug-fire` uses the configured Codex binary and therefore really queues a continuation into the registered task. Automated tests use a fake binary instead.

Other commands:

```bash
bin/codex-wait list --json
bin/codex-wait status <WAIT_ID> --json
bin/codex-wait cancel <WAIT_ID> --json
bin/codex-wait doctor --json
```

## Relay

Required environment variables:

```text
WEBHOOK_SECRET=<github webhook secret>
DEVICE_AUTH_KEYS={"mac-main":"a-long-random-device-token"}
DATABASE_PATH=/var/lib/codex-wait/relay.db
LISTEN_ADDR=127.0.0.1:8787
```

Run with built-in TLS:

```bash
bin/codex-wait-relay \
  --tls-cert /absolute/path/fullchain.pem \
  --tls-key /absolute/path/privkey.pem
```

It can also listen on loopback HTTP behind a TLS-terminating reverse proxy. The externally visible endpoints must be HTTPS/WSS in production:

- `POST /v1/webhooks/github`
- `GET /v1/ws`
- `GET /healthz`

An optional `DEBUG_TOKEN` enables `POST /v1/debug/merge`; without it that endpoint returns 404.

For loopback-only development, `codex-waitd` rejects `ws://` unless `CODEX_WAIT_ALLOW_INSECURE_WS=1` is explicitly set.

## Delivery semantics

Delivery is at least once. SQLite transactions and stable `event_id`/`wait_id` values suppress webhook, WebSocket, and replay duplicates during normal operation. A crash after `codex queue` exits zero but before `DELIVERED` is committed can queue the same envelope again. The fixed continuation template therefore tells Codex to verify local state before acting and not repeat completed work.

See [`docs/architecture.md`](docs/architecture.md) for protocol, state, security, and acceptance details.

## License

Licensed under the [MIT License](LICENSE).
