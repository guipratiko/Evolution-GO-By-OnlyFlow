# Evolution GO — fix PostgreSQL connection leak (#112)

## Problem

`StartClient` called `sqlstore.New(...)` on **every** connect / reconnect / QR attempt.
Each call opens a new `*sql.DB` pool. When reconnect or QR fails (or the client is
replaced), that pool was never closed → idle connections on `onlydbevoflow_auth`
grew until `max_connections` (issue
https://github.com/evolution-foundation/evolution-go/issues/112).

Observed in production with 8 instances: `auth` climbed from ~25 to 80+ and kept rising.

## Fix

Reuse a **single shared** `sqlstore.Container` built with `sqlstore.NewWithDB` on the
existing pooled `authDB` (Postgres) or `sqliteDB` (SQLite). Upgrade runs once via
`sync.Once`. Reconnects no longer open new pools.

Also: if a disconnected client is still in `clientPointer`, disconnect and remove it
before creating a new one (avoids orphaned sockets / handlers).

## Files

- `pkg/whatsmeow/service/whatsmeow.go`

## Deploy

1. Point EasyPanel at commit containing this fix (`fix: reuse shared sqlstore...`), not only `sync: 0.7.2`.
2. Prefer **Dockerfile** build (Alpine + `libwebp-dev` + `CGO_ENABLED=1`).
3. If EasyPanel uses **Nixpacks**, `nixpacks.toml` installs `libwebp-dev` / build-essential so `github.com/chai2010/webp` links correctly.

Without libwebp, the build fails with `undefined: webpDecodeRGB` / `webpEncodeRGB`.

After deploy, restart/redeploy Evolution GO. Postgres `auth` connections should stay near the shared pool size and stop climbing on QR/reconnect.
