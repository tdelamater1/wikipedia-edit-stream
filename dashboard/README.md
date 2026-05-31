# dashboard

Phase 3 of wikipedia-edit-stream: the live web dashboard. A small Go service that **watches
the MongoDB `hot_topics` change stream** and **pushes** every update to connected browsers
over **WebSocket**, which render the live top-10.

```
Mongo hot_topics/_id=current ──change stream──▶ [Go: watcher → hub] ──WebSocket──▶ browser
```

This is the first Go component in the project (the ingest + aggregation apps are Java).

## How it's put together

| File          | Role |
|---------------|------|
| `main.go`     | Wiring: config, Mongo connect, start hub + watcher, HTTP server, graceful shutdown. Embeds `web/index.html`. |
| `config.go`   | Env → config; assembles the Mongo URI (password URL-escaped, like the Java apps' discrete props). |
| `watcher.go`  | Opens the change stream, seeds current state, converts each BSON doc to JSON, broadcasts. Reconnects on error. |
| `hub.go`      | The broadcaster — one goroutine owns the client set (no mutex) and fans out messages; caches the latest so new clients render instantly. |
| `client.go`   | Per-connection read/write pumps + WebSocket ping/pong keepalive (the canonical gorilla pattern). |
| `web/index.html` | Vanilla-JS frontend: connects, renders the table, **reconnects with backoff** (WebSocket has no auto-reconnect). |

> Uses `go.mongodb.org/mongo-driver` **v1** (most docs/examples). A v2 line exists with a
> builder-style options API if you want to explore it later.

## Run locally

1. Port-forward Mongo (separate terminal):
   ```bash
   kubectl -n mongodb port-forward svc/mongodb-svc 27017:27017
   ```
2. Build deps + run (defaults target localhost + `directConnection=true`):
   ```bash
   go mod tidy          # first time: resolves deps + writes go.sum
   MONGO_PASSWORD='<the wikipedia user password>' go run .
   ```
3. Open <http://localhost:8080>. With the aggregator running you'll see the top-10 refresh
   every ~5s; the status pill reads **live**.

Env vars (all optional except the password): `HTTP_ADDR` (`:8080`), `MONGO_HOST`,
`MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_AUTH_DB`, `MONGO_DB`,
`MONGO_DIRECT_CONNECTION` (`true` locally, `false` in-cluster), `HOT_TOPICS_COLLECTION`.

## Build the image

```bash
docker build -t ghcr.io/tdelamater1/wikipedia-dashboard:0.1.0 .
docker push ghcr.io/tdelamater1/wikipedia-dashboard:0.1.0
```

Deploy manifests live in the `homelab_argocd` repo under `k8s/wikipedia-dashboard/` (added
when we wire up the in-cluster deploy + Traefik exposure).
