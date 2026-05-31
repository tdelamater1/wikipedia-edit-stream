# aggregation — hot-topics Kafka Streams app

Spring Boot + **Kafka Streams** app (Phase 2). Consumes `wikipedia-events`, computes a
rolling **top-10 trending pages** over a hopping window, and upserts it into the MongoDB
`hot_topics` collection — the doc the Phase 3 dashboard watches via a change stream.

## Topology

```
wikipedia-events (JSON)
  → parse + filter (main namespace)
  → key by title
  → windowedBy(hopping 10min / advance 1min, grace 2min)  → KTable<(title,window), PageAgg>
  → map to (windowEnd, ScoredPage)   hotness = editCount × ln(1 + distinctEditors)
  → groupByKey(windowEnd) → aggregate TopN   ← "top-N across keys": all pages funnel to one
  → foreach → upsert hot_topics {_id:"current", windowStart/End, topPages:[…10…]}
```

State stores are **RocksDB**-backed (the production default). The single-key `windowEnd`
collapse is the classic top-N pattern; the writer only publishes the latest `windowEnd` so
`hot_topics/current` always reflects "now" despite overlapping windows.
See the design note in Obsidian: *kafka-streams-top-n-and-performance*.

## Configuration

Env-overridable (see `src/main/resources/application.properties`). Highlights:

| Env var | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `192.168.4.201:9094` | external listener (local runs) |
| `KAFKA_STREAMS_APP_ID` | `wikipedia-aggregation` | Streams application id (consumer group) |
| `WINDOW_SIZE_MINUTES` / `WINDOW_ADVANCE_MINUTES` / `WINDOW_GRACE_MINUTES` | `10` / `1` / `2` | hopping window |
| `TOPN_SIZE` / `TOPN_MAX_TRACKED` | `10` / `100` | published size / internal tracked set |
| `FILTER_MAIN_NAMESPACE_ONLY` | `true` | count only article-namespace edits |
| `MONGO_*` | localhost / `wikipedia` | same Boot-4 `spring.mongodb.*` scheme as ingest |
| `MONGO_DIRECT_CONNECTION` | `true` | `false` in-cluster |

## Run locally

```bash
kubectl -n mongodb port-forward svc/mongodb-svc 27017:27017          # terminal 1
export MONGO_PASSWORD="$(kubectl -n mongodb get secret wikipedia-db-password -o jsonpath='{.data.password}' | base64 -d)"
./mvnw spring-boot:run                                                # terminal 2
```
(IntelliJ: set `MONGO_PASSWORD` in the run config's environment variables.) The laptop
producer must be running for there to be edits to aggregate.

## Verify

In Compass / mongosh, the `wikipedia` DB should grow a `hot_topics` collection with one doc:

```js
db.hot_topics.findOne({ _id: "current" })   // windowEnd advances; topPages ranked by hotness
```

Give it a minute or two of edits for the window to populate. `topPages[0]` is the current
hottest page.

## Test / build

```bash
./mvnw test       # TopN ranking/pruning unit tests (no infra needed)
./mvnw package    # executable jar
```
