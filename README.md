# wikipedia-edit-stream

Downstream stream-processing pipeline for live Wikipedia edits. The
[`kafka-wikipedia-data-stream`](https://github.com/tdelamater1/kafka-wikipedia-data-stream)
producer (Python, run from a laptop) streams English-Wikipedia edits into the Kafka topic
`wikipedia-events` on the homelab k3s cluster. This repo consumes that topic, enriches and
aggregates it into MongoDB, and serves a live dashboard.


## Components (monorepo)

| Dir            | Lang             | Role                                                                 | Status |
|----------------|------------------|---------------------------------------------------------------------|--------|
| `ingest/`      | Java (Spring Boot) | Consume `wikipedia-events` → enrich → MongoDB `edits`              | **Phase 1 (current)** |
| _aggregation/_ | Java (Kafka Streams) | Windowed top-N + spike detection → `page_stats`/`hot_topics`/`alerts` | Phase 2 |
| _dashboard/_   | Go               | Mongo change stream → WebSocket → live top-10 browser UI            | Phase 3 |

## Architecture (two streaming layers)

Change streams do **not** replace Kafka Streams — they solve a different problem:

```
Wikimedia SSE → [Python producer, laptop] → Kafka (wikipedia-events, in-cluster)
   ├─ [ingest/]            Java  → Mongo: edits
   └─ [aggregation/]       Java  → Mongo: page_stats, hot_topics, alerts
                                      │
              Mongo change stream ◀───┘
                  └─ [dashboard/]  Go: change stream → WebSocket → browser
```

- **Kafka Streams** sits *between* Kafka and Mongo (ETL + windowed aggregation).
- **Change streams** sit *after* Mongo (push DB changes to the dashboard — no polling).

## Infra

MongoDB runs in-cluster as a single-node replica set (`replSet=mongodb`) via the MongoDB
Community Operator, managed by ArgoCD in the `homelab_argocd` repo (`k8s/mongodb/`). The
replica set is required for change streams. See that repo's `k8s/mongodb/README.md`.
