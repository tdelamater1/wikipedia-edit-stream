# producer — Wikipedia EventStreams → Kafka

The **source connector** for the `wikipedia-edit-stream` pipeline. A small Python service that
holds a persistent connection to the Wikimedia
[EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) `recentchange`
SSE feed, keeps **non-bot edits to `en.wikipedia.org`**, reshapes each one, and produces it to
the Kafka topic `wikipedia-events`.

It's the `producer/` module of the `wikipedia-edit-stream` monorepo and runs **in-cluster**
(GitOps manifests in `homelab_argocd/k8s/wikipedia-producer`, single replica). It began as a
standalone repo (`kafka-wikipedia-data-stream`, now archived) based on
[this Medium tutorial](https://towardsdatascience.com/introduction-to-apache-kafka-with-wikipedias-eventstreams-service-d06d4628e8d9).

## Event format — the `wikipedia-events` contract

This is the shared schema the downstream `ingest` consumer (and `aggregation`) depend on, so
changes here are a contract change:

```json
{
  "id": 1426354584,
  "domain": "en.wikipedia.org",
  "namespace": "main namespace",
  "title": "Article_title",
  "comment": "edit summary",
  "timestamp": "2026-05-31T21:55:14Z",
  "user_name": "a_user_name",
  "minor": false,
  "old_length": 6019,
  "new_length": 8687,
  "revision_old": 1426354000,
  "revision_new": 1426354584
}
```

## Reliability / good-citizen behavior

- Persistent SSE connection; **resumes via `Last-Event-ID`** on reconnect.
- Descriptive **`User-Agent`** with a contact email (Wikimedia asks for this).
- **1s cooldown** before reconnecting after a clean stream end (Wikimedia rotates long-lived
  connections routinely); **exponential backoff 1s→60s** on errors, reset after a good send.
- `(connect, read)` timeouts so a silently stalled stream is detected and reconnected —
  important for an unattended in-cluster service.

## Configuration

Env vars (used by the in-cluster ConfigMap), each overridable by a CLI arg:

| Env | CLI | Default |
|-----|-----|---------|
| `BOOTSTRAP_SERVER` | `--bootstrap_server` | `192.168.4.201:9094` (external listener, for local runs) |
| `TOPIC_NAME` | `--topic_name` | `wikipedia-events` |

In-cluster the ConfigMap points `BOOTSTRAP_SERVER` at Kafka's internal listener
(`runnyeye-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092`).

## Run locally

```sh
python3 -m venv kafaka_venv
source kafaka_venv/bin/activate
pip install -r requirements.txt

python wikipedia_events_kafka_producer.py            # uses defaults / env vars
# or override:
python wikipedia_events_kafka_producer.py --bootstrap_server localhost:9092
python wikipedia_events_kafka_producer.py -h         # all options
```

## Build + push the image

```sh
docker build -t ghcr.io/tdelamater1/wikipedia-producer:0.1.0 .
docker push ghcr.io/tdelamater1/wikipedia-producer:0.1.0
```

## Deploy (in-cluster)

Manifests live in `homelab_argocd/k8s/wikipedia-producer/` (ArgoCD app, ConfigMap, single-
replica Deployment). See that README for the bootstrap steps.

> **Single replica only.** Two producers would each read the firehose and double-produce every
> event; the aggregation counts every Kafka message, so a second producer inflates edit counts.
