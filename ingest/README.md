# ingest — Wikipedia edit ETL consumer

Spring Boot app that consumes raw edit events from the `wikipedia-events` Kafka topic,
enriches each one, and upserts it into the MongoDB `edits` collection.

## What it does

For every message it:

- parses the producer's snake_case JSON into `WikipediaEdit`;
- enriches it (`EditEnricher`): reconstructs the article `pageUrl`
  (`https://<domain>/wiki/<Title_with_underscores>`), computes `byteDelta`
  (`new_length - old_length`, null-safe), parses the ISO event `timestamp`, and stamps
  `ingestedAt`;
- upserts it into `edits` keyed by the source event id (`_id`), so replays are idempotent.

On startup `MongoIndexInitializer` ensures two indexes on `edits`:
`{title:1, timestamp:-1}` and a **TTL** on `{ingestedAt:1}` (48h default) so raw data
never grows unbounded.

## Configuration

All settings are env-overridable (see `src/main/resources/application.properties`):

| Env var                  | Default                                                          | Purpose                       |
|--------------------------|------------------------------------------------------------------|-------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`| `192.168.4.201:9094`                                             | external Strimzi listener     |
| `KAFKA_TOPIC`            | `wikipedia-events`                                               | source topic                  |
| `KAFKA_GROUP_ID`         | `wikipedia-ingest`                                               | consumer group                |
| `KAFKA_AUTO_OFFSET_RESET`| `latest`                                                         | `earliest` to replay backlog  |
| `MONGO_HOST`             | `localhost`                                                      | Mongo host (port-forward)     |
| `MONGO_PORT`             | `27017`                                                          | Mongo port                    |
| `MONGO_DB`               | `wikipedia`                                                      | database                      |
| `MONGO_USERNAME`         | `wikipedia`                                                      | app user                      |
| `MONGO_PASSWORD`         | _(empty)_                                                        | **raw** password (Spring escapes it) |
| `MONGO_AUTH_DB`          | `admin`                                                          | auth source (user lives in admin) |
| `MONGO_DIRECT_CONNECTION`| `true`                                                          | direct mode; set `false` in-cluster |
| `EDITS_TTL_SECONDS`      | `172800`                                                         | TTL on `edits` (48h)          |

## Run locally

The defaults target the *external* Kafka listener, so this runs from a laptop. MongoDB is
internal-only — port-forward it first:

```bash
# 1. Port-forward the in-cluster Mongo (direct mode bypasses RS topology discovery)
kubectl -n mongodb port-forward svc/mongodb-svc 27017:27017

# 2. Run the consumer — only the RAW password is needed (Spring escapes it)
export MONGO_PASSWORD="$(kubectl -n mongodb get secret wikipedia-db-password -o jsonpath='{.data.password}' | base64 -d)"
./mvnw spring-boot:run
```

**In IntelliJ:** Run → Edit Configurations → your `IngestApplication` → **Environment
variables**, add `MONGO_PASSWORD` set to the raw password (output of the
`kubectl ... base64 -d` command above). No URI-encoding required.

## Verify

With the producer running on your laptop and the consumer up, edits should land within
seconds. Check in Compass (connected to `localhost:27017` with `directConnection=true`) or:

```bash
kubectl -n mongodb exec -it mongodb-0 -c mongod -- mongosh --quiet \
  -u wikipedia -p "<password>" --authenticationDatabase admin wikipedia \
  --eval 'db.edits.countDocuments(); db.edits.findOne();'
```

Confirm `pageUrl`, `byteDelta`, `timestamp`, and `ingestedAt` are populated, and that
`db.edits.getIndexes()` shows the `title_timestamp` and TTL `ingestedAt_ttl` indexes.

## Test / build

```bash
./mvnw test        # fast unit tests (EditEnricher), no Kafka/Mongo needed
./mvnw package     # build the executable jar
```

> Notes: Spring Boot 4 ships **Jackson 3** — `ObjectMapper` lives under `tools.jackson.*`
> (Jackson annotations stay `com.fasterxml.jackson.annotation`). The Maven wrapper
> (`./mvnw`) means no system Maven install is required.
