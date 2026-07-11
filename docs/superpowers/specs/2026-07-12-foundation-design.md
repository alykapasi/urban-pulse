# UrbanPulse Foundation Design

## Purpose

Create the smallest platform foundation that lets UrbanPulse add both
historical and real-time NYC mobility sources without redesigning ingestion
for every source. This milestone establishes boundaries only; it does not
ingest a production dataset or provision paid infrastructure.

## Decisions carried forward

- UrbanPulse is a NYC mobility data-engineering project.
- The platform uses two lanes: batch and live.
- Platform mechanics are shared; each source owns its access and data
  semantics.
- The live lane is always on in production and uses Kafka as transport and a
  short replay buffer, not as the durable data store.
- Delivery is at-least-once. Consumers must be idempotent through stable
  capture identifiers.
- The production baseline is one Hetzner VPS for the runtime and Backblaze B2
  for durable raw archives and database backups. The target operating budget
  is USD 20 per month, with USD 50 as a hard cap.

## Source catalogue

Each planned source is classified by ingress pattern before an adapter is
implemented. A source may appear in more than one row when it exposes both
historical and real-time data.

| Source family | Candidate sources | Lane | Ingress pattern | Initial purpose |
| --- | --- | --- | --- | --- |
| Published artifacts | TLC trip records; Citi Bike trip history; static MTA and NYC Ferry GTFS; taxi zones; PLUTO | Batch | Versioned files downloaded on a schedule | Historical facts and reference data |
| Incremental tabular APIs | NYC Open Data: 311, collisions, events, traffic speeds, counts, ferry ridership | Batch | Deterministic pagination plus source checkpoint | Context, disruptions, and operational measures |
| Weather observations | NOAA station history and weather APIs | Batch | File/API snapshots with a time checkpoint | Environmental context |
| Public real-time feeds | Citi Bike GBFS; MTA subway and ferry GTFS-RT | Live | Poll a changing snapshot and emit observed events | Availability, vehicle/trip, and alert signals |
| Keyed real-time feeds | MTA BusTime GTFS-RT/SIRI | Live | Authenticated polling with source-owned credentials | Bus location and service signals |

The catalogue is intentionally organized by ingress behavior instead of by
vendor. New sources must be added by selecting one of these patterns or by
introducing a clearly documented new pattern.

## Responsibility boundaries

### Shared platform responsibilities

- Start and supervise the batch and live processes.
- Provide Kafka transport for live captures.
- Store durable raw payloads in object storage.
- Persist source-run and capture metadata.
- Provide common retry, logging, metrics, and health-check primitives.
- Preserve capture lineage from source request through archive location.

### Source-owned responsibilities

Each source adapter owns discovery, authentication, rate limiting, pagination
or polling, checkpoints, request construction, decoding, schema-drift
handling, deduplication rules, data-quality checks, and interpretation of
event time and geospatial fields.

This keeps a Socrata offset/checkpoint, a monthly file release, and a GTFS-RT
poll from being forced through a false shared abstraction.

## Data flow and capture contract

Batch sources write downloaded raw artifacts directly to object storage and
record their run and capture metadata. Live sources poll their upstream feed,
publish a raw capture event to Kafka, and an archive consumer writes the raw
payload to object storage.

Every capture must include:

- `source_id`: stable identifier for the adapter and dataset.
- `capture_id`: deterministic identifier used for idempotency.
- `fetched_at`: when UrbanPulse received the payload, in UTC.
- `observed_at`: when the payload was observed upstream, when the source
  supplies that meaning.
- `published_at`: source publication/event time, when distinct and available.
- `payload_uri` and checksum: immutable raw archive location and integrity
  marker.
- parser/schema version and source checkpoint, when applicable.

`fetched_at`, `observed_at`, and `published_at` must remain distinct. Missing
source times are represented as absent rather than inferred.

## Runtime topology

### Local development

Docker Compose runs a single Kafka broker in KRaft mode, PostgreSQL, and an
S3-compatible local object store. Python workers run through `uv` from the
repository so local code changes do not require rebuilding a worker image.

The first runtime milestone proves only that services start cleanly, retain
their state across a restart, expose health checks, and can be reached from a
small Python smoke test. It includes no source-specific poller or downloader.

### Production

One Hetzner VPS runs the same compact services with Docker Compose. Backblaze
B2 replaces the local object store for raw archives and encrypted PostgreSQL
backups. The initial deployment is intentionally a modular monolith: separate
source packages and batch/live processes, not a microservice per source.

## Explicit non-goals for this milestone

- Provisioning Hetzner, B2, or secrets.
- Ingesting a real public dataset.
- Curated tables, lakehouse formats, transformations, or dashboards.
- A workflow orchestrator, Kubernetes, managed Kafka, or a metrics stack.
- Multi-city support.

## Delivery order

1. Record this foundation and catalogue decision.
2. Build and test the local runtime skeleton.
3. Add a static TLC artifact vertical slice using the batch boundary.
4. Add one real-time feed using the live boundary and archive consumer.
5. Only then provision the always-on Hetzner/B2 deployment.

## Acceptance criteria for the next implementation plan

- A new developer can start the local services with one documented command.
- Kafka, PostgreSQL, and local object storage each have a health check.
- Restarting the Compose stack preserves service state.
- A Python smoke test can connect to each service using development settings.
- No paid cloud resource, source credential, or real external ingest is
  required.
