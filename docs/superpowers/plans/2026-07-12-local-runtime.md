# Local Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide a reproducible local Kafka, PostgreSQL, and S3-compatible
object-store runtime that UrbanPulse Python code can health-check without
contacting a public data source.

**Architecture:** Docker Compose owns the three stateful local services and
their named volumes. Python runs from the host through `uv`; a small,
configuration-driven smoke command checks Kafka metadata, PostgreSQL `SELECT
1`, and the raw object-store bucket. This deliberately establishes platform
connectivity only, not a source adapter or data pipeline.

**Tech Stack:** Python 3.12, uv, pytest, Ruff, Pydantic Settings, Apache Kafka
4.0.2 (single-node KRaft), PostgreSQL 17.10, SeaweedFS 4.36 S3 API, Docker
Compose.

## Global Constraints

- Use Python 3.12+ and manage every Python dependency through `uv`.
- Keep the runtime local-only: no Hetzner/B2 provisioning, API key, real source
  fetch, curated table, or orchestration service.
- Use Apache Kafka in single-node KRaft mode. Kafka is a local transport and
  short replay buffer, not the durable archive.
- Persist the three service data directories in named Compose volumes.
- Keep development credentials explicitly local and non-secret; never reuse
  them in a deployed environment.
- Bind host ports `19092`, `55433`, and `18333` to avoid the common Kafka,
  PostgreSQL, and S3 port collisions on the developer machine.
- Bind those host ports to `127.0.0.1` only. A later production Compose file
  must use private networking and explicit authentication instead of copying
  this plaintext local configuration.
- Run Docker-dependent tests only when `URBANPULSE_RUN_INTEGRATION=1` is set.
- Keep the current non-goals intact: no actual ingestion, transformations,
  lakehouse format, dashboard, IaC, or production deployment.

---

## File structure

| File | Responsibility |
| --- | --- |
| `compose.yaml` | Local stateful services, persistent volumes, host ports, and health checks. |
| `.env.example` | Documented local runtime values read by the Python settings object. |
| `pyproject.toml` | Runtime client dependencies, CLI entry point, and pytest marker. |
| `uv.lock` | Resolved dependency graph created by `uv lock`. |
| `src/urbanpulse/runtime/__init__.py` | Runtime package boundary. |
| `src/urbanpulse/runtime/settings.py` | Typed local/overrideable connection settings. |
| `src/urbanpulse/runtime/smoke.py` | Probe functions and the `urbanpulse-runtime-smoke` command. |
| `tests/runtime/test_settings.py` | Unit tests for defaults and environment overrides. |
| `tests/runtime/test_smoke.py` | Unit tests for probe ordering, success, and failure behavior. |
| `tests/integration/test_runtime_smoke.py` | Opt-in test against a running Compose stack. |
| `README.md` | Local runtime start, check, test, and shutdown instructions. |
| `docs/decision-log.md` | ADR entries for the architecture, runtime, and deployment choices. |

## Task 1: Add typed runtime settings and client dependencies

**Files:**

- Create: `src/urbanpulse/runtime/__init__.py`
- Create: `src/urbanpulse/runtime/settings.py`
- Create: `tests/runtime/test_settings.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Interfaces:**

- Produces: `RuntimeSettings`, created with `RuntimeSettings()` or
  `RuntimeSettings(_env_file=None)` in unit tests.
- Consumes: `URBANPULSE_`-prefixed environment variables and an optional local
  `.env` file.
- Provides to Task 3: service endpoints, credentials, and raw-bucket name.

- [ ] **Step 1: Write the settings tests before adding the implementation.**

```python
# tests/runtime/test_settings.py
from urbanpulse.runtime.settings import RuntimeSettings


def test_local_defaults_match_compose_host_endpoints() -> None:
    settings = RuntimeSettings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "localhost:19092"
    assert settings.postgres_dsn == (
        "postgresql://urbanpulse:urbanpulse-local-only@localhost:55433/urbanpulse"
    )
    assert settings.object_store_endpoint_url == "http://localhost:18333"
    assert settings.object_store_bucket == "urbanpulse-raw"


def test_prefixed_environment_variables_override_local_defaults(
    monkeypatch,
) -> None:
    monkeypatch.setenv("URBANPULSE_KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    monkeypatch.setenv("URBANPULSE_OBJECT_STORE_BUCKET", "alternate-raw")

    settings = RuntimeSettings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "kafka:19092"
    assert settings.object_store_bucket == "alternate-raw"
```

- [ ] **Step 2: Run the test to verify it fails because the runtime package does not exist.**

Run: `uv run pytest tests/runtime/test_settings.py -q`

Expected: FAIL during collection with `ModuleNotFoundError: No module named
'urbanpulse.runtime'`.

- [ ] **Step 3: Add the minimal dependency and CLI configuration.**

Keep the existing project metadata and Ruff configuration. Within the existing
`[project]` table, replace only `dependencies = []` with:

```toml
dependencies = [
    "boto3>=1.39,<2",
    "confluent-kafka>=2.11,<3",
    "psycopg[binary]>=3.2,<4",
    "pydantic-settings>=2.10,<3",
]
```

Then insert this table immediately after the `[project]` table:

```toml
[project.scripts]
urbanpulse-runtime-smoke = "urbanpulse.runtime.smoke:main"
```

Finally, replace the existing `[tool.pytest.ini_options]` table with:

```toml
[tool.pytest.ini_options]
addopts = ["--strict-config", "--strict-markers"]
markers = [
    "integration: requires the local Docker Compose runtime",
]
testpaths = ["tests"]
```

Keep the existing build-system and Ruff configuration unchanged. Then run:

```bash
uv lock
uv sync
```

- [ ] **Step 4: Implement the narrow settings boundary.**

```python
# src/urbanpulse/runtime/__init__.py
"""Local service configuration and diagnostics for UrbanPulse."""
```

```python
# src/urbanpulse/runtime/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class RuntimeSettings(BaseSettings):
    """Connection settings for the local Compose runtime."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="URBANPULSE_",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:19092"
    postgres_dsn: str = (
        "postgresql://urbanpulse:urbanpulse-local-only@localhost:55433/urbanpulse"
    )
    object_store_endpoint_url: str = "http://localhost:18333"
    object_store_access_key_id: str = "urbanpulse"
    object_store_secret_access_key: str = "urbanpulse-local-only"
    object_store_bucket: str = "urbanpulse-raw"
```

- [ ] **Step 5: Re-run the focused test and format/lint the new files.**

Run:

```bash
uv run pytest tests/runtime/test_settings.py -q
uv run ruff check src/urbanpulse/runtime tests/runtime/test_settings.py
```

Expected: both commands pass.

- [ ] **Step 6: Commit the independently testable settings slice.**

```bash
git add pyproject.toml uv.lock src/urbanpulse/runtime tests/runtime/test_settings.py
git commit -m "feat: add local runtime settings"
```

## Task 2: Define the persistent Docker Compose runtime

**Files:**

- Create: `compose.yaml`
- Create: `.env.example`

**Interfaces:**

- Consumes: local-only values whose defaults are defined by `RuntimeSettings`.
- Produces: Kafka at `localhost:19092`, PostgreSQL at `localhost:55433`, and
  an S3 API at `localhost:18333`.
- Provides to Task 3: healthy, persistent endpoints for the smoke command.

- [ ] **Step 1: Validate that Compose cannot yet find the runtime definition.**

Run: `docker compose config`

Expected: FAIL because `compose.yaml` does not exist yet.

- [ ] **Step 2: Add the local environment template.**

```dotenv
# Local development only. These values are deliberately non-secret.
URBANPULSE_KAFKA_BOOTSTRAP_SERVERS=localhost:19092
URBANPULSE_POSTGRES_DSN=postgresql://urbanpulse:urbanpulse-local-only@localhost:55433/urbanpulse
URBANPULSE_OBJECT_STORE_ENDPOINT_URL=http://localhost:18333
URBANPULSE_OBJECT_STORE_ACCESS_KEY_ID=urbanpulse
URBANPULSE_OBJECT_STORE_SECRET_ACCESS_KEY=urbanpulse-local-only
URBANPULSE_OBJECT_STORE_BUCKET=urbanpulse-raw
```

- [ ] **Step 3: Add the exact Compose topology.**

```yaml
# compose.yaml
name: urbanpulse

services:
  kafka:
    image: apache/kafka:4.0.2
    hostname: kafka
    ports:
      - "127.0.0.1:19092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >-
        CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: >-
        PLAINTEXT_HOST://localhost:19092,PLAINTEXT://kafka:19092
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_LISTENERS: >-
        CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
    volumes:
      - kafka-data:/tmp/kraft-combined-logs
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "/opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 > /dev/null 2>&1",
        ]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 30s

  postgres:
    image: postgres:17.10-alpine3.23
    ports:
      - "127.0.0.1:55433:5432"
    environment:
      POSTGRES_DB: urbanpulse
      POSTGRES_PASSWORD: urbanpulse-local-only
      POSTGRES_USER: urbanpulse
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U urbanpulse -d urbanpulse"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 10s

  object-store:
    image: chrislusf/seaweedfs:4.36
    command: ["mini", "-dir=/data"]
    ports:
      - "127.0.0.1:18333:8333"
    environment:
      AWS_ACCESS_KEY_ID: urbanpulse
      AWS_SECRET_ACCESS_KEY: urbanpulse-local-only
      S3_BUCKET: urbanpulse-raw
    volumes:
      - seaweed-data:/data
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "echo 'volume.list' | /usr/bin/weed shell -master=127.0.0.1:9333 > /dev/null 2>&1",
        ]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s

volumes:
  kafka-data:
  postgres-data:
  seaweed-data:
```

The two Kafka listeners are intentional: host Python clients use
`localhost:19092`; a future containerized worker can use `kafka:19092`.
Likewise, future containerized clients use `postgres:5432` and
`object-store:8333`, never `localhost`.

- [ ] **Step 4: Validate and start the stack.**

Run:

```bash
docker compose config
docker compose up --detach --wait
docker compose ps
```

Expected: configuration validates and all three services report `healthy`.

- [ ] **Step 5: Verify that the named volumes survive a restart.**

Create one Kafka topic and one PostgreSQL row, then remove/recreate the
containers without using `--volumes`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic runtime-persistence-check \
  --partitions 1 --replication-factor 1
docker compose exec postgres psql -U urbanpulse -d urbanpulse \
  -c "CREATE TABLE IF NOT EXISTS runtime_persistence_check (id integer PRIMARY KEY); INSERT INTO runtime_persistence_check (id) VALUES (1) ON CONFLICT (id) DO NOTHING;"
docker compose down
docker compose up --detach --wait
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic runtime-persistence-check
docker compose exec postgres psql -U urbanpulse -d urbanpulse \
  -c "SELECT id FROM runtime_persistence_check WHERE id = 1;"
```

Expected: the topic description succeeds and PostgreSQL returns one row with
`id = 1`. Do not run `docker compose down --volumes`; that command intentionally
deletes the local state and its KRaft cluster identity.

- [ ] **Step 6: Commit the runtime topology.**

```bash
git add compose.yaml .env.example
git commit -m "feat: add local service runtime"
```

## Task 3: Implement and test the runtime smoke command

**Files:**

- Create: `src/urbanpulse/runtime/smoke.py`
- Create: `tests/runtime/test_smoke.py`
- Create: `tests/integration/test_runtime_smoke.py`

**Interfaces:**

- Consumes: `RuntimeSettings` from Task 1 and the local endpoints from Task 2.
- Produces: `check_runtime(settings) -> tuple[ServiceCheck, ...]` and the
  `urbanpulse-runtime-smoke` command with exit status `0` when all probes pass
  and `1` otherwise.
- Does not create topics, write source data, or mutate a source checkpoint.

- [ ] **Step 1: Write unit tests for full reporting and failure handling.**

```python
# tests/runtime/test_smoke.py
from urbanpulse.runtime import smoke
from urbanpulse.runtime.settings import RuntimeSettings


def test_check_runtime_reports_all_services_when_one_probe_fails(monkeypatch) -> None:
    monkeypatch.setattr(smoke, "_check_kafka", lambda _: "1 topic visible")
    monkeypatch.setattr(
        smoke,
        "_check_postgres",
        lambda _: (_ for _ in ()).throw(ConnectionError("not reachable")),
    )
    monkeypatch.setattr(smoke, "_check_object_store", lambda _: "bucket reachable")

    checks = smoke.check_runtime(RuntimeSettings(_env_file=None))

    assert [(check.name, check.ok) for check in checks] == [
        ("kafka", True),
        ("postgres", False),
        ("object_store", True),
    ]
    assert checks[1].detail == "ConnectionError"


def test_main_returns_one_when_any_service_is_unhealthy(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        smoke,
        "check_runtime",
        lambda _: (
            smoke.ServiceCheck("kafka", True, "1 topic visible"),
            smoke.ServiceCheck("postgres", False, "ConnectionError"),
            smoke.ServiceCheck("object_store", True, "bucket reachable"),
        ),
    )

    assert smoke.main() == 1
    assert "FAIL postgres: ConnectionError" in capsys.readouterr().out
```

- [ ] **Step 2: Run the unit tests and verify they fail because `smoke.py` is absent.**

Run: `uv run pytest tests/runtime/test_smoke.py -q`

Expected: FAIL during collection with an import error for
`urbanpulse.runtime.smoke`.

- [ ] **Step 3: Implement the three probe functions and CLI without leaking credentials.**

```python
# src/urbanpulse/runtime/smoke.py
from collections.abc import Callable
from dataclasses import dataclass

import boto3
from botocore.config import Config
from confluent_kafka.admin import AdminClient
from psycopg import connect

from urbanpulse.runtime.settings import RuntimeSettings

Probe = Callable[[RuntimeSettings], str]


@dataclass(frozen=True)
class ServiceCheck:
    name: str
    ok: bool
    detail: str


def _check_kafka(settings: RuntimeSettings) -> str:
    metadata = AdminClient(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "socket.timeout.ms": 5_000,
        }
    ).list_topics(timeout=5)
    return f"{len(metadata.topics)} topics visible"


def _check_postgres(settings: RuntimeSettings) -> str:
    with connect(settings.postgres_dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone() != (1,):
                raise RuntimeError("PostgreSQL did not return the expected value")
    return "SELECT 1 succeeded"


def _check_object_store(settings: RuntimeSettings) -> str:
    client = boto3.client(
        "s3",
        endpoint_url=settings.object_store_endpoint_url,
        aws_access_key_id=settings.object_store_access_key_id,
        aws_secret_access_key=settings.object_store_secret_access_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    client.head_bucket(Bucket=settings.object_store_bucket)
    return f"bucket {settings.object_store_bucket} is reachable"


def _run_check(name: str, probe: Probe, settings: RuntimeSettings) -> ServiceCheck:
    try:
        return ServiceCheck(name=name, ok=True, detail=probe(settings))
    except Exception as error:  # The command reports all service states.
        return ServiceCheck(name=name, ok=False, detail=type(error).__name__)


def check_runtime(settings: RuntimeSettings) -> tuple[ServiceCheck, ...]:
    return (
        _run_check("kafka", _check_kafka, settings),
        _run_check("postgres", _check_postgres, settings),
        _run_check("object_store", _check_object_store, settings),
    )


def main() -> int:
    checks = check_runtime(RuntimeSettings())
    for check in checks:
        status = "PASS" if check.ok else "FAIL"
        print(f"{status} {check.name}: {check.detail}")
    return 0 if all(check.ok for check in checks) else 1
```

Do not print a caught exception message: PostgreSQL or S3 client exceptions can
contain a configured endpoint or credentials.

- [ ] **Step 4: Add the opt-in integration test.**

```python
# tests/integration/test_runtime_smoke.py
import os

import pytest

from urbanpulse.runtime.smoke import main


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("URBANPULSE_RUN_INTEGRATION") != "1",
    reason="set URBANPULSE_RUN_INTEGRATION=1 after starting Docker Compose",
)
def test_runtime_smoke_passes_against_compose_services() -> None:
    assert main() == 0
```

- [ ] **Step 5: Run unit, integration, and lint verification.**

Run:

```bash
uv run pytest tests/runtime -q
docker compose up --detach --wait
URBANPULSE_RUN_INTEGRATION=1 uv run pytest -m integration -q
uv run urbanpulse-runtime-smoke
uv run ruff check .
```

Expected: the unit suite, opt-in integration test, smoke command, and Ruff all
pass. The smoke output has three `PASS` lines.

- [ ] **Step 6: Commit the diagnostics slice.**

```bash
git add src/urbanpulse/runtime tests/runtime tests/integration pyproject.toml uv.lock
git commit -m "feat: add runtime smoke checks"
```

## Task 4: Document the workflow and record decisions

**Files:**

- Modify: `README.md`
- Modify: `docs/decision-log.md`

**Interfaces:**

- Consumes: the services and command from Tasks 1–3.
- Produces: a copy-pasteable developer workflow and ADR history consistent with
  the approved foundation spec.

- [ ] **Step 1: Update the README with the local workflow.**

Insert this section after `## Setup` and before `## Layout`:

````markdown
## Local runtime

The source-free local runtime uses Kafka, PostgreSQL, and a local
S3-compatible object store. Its named Docker volumes persist when containers
are stopped or recreated.

```bash
cp .env.example .env
uv sync
docker compose up --detach --wait
uv run urbanpulse-runtime-smoke
URBANPULSE_RUN_INTEGRATION=1 uv run pytest -m integration -q
docker compose down
```

| Service | Host endpoint |
| --- | --- |
| Kafka | `localhost:19092` |
| PostgreSQL | `localhost:55433` |
| S3 API | `http://localhost:18333` |

`.env` contains deliberately non-secret development values and is ignored by
Git. Run `docker compose down --volumes` only when intentionally resetting all
local Kafka, PostgreSQL, and object-store data.
````

- [ ] **Step 2: Add the three exact ADR entries before `## Open decisions`.**

```markdown
## ADR-0003: Ingestion architecture = two-lane, source-owned

- **Decision:** Use a batch lane for published artifacts and incremental APIs,
  and an always-on live lane of source pollers, Kafka, and a raw archive
  consumer. Share platform mechanics but keep access, checkpoint, parsing,
  schema, deduplication, time, and geospatial semantics in each source adapter.
- **Reason:** A monthly TLC artifact, a Socrata page sequence, and a GTFS-RT
  poll have different correctness rules. A shared ingestion abstraction must
  not erase those differences.
- **Tradeoffs:** The first source adapters repeat some glue and the project
  needs explicit capture metadata. In return, adding a new ingress pattern
  does not require redesigning the whole pipeline.
- **Revisit when:** A second source demonstrates a genuinely reusable adapter
  boundary that is not already represented by the catalogue.

## ADR-0004: Local runtime = Docker Compose with Kafka, PostgreSQL, and SeaweedFS

- **Decision:** Use Docker Compose for a single Kafka KRaft broker, PostgreSQL,
  and SeaweedFS in `mini` mode with named local volumes. Python workers run on
  the host through `uv`.
- **Reason:** The stack teaches the core operational surfaces while remaining
  portable to the future single-VPS deployment. SeaweedFS gives local code an
  S3 API without making the eventual B2 archive vendor-specific.
- **Tradeoffs:** This is a single-node, plaintext development runtime with no
  high availability. It is intentionally not a production Compose file.
- **Revisit when:** A source worker must run inside Compose, or a real
  production deployment begins.

## ADR-0005: Production baseline = Hetzner VPS plus Backblaze B2

- **Decision:** Run the compact always-on runtime on one Hetzner VPS and store
  durable raw payloads and encrypted PostgreSQL backups in Backblaze B2.
- **Reason:** This keeps the projected baseline near USD 11-14 per month,
  within the USD 20 soft budget and below the USD 50 hard cap while preserving
  Kafka and PostgreSQL operations as learning surfaces.
- **Tradeoffs:** One VPS is not highly available and requires monitoring,
  backup verification, and careful resource limits. Kafka is transport, not
  the long-term system of record.
- **Revisit when:** Measured retention, capture rate, recovery objectives, or
  reliability requirements exceed the budget and single-host assumptions.
```

- [ ] **Step 3: Run the complete quality and runtime verification.**

Run:

```bash
uv run pytest -q
uv run ruff check .
docker compose config
docker compose up --detach --wait
URBANPULSE_RUN_INTEGRATION=1 uv run pytest -m integration -q
uv run urbanpulse-runtime-smoke
git diff --check
```

Expected: all commands succeed. Finish with `docker compose down` to stop the
containers while preserving their named volumes.

- [ ] **Step 4: Commit the documentation slice.**

```bash
git add README.md docs/decision-log.md
git commit -m "docs: explain local runtime workflow"
```

## Review checklist

- [ ] `compose.yaml` uses only local, named volumes and exposes no source
  credential.
- [ ] Host and internal Kafka listeners are both valid for their respective
  clients.
- [ ] Runtime defaults, `.env.example`, and Compose port mappings agree.
- [ ] The smoke command checks all three services even when one is unhealthy.
- [ ] The default test suite skips Docker integration rather than requiring
  Docker Desktop.
- [ ] No real data source, source adapter, cloud resource, or deployment code
  was introduced.
