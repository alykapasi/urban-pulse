# Decision Log

Short ADR-style entries, newest last.

---

## ADR-0001: Target city = New York City

- **Decision:** Build UrbanPulse on NYC public mobility data.
- **Reason:** Richest open mobility ecosystem: TLC taxi/FHV trips (monthly Parquet), Citi Bike trips, MTA GTFS + real-time feeds, taxi zone shapefiles, 311, NYC Open Data, good weather coverage.
- **Options considered:** Chicago (taxi + Divvy — smaller volume/variety), SF, London (TfL — access friction).
- **Tradeoffs:** NYC TLC data is pre-bucketed to zones (no raw lat/lon for taxis since 2016), so point-level spatial joins will come from Citi Bike / other sources.
- **Revisit when:** Never, realistically — multi-city support could be a stretch goal.

## ADR-0002: Environment tooling = uv

- **Decision:** Manage Python env + dependencies with uv (pyproject.toml + uv.lock), Python 3.12.
- **Reason:** Single fast tool, lockfile reproducibility, current community default.
- **Options considered:** pip+venv (no lockfile), Poetry (heavier, slower).
- **Tradeoffs:** None significant.
- **Revisit when:** Spark enters the stack (JVM deps live outside uv anyway).

---

## Open decisions (TODO)

- [ ] First dataset + ingestion strategy
- [ ] Storage layout: raw/cleaned/curated layer naming and directory structure
- [ ] Table format (plain Parquet vs Delta/Iceberg) and partitioning scheme
- [ ] Compute engine (Polars / DuckDB / Spark) — may differ per layer
- [ ] Orchestration (none / Airflow / Dagster) — defer until >1 pipeline exists
- [ ] Geospatial modeling: zone reference tables, CRS conventions
- [ ] Timezone convention (likely America/New_York for event time, UTC internal?)
