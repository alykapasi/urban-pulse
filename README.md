# UrbanPulse

City-scale mobility data platform for **New York City**. Combines public mobility
datasets (taxi trips, bike-share, transit feeds, weather, events, zone geometries)
into a local-first lakehouse for movement-pattern analytics.

Built as a portfolio data engineering project — architecture decisions are made
incrementally and recorded in [docs/decision-log.md](docs/decision-log.md).

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
uv sync          # create venv + install deps
uv run pytest    # run tests
uv run ruff check .
```

## Layout

```
src/urbanpulse/   # pipeline code
tests/            # pytest suite
docs/             # decision log, data source notes, design docs
data/             # local datasets (gitignored)
```

## Status

Scaffold only. First data slice TBD — see decision log.
