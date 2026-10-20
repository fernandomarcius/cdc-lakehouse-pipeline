# cdc-lakehouse-pipeline

A small, generic reference implementation of **SQL Server CDC -> lake (dbt) -> exactly-once sink**.

It shows how to take the change stream of a SQL Server database, turn it into an event store and current state in a lakehouse
with [dbt](https://www.getdbt.com/), and replicate it back to a SQL Server **exactly once**, with any SQL engine that speaks the
Trino protocol (Trino itself, or [CodeLake](https://github.com/fernandomarcius/codelake-engine)).

The example domain is a toy: `customers` and `orders`, multi-tenant through a configurable tenant column (`tenant_id` by default).

## Flow

```
 SQL Server (CDC on)            Trino-protocol engine + lake                        SQL Server (target)
 +-------------------+        +--------------------------------------------+       +--------------------+
 | dbo.customers     |        |  raw        append by LSN                  |       |                    |
 | dbo.orders        |  CDC   |    customers_ct, orders_ct, lsn_time_map   |       | dbo.Customers      |
 |  -> cdc.*_CT      |------->|        |                                   |       | dbo.Orders         |
 +-------------------+ dbt    |        v                                   |       |  (append, per      |
                              |  current    event store + state            |       |   tenant, by LSN)  |
                              |    customers_events / orders_events        |       |                    |
                              |    customers_current / orders_current      |  sink | dbo.CustomersSnap. |
                              |        |                                   |------>|  (full replace)    |
                              |        v                                   |       |                    |
                              |  streaming  target-shaped, per-tenant      |       | sink_ctl.checkpoint|
                              |    customers, orders, customers_snapshot   |       |  (same transaction |
                              +--------------------------------------------+       |   as the rows)     |
                                                                                   +--------------------+
```

* `dbt/` builds the three layers. Run it on a schedule (or let your orchestrator do it).
* `sink/` reads the `streaming` tables through the engine and writes them to SQL Server with `python-tds`.
* `worker/cycle.py` runs the sink in a loop and serves `/healthz`.

## Why it is built this way

**Exactly once.** For each (table, tenant) the sink keeps the last LSN it delivered in `sink_ctl.checkpoint`. The rows and the
checkpoint update are written in *one transaction* and committed together. A crash leaves both or neither, so a restart re-reads
from the old checkpoint without duplicating or skipping anything. The checkpoint depends on the LSN, never on file names, so
compacting or rewriting lake files needs no re-seeding. A cycle is capped (`SINK_MAX_ROWS`) at an LSN boundary, never in the
middle of one.

**Literal high watermark.** The streaming models must only read events newer than what is already in the target, per tenant. A
correlated subquery (`lsn > (SELECT max(lsn) FROM this WHERE tenant = src.tenant)`) is planned by distributed engines as a join,
so the filter runs only after the whole source is read and can spill even when nothing is new. The `high_watermark` macro reads
each tenant's mark first (a cheap aggregate) and injects it as a literal (`tenant_id = 1 AND lsn > X'...'`). Literals are pushed
down to the scan, so non-matching files are skipped. Tenants with no row yet get everything. See `dbt/macros/high_watermark.sql`.

**Deterministic tie-break.** Many changes can share one commit LSN. Each event is ordered by `(__$seqval, __$operation)`, which
gives a total order; `recorded_at` adds 5 ms per position so events keep their order in time as well. The current-state view picks
the latest event by `(LSN, seqval, operation)`. Re-running a model over the same input always gives the same answer.
CDC update before-images (operation 3) are dropped: they add no information.

**Only events with a known commit time.** The event store ingests only LSNs already present in `cdc.lsn_time_mapping`. The rest
waits for the next run, so no event is lost and none carries a NULL time.

**Snapshots never empty the target.** A snapshot table is replaced (`DELETE` + `INSERT` in one transaction) only if the lake returns
at least one row. An empty read is treated as a broken upstream, not as "delete everything".

**Tolerant to a starting engine.** Engines often start before their catalogs are mounted. `CATALOG_NOT_FOUND`, connection errors
and HTTP 503 mean *wait*: the cycle is skipped, no failure is recorded, and the next cycle retries.

**Activation order: prepare -> verify -> seed -> enable.** The sink writes nothing until a checkpoint exists, so there is no flag
to forget:

1. `prepare` creates the control schema and tables in the target (needs CREATE rights; run once).
2. `verify` compares row counts per table and tenant between target and lake (read-only). Fix differences before going on.
3. `seed --confirm` sets each checkpoint to the highest LSN already in the lake (empty tables get the floor `X'00'`).
   Existing checkpoints are never overwritten.
4. *enable*: from now on `event-store` (and the worker loop) appends everything newer than the checkpoints.

## Layout

```
dbt/                 dbt project (profiles.yml uses env_var; no secrets)
  macros/            high_watermark, generate_schema_name, drop_table
  models/raw/        CDC change tables appended by LSN
  models/current/    *_events (insert-only event store) and *_current (state views)
  models/streaming/  target-shaped incremental tables and a snapshot table
sink/
  sink.py            CLI + sink logic        tables.yaml  tables to sync
  ddl/               example target tables
worker/cycle.py      loop + /healthz + SIGTERM handling
tests/               pytest, no network, no database
examples/            SQL Server CDC setup for the toy tables
```

## Configuration

Tenant column: set `tenant_key` in `sink/tables.yaml` **and** the dbt var `tenant_key` in `dbt/dbt_project.yml`
(`dbt run --vars '{tenant_key: account_id}'` also works). Add your own tables by writing the dbt models and listing them in
`tables.yaml`.

| Variable | Meaning |
|---|---|
| `ENGINE_HOST`, `ENGINE_PORT`, `ENGINE_USER`, `ENGINE_CATALOG`, `ENGINE_SCHEMA` | Trino-protocol engine that holds the lake (`ENGINE_SCHEMA` = the dbt `streaming` schema) |
| `TARGET_HOST`, `TARGET_PORT`, `TARGET_DATABASE`, `TARGET_USERNAME` | target SQL Server |
| `TARGET_PASSWORD` or `TARGET_PASSWORD_FILE` | secret (file wins) |
| `TARGET_CA_FILE` | optional CA bundle for TLS |
| `LAKE_CATALOG`, `SOURCE_CATALOG` | dbt: lake catalog / catalog of the SQL Server connector that exposes the CDC tables |
| `CYCLE_INTERVAL`, `SNAPSHOT_INTERVAL`, `PORT`, `HEALTH_MAX_SECONDS`, `SINK_ACTIVE` | worker |
| `SINK_CONFIG`, `SINK_CONTROL_SCHEMA`, `SINK_MAX_ROWS` | sink |

Logs are one JSON object per line.

## Running it

Prerequisites: a SQL Server with CDC enabled on the source tables (`examples/sqlserver_cdc_setup.sql`), an engine with a
SQL Server connector catalog (`SOURCE_CATALOG`) and a writable lake catalog (`LAKE_CATALOG`), and target tables in the target
database (`sink/ddl/example_targets.sql`).

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

export ENGINE_HOST=... ENGINE_PORT=8080 LAKE_CATALOG=lake SOURCE_CATALOG=sqlserver
dbt build --project-dir dbt --profiles-dir dbt          # raw -> current -> streaming

export TARGET_HOST=... TARGET_DATABASE=... TARGET_USERNAME=... TARGET_PASSWORD=...
python sink/sink.py prepare
python sink/sink.py verify
python sink/sink.py seed --confirm
python worker/cycle.py                                  # or: docker compose up --build  (see .env.example)
```

Run `dbt build` on a schedule next to the worker: the sink only moves what dbt has already put in `streaming`.

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest -q          # unit tests (mocks only) + `dbt parse`
```

CI (`.github/workflows/ci.yml`) runs the same two things on every push and pull request.

## Limits

This is a reference, not a product. It has not been run against a live CDC source in this repository; the unit tests mock
every connection. Adapt column types, batch sizes and the `raw` layer to your engine and connector before using it in production.

## License

Apache-2.0. See `LICENSE`.
