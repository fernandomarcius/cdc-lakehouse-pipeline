#!/usr/bin/env python3
"""Exactly-once sink: lake tables (read through a Trino-protocol engine) -> SQL Server.

Event-store tables are appended per tenant from a checkpoint (last LSN). The checkpoint is written in the SAME transaction as
the rows, so a crash either keeps both or neither: no duplicates, no gaps. Snapshot tables are replaced as a whole, but only
when the lake returns at least one row.

Activation order (nothing is written until the last step):  prepare -> verify -> seed -> enable

    python sink.py prepare             create the control schema/tables in the target
    python sink.py verify              compare row counts target x lake per table and tenant (read-only)
    python sink.py seed --confirm      checkpoint = highest LSN already in the lake (only where none exists)
    python sink.py event-store [name]  one event-store cycle (this is "enable": the worker calls it in a loop)
    python sink.py snapshot            one snapshot cycle

Environment:
    ENGINE_HOST, ENGINE_PORT, ENGINE_USER, ENGINE_CATALOG, ENGINE_SCHEMA   Trino-protocol engine holding the lake
    TARGET_HOST, TARGET_PORT, TARGET_DATABASE, TARGET_USERNAME             SQL Server
    TARGET_PASSWORD or TARGET_PASSWORD_FILE                                 secret (file wins when both are set)
    TARGET_CA_FILE                                                          optional CA bundle for TLS
    SINK_CONFIG (default: tables.yaml next to this file), SINK_CONTROL_SCHEMA (default: sink_ctl), SINK_MAX_ROWS
"""
import binascii
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
NAME_OK = re.compile(r"^[A-Za-z0-9_]+$")
TENANT_OK = re.compile(r"^[A-Za-z0-9_\-]+$")
PARAM_BATCH = 2000      # SQL Server allows 2100 parameters per statement
LSN_FLOOR = b"\x00"     # lowest possible mark: an empty table is seeded here so the first row that arrives is sent
LSN_COL = "_src_start_lsn"
CONTROL_SCHEMA = os.environ.get("SINK_CONTROL_SCHEMA", "sink_ctl")
MAX_ROWS = int(os.environ.get("SINK_MAX_ROWS", "200000"))   # per table and tenant, per cycle


def log(level, msg, **kv):
    print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "level": level, "logger": "cdc_sink", "message": msg, **kv},
                     ensure_ascii=False, default=str), flush=True)


# ---- pure helpers (unit-tested without a database) ----------------------------------------------------------------
def hex_lsn(b):
    """bytes -> 'ABCD...' (upper case), for X'..' literals."""
    return binascii.hexlify(bytes(b)).decode().upper()


def ident(name):
    """Validate an identifier before it is interpolated into SQL."""
    if not isinstance(name, str) or not NAME_OK.match(name):
        raise ValueError(f"invalid identifier: {name!r}")
    return name


def tenant_literal(value):
    """SQL literal for a tenant id: integers bare, other values as a validated quoted string."""
    if isinstance(value, bool):
        raise ValueError("invalid tenant value: bool")
    if isinstance(value, int):
        return str(value)
    text = str(value)
    if not TENANT_OK.match(text):
        raise ValueError(f"invalid tenant value: {value!r}")
    return f"'{text}'"


def common_columns(target_cols, lake_cols):
    """Target columns (target order) that also exist in the lake, case-insensitive.
    Returns ([(target_name, lake_name)], missing_in_lake). A target column missing in the lake is left out of the INSERT
    (the database DEFAULT applies) and reported."""
    by_lower = {c.lower(): c for c in lake_cols}
    pairs, missing = [], []
    for t in target_cols:
        lc = by_lower.get(t.lower())
        if lc is None:
            missing.append(t)
        else:
            pairs.append((t, lc))
    return pairs, missing


def batches(rows, n_columns, limit=PARAM_BATCH):
    """Group rows into multi-row INSERT batches that respect the parameter limit."""
    per_batch = max(1, limit // max(1, n_columns))
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= per_batch:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_sql(target, columns, n_rows):
    cols = ", ".join(f"[{ident(c)}]" for c in columns)
    one = "(" + ", ".join(["%s"] * len(columns)) + ")"
    return f"INSERT INTO dbo.[{ident(target)}] ({cols}) VALUES " + ", ".join([one] * n_rows)


def catalog_missing(err):
    """True when the engine is up but has no catalog yet (CATALOG_NOT_FOUND), or is restarting (connection refused / 503).
    Both are waiting, not failure: engines routinely start before their catalogs are mounted."""
    text = repr(err)
    return any(m in text for m in ("CATALOG_NOT_FOUND", "TrinoConnectionError", "Http503Error"))


def load_config(path=None):
    import yaml
    path = path or os.environ.get("SINK_CONFIG") or os.path.join(HERE, "tables.yaml")
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    cfg = {
        "tenant_key": ident(raw.get("tenant_key", "tenant_id")),
        "tenants": raw.get("tenants"),
        "event_store": [(ident(t["lake"]), ident(t["target"])) for t in raw.get("event_store") or []],
        "snapshot": [(ident(t["lake"]), ident(t["target"])) for t in raw.get("snapshot") or []],
    }
    if cfg["tenants"] is not None:
        for t in cfg["tenants"]:
            tenant_literal(t)  # validate early
    return cfg


# ---- connections --------------------------------------------------------------------------------------------------
def _password():
    path = os.environ.get("TARGET_PASSWORD_FILE")
    if path:
        with open(path) as f:
            return f.read().strip()
    return os.environ["TARGET_PASSWORD"]


def connect_target(autocommit=False):
    import pytds
    return pytds.connect(
        dsn=os.environ["TARGET_HOST"], port=int(os.environ.get("TARGET_PORT", "1433")),
        database=os.environ["TARGET_DATABASE"], user=os.environ["TARGET_USERNAME"], password=_password(),
        login_timeout=30, timeout=int(os.environ.get("SINK_QUERY_TIMEOUT", "600")), autocommit=autocommit,
        cafile=(os.environ.get("TARGET_CA_FILE") or None))


def connect_engine():
    import trino
    return trino.dbapi.connect(
        host=os.environ.get("ENGINE_HOST", "127.0.0.1"), port=int(os.environ.get("ENGINE_PORT", "8080")),
        user=os.environ.get("ENGINE_USER", "sink"), catalog=os.environ.get("ENGINE_CATALOG", "lake"),
        schema=os.environ.get("ENGINE_SCHEMA", "streaming"), http_scheme="http", request_timeout=600)


def to_params(params):
    """bytes -> pytds.Binary. python-tds sends plain `bytes` as text, which breaks LSNs and binary columns."""
    if params is None or not any(isinstance(v, (bytes, bytearray)) for v in params):
        return params
    import pytds
    return [pytds.Binary(bytes(v)) if isinstance(v, (bytes, bytearray)) else v for v in params]


def query(cx, sql, params=None):
    cur = cx.cursor()
    try:
        params = to_params(params)
        cur.execute(sql, params) if params is not None else cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def execute(cx, sql, params=None):
    cur = cx.cursor()
    try:
        cur.execute(sql, to_params(params)) if params is not None else cur.execute(sql)
    finally:
        cur.close()


# ---- control tables -----------------------------------------------------------------------------------------------
def control_ddl():
    s = ident(CONTROL_SCHEMA)
    return [
        f"IF SCHEMA_ID('{s}') IS NULL EXEC('CREATE SCHEMA {s}')",
        f"""IF OBJECT_ID('{s}.checkpoint') IS NULL
CREATE TABLE {s}.checkpoint (
    table_name varchar(80) NOT NULL, tenant varchar(64) NOT NULL, last_lsn varbinary(10) NOT NULL,
    rows_total bigint NOT NULL CONSTRAINT DF_{s}_ck_rows DEFAULT (0),
    updated_at datetime2(3) NOT NULL CONSTRAINT DF_{s}_ck_upd DEFAULT (SYSDATETIME()),
    CONSTRAINT PK_{s}_checkpoint PRIMARY KEY (table_name, tenant))""",
        f"""IF OBJECT_ID('{s}.run_log') IS NULL
CREATE TABLE {s}.run_log (
    run_id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY, table_name varchar(80) NOT NULL, started_at datetime2(3) NOT NULL,
    ended_at datetime2(3) NULL, rows_inserted int NULL, status varchar(10) NOT NULL, error_msg nvarchar(max) NULL)""",
    ]


def prepare():
    """Create the control schema and tables. Needs CREATE rights; run once."""
    cx = connect_target(autocommit=True)
    try:
        for stmt in control_ddl():
            execute(cx, stmt)
        for obj in ("checkpoint", "run_log"):
            found = query(cx, "SELECT OBJECT_ID(%s)", (f"{CONTROL_SCHEMA}.{obj}",))[0][0]
            log("INFO" if found else "ERROR", "control_object", object=f"{CONTROL_SCHEMA}.{obj}", exists=bool(found))
        log("INFO", "prepared")
    finally:
        cx.close()


def get_checkpoint(cx, table, tenant):
    r = query(cx, f"SELECT last_lsn FROM {CONTROL_SCHEMA}.checkpoint WHERE table_name = %s AND tenant = %s", (table, str(tenant)))
    return bytes(r[0][0]) if r else None


def save_checkpoint(cx, table, tenant, lsn, n_rows):
    """Upsert inside the caller's transaction: this is what makes the sink exactly-once."""
    execute(cx, f"""
        MERGE {CONTROL_SCHEMA}.checkpoint AS t USING (SELECT %s AS table_name, %s AS tenant) AS s
          ON t.table_name = s.table_name AND t.tenant = s.tenant
        WHEN MATCHED THEN UPDATE SET last_lsn = %s, rows_total = rows_total + %s, updated_at = SYSDATETIME()
        WHEN NOT MATCHED THEN INSERT (table_name, tenant, last_lsn, rows_total) VALUES (s.table_name, s.tenant, %s, %s);""",
            [table, str(tenant), lsn, n_rows, lsn, n_rows])


def enabled(cx):
    """The sink acts only after seeding: control table exists AND holds at least one checkpoint.
    There is no on/off flag to forget: without `prepare` + `seed --confirm` the loop runs but writes nothing
    (not even snapshots, which delete and rewrite a table)."""
    try:
        if query(cx, f"SELECT OBJECT_ID('{CONTROL_SCHEMA}.checkpoint')")[0][0] is None:
            return False
        return query(cx, f"SELECT COUNT_BIG(*) FROM {CONTROL_SCHEMA}.checkpoint")[0][0] > 0
    finally:
        cx.rollback()


def record(table, started, rows, status, error=None):
    """Best-effort run log on its own connection, so it survives a rolled-back data transaction."""
    try:
        c = connect_target(autocommit=True)
        try:
            execute(c, f"""INSERT {CONTROL_SCHEMA}.run_log(table_name, started_at, ended_at, rows_inserted, status, error_msg)
                VALUES (%s, %s, SYSDATETIME(), %s, %s, %s)""",
                    [table, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(started)), rows, status, error])
        finally:
            c.close()
    except Exception:
        pass


# ---- schema discovery ---------------------------------------------------------------------------------------------
def target_columns(cx, target):
    rows = query(cx, """SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(%s) AND is_identity = 0
                        AND is_computed = 0 AND LEFT(name, 1) <> '_' ORDER BY column_id""", (f"dbo.{ident(target)}",))
    return [r[0] for r in rows]


def lake_columns(lake, table):
    cur = lake.cursor()
    try:
        cur.execute(f'SELECT * FROM "{ident(table)}" LIMIT 0')
        cur.fetchall()
        return [d[0] for d in cur.description]
    finally:
        cur.close()


def discover_tenants(cfg, lake, table):
    if cfg["tenants"] is not None:
        return list(cfg["tenants"])
    rows = query(lake, f'SELECT DISTINCT "{cfg["tenant_key"]}" FROM "{ident(table)}" WHERE "{cfg["tenant_key"]}" IS NOT NULL')
    return sorted(r[0] for r in rows)


# ---- event store --------------------------------------------------------------------------------------------------
def sink_event_store(cfg, cx, lake, lake_table, target):
    """One cycle of one table. Returns the number of rows inserted (all tenants)."""
    tk = cfg["tenant_key"]
    pairs, missing = common_columns(target_columns(cx, target), lake_columns(lake, lake_table))
    if not pairs:
        raise RuntimeError(f"{target}: no column in common with {lake_table}")
    if missing:
        log("WARN", "target_columns_missing_in_lake", table=lake_table, columns=missing)
    total = 0
    for tenant in discover_tenants(cfg, lake, lake_table):
        ck = get_checkpoint(cx, lake_table, tenant)
        cx.rollback()  # close the implicit transaction opened by the checkpoint read
        if ck is None:
            log("WARN", "no_checkpoint_skipping", table=lake_table, tenant=tenant, hint="python sink.py seed --confirm")
            continue
        base = f'FROM "{lake_table}" WHERE "{tk}" = {tenant_literal(tenant)} AND "{LSN_COL}" > X\'{hex_lsn(ck)}\''
        # Cap the cycle at MAX_ROWS rows: find the LSN of the last row of the window, then read up to it. Cutting at an LSN
        # (never in the middle of one) keeps `> checkpoint` exact on the next cycle.
        cap = query(lake, f'SELECT max(l) FROM (SELECT "{LSN_COL}" AS l {base} ORDER BY 1 LIMIT {MAX_ROWS})')
        ceiling = cap[0][0] if cap else None
        if ceiling is None:
            continue
        select = ", ".join(f'"{ident(h)}"' for _, h in pairs)
        cur = lake.cursor()
        cur.execute(f'SELECT {select}, "{LSN_COL}" {base} AND "{LSN_COL}" <= X\'{hex_lsn(bytes(ceiling))}\' ORDER BY "{LSN_COL}"')
        n, last, started = 0, ck, time.time()
        try:
            while True:
                chunk = cur.fetchmany(5000)
                if not chunk:
                    break
                last = bytes(chunk[-1][-1])
                for batch in batches((tuple(r[:-1]) for r in chunk), len(pairs)):
                    execute(cx, insert_sql(target, [t for t, _ in pairs], len(batch)), [v for row in batch for v in row])
                    n += len(batch)
        finally:
            cur.close()
        save_checkpoint(cx, lake_table, tenant, last, n)
        cx.commit()  # rows + checkpoint together
        total += n
        log("INFO", "event_store_written", table=lake_table, target=target, tenant=tenant, rows=n, seconds=round(time.time() - started, 1))
    return total


# Result of the last cycle, for the worker's heartbeat. lake_unavailable: engine down / no catalog, the cycle was skipped.
cycle_state = {"lake_unavailable": False}


def _close_quietly(*conns):
    for c in conns:
        try:
            c.close()
        except Exception:
            pass


def run_event_store(cfg, only=None):
    """One cycle over all event-store tables. One table failing does not stop the others. Returns the number of failures."""
    cycle_state["lake_unavailable"] = False
    cx = connect_target(autocommit=False)
    if not enabled(cx):
        log("INFO", "waiting_for_seed", hint="sink.py prepare; verify; seed --confirm")
        cx.close()
        return 0
    lake = connect_engine()
    failures = 0
    try:
        for lake_table, target in cfg["event_store"]:
            if only and only not in (lake_table, target):
                continue
            started = time.time()
            try:
                sink_event_store(cfg, cx, lake, lake_table, target)
            except Exception as e:
                try:
                    cx.rollback()
                except Exception:
                    pass
                if catalog_missing(e):
                    # Engine restarting or catalog not mounted yet: wait. Not a failure, no FAIL row, and no point trying the rest.
                    log("WARN", "lake_unavailable", table=lake_table, hint="engine down or catalog missing; waiting")
                    cycle_state["lake_unavailable"] = True
                    break
                failures += 1
                log("ERROR", "event_store_failed", table=lake_table, error=repr(e)[:500])
                record(lake_table, started, 0, "FAIL", repr(e)[:3000])
    finally:
        _close_quietly(lake, cx)
    return failures


# ---- snapshot -----------------------------------------------------------------------------------------------------
def sink_snapshot(cfg, cx, lake, lake_table, target):
    pairs, _ = common_columns(target_columns(cx, target), lake_columns(lake, lake_table))
    if not pairs:
        raise RuntimeError(f"{target}: no column in common with {lake_table}")
    select = ", ".join(f'"{ident(h)}"' for _, h in pairs)
    cur = lake.cursor()
    cur.execute(f'SELECT {select} FROM "{ident(lake_table)}"')
    rows = []
    try:
        while True:
            chunk = cur.fetchmany(5000)
            if not chunk:
                break
            rows.extend(tuple(r) for r in chunk)
    finally:
        cur.close()
    if not rows:
        # An empty read is far more likely a broken upstream than a legitimately empty table: never wipe the target for it.
        raise RuntimeError(f"snapshot of {lake_table} returned no rows: target NOT emptied")
    execute(cx, f"DELETE FROM dbo.[{ident(target)}]")  # same transaction as the INSERTs; works without ALTER rights
    for batch in batches(rows, len(pairs)):
        execute(cx, insert_sql(target, [t for t, _ in pairs], len(batch)), [v for row in batch for v in row])
    cx.commit()
    log("INFO", "snapshot_written", table=lake_table, target=target, rows=len(rows))


def run_snapshot(cfg):
    cx = connect_target(autocommit=False)
    if not enabled(cx):
        log("INFO", "waiting_for_seed_snapshot_skipped")
        cx.close()
        return 0
    lake = connect_engine()
    failures = 0
    try:
        for lake_table, target in cfg["snapshot"]:
            try:
                sink_snapshot(cfg, cx, lake, lake_table, target)
            except Exception as e:
                try:
                    cx.rollback()
                except Exception:
                    pass
                if catalog_missing(e):
                    log("WARN", "lake_unavailable", table=lake_table)
                    break
                failures += 1
                log("ERROR", "snapshot_failed", table=lake_table, error=repr(e)[:500])
    finally:
        _close_quietly(lake, cx)
    return failures


# ---- verify and seed ----------------------------------------------------------------------------------------------
def verify(cfg):
    """Compare row counts per tenant: dbo.<target> x lake table. Read-only. Returns 0 when everything matches."""
    tk = cfg["tenant_key"]
    cx = connect_target(autocommit=True)
    lake = connect_engine()
    ok = True
    try:
        for lake_table, target in cfg["event_store"]:
            try:
                d = {str(k): int(v) for k, v in query(cx, f"SELECT [{tk}], COUNT_BIG(*) FROM dbo.[{ident(target)}] GROUP BY [{tk}]")}
                l = {str(k): int(v) for k, v in query(lake, f'SELECT "{tk}", count(*) FROM "{ident(lake_table)}" GROUP BY 1')}
            except Exception as e:
                ok = False
                print(f"{target:30s} ERROR {repr(e)[:120]}")
                continue
            tenants = [str(t) for t in cfg["tenants"]] if cfg["tenants"] is not None else sorted(set(d) | set(l))
            for t in tenants:
                dg, lg = d.get(t, 0), l.get(t, 0)
                ok = ok and dg == lg
                print(f"{target:30s} tenant {t:>8}  target={dg:>10}  lake={lg:>10}  {'ok' if dg == lg else 'DIFFERS'}")
    finally:
        _close_quietly(cx, lake)
    return 0 if ok else 1


def seed(cfg):
    """checkpoint = highest LSN in the lake, only where none exists. Rows already in the lake are assumed to be in the target
    (that is what `verify` is for); everything newer is sent by the sink."""
    tk = cfg["tenant_key"]
    cx = connect_target(autocommit=False)
    lake = connect_engine()
    try:
        for lake_table, _ in cfg["event_store"]:
            for tenant in discover_tenants(cfg, lake, lake_table):
                existing = get_checkpoint(cx, lake_table, tenant)
                cx.rollback()
                if existing is not None:
                    continue
                r = query(lake, f'SELECT max("{LSN_COL}") FROM "{ident(lake_table)}" WHERE "{tk}" = {tenant_literal(tenant)}')
                # A table with no rows still gets a checkpoint: without one the sink would skip it forever and the first row
                # to arrive would never be sent. X'00' is the same floor the dbt models use.
                empty = not r or r[0][0] is None
                lsn = LSN_FLOOR if empty else bytes(r[0][0])
                save_checkpoint(cx, lake_table, tenant, lsn, 0)
                cx.commit()
                log("INFO", "checkpoint_seeded", table=lake_table, tenant=tenant, lsn=hex_lsn(lsn), lake_empty=empty)
    finally:
        _close_quietly(cx, lake)


def main(argv):
    cmd = argv[1] if len(argv) > 1 else ""
    if cmd == "prepare":
        prepare()
    elif cmd == "verify":
        return verify(load_config())
    elif cmd == "seed":
        if "--confirm" not in argv:
            sys.exit("seed writes checkpoints: run `verify` first, then `seed --confirm`")
        seed(load_config())
    elif cmd == "event-store":
        return 1 if run_event_store(load_config(), argv[2] if len(argv) > 2 else None) else 0
    elif cmd == "snapshot":
        return 1 if run_snapshot(load_config()) else 0
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
