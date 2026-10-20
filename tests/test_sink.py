import json

import pytest

import sink

CFG = {"tenant_key": "tenant_id", "tenants": None,
       "event_store": [("customers", "Customers"), ("orders", "Orders")], "snapshot": [("customers_snapshot", "CustomersSnapshot")]}


class Fake:
    """Connection stand-in: records commit/rollback/close."""

    def __init__(self):
        self.calls = []

    def close(self):
        self.calls.append("close")

    def rollback(self):
        self.calls.append("rollback")

    def commit(self):
        self.calls.append("commit")


class TestPureHelpers:
    def test_hex_lsn(self):
        assert sink.hex_lsn(b"\x00\x1e\x62\xc5") == "001E62C5"

    def test_ident_rejects_injection(self):
        assert sink.ident("Customers") == "Customers"
        for bad in ("a b", "x];DROP TABLE y;--", "a.b", "", None):
            with pytest.raises(ValueError):
                sink.ident(bad)

    def test_tenant_literal(self):
        assert sink.tenant_literal(7) == "7"
        assert sink.tenant_literal("acme-1") == "'acme-1'"
        for bad in ("a'b", "1; DROP", True):
            with pytest.raises(ValueError):
                sink.tenant_literal(bad)

    def test_common_columns_case_insensitive_and_reports_missing(self):
        pairs, missing = sink.common_columns(["Id", "Tenant_Id", "Extra", "CreatedAt"], ["id", "tenant_id", "createdat", "x"])
        assert pairs == [("Id", "id"), ("Tenant_Id", "tenant_id"), ("CreatedAt", "createdat")]
        assert missing == ["Extra"]

    def test_batches_respect_parameter_limit(self):
        rows = [(i,) * 40 for i in range(105)]
        sizes = [len(b) for b in sink.batches(rows, 40)]
        assert sizes == [50, 50, 5]  # 2000 // 40 rows per statement
        assert all(s * 40 <= 2100 for s in sizes)
        assert list(sink.batches([], 5)) == []
        assert [len(b) for b in sink.batches([(1,)] * 3, 5000)] == [1, 1, 1]  # at least one row per batch

    def test_insert_sql_shape(self):
        assert sink.insert_sql("Customers", ["id", "name"], 2) == "INSERT INTO dbo.[Customers] ([id], [name]) VALUES (%s, %s), (%s, %s)"

    def test_log_is_json(self, capsys):
        sink.log("INFO", "hello", n=1)
        line = json.loads(capsys.readouterr().out)
        assert line["message"] == "hello" and line["n"] == 1 and line["level"] == "INFO"

    def test_load_config(self, tmp_path):
        f = tmp_path / "t.yaml"
        f.write_text("tenant_key: account_id\nevent_store:\n  - {lake: a, target: A}\nsnapshot: []\n")
        cfg = sink.load_config(str(f))
        assert cfg["tenant_key"] == "account_id" and cfg["event_store"] == [("a", "A")] and cfg["tenants"] is None

    def test_load_config_rejects_bad_identifier(self, tmp_path):
        f = tmp_path / "t.yaml"
        f.write_text("event_store:\n  - {lake: 'a;b', target: A}\n")
        with pytest.raises(ValueError):
            sink.load_config(str(f))

    def test_shipped_tables_yaml_is_valid(self):
        cfg = sink.load_config()
        assert cfg["event_store"] and cfg["tenant_key"] == "tenant_id"


class TestCatalogMissing:
    @pytest.mark.parametrize("err", [
        "TrinoUserError(name=CATALOG_NOT_FOUND, message=\"Catalog 'lake' does not exist\")",
        "TrinoConnectionError('failed to execute: Max retries exceeded')",
        "Http503Error('error 503: service unavailable')"])
    def test_waiting_conditions(self, err):
        assert sink.catalog_missing(RuntimeError(err))

    def test_real_failure_is_not_waiting(self):
        assert not sink.catalog_missing(RuntimeError("TrinoUserError(name=SYNTAX_ERROR)"))


@pytest.fixture
def cycle(monkeypatch):
    """Runs run_event_store with every external piece replaced."""
    def build(error=None, is_enabled=True):
        calls, records = [], []
        monkeypatch.setattr(sink, "connect_target", lambda autocommit=False: Fake())
        monkeypatch.setattr(sink, "connect_engine", lambda: Fake())
        monkeypatch.setattr(sink, "enabled", lambda cx: is_enabled)
        monkeypatch.setattr(sink, "record", lambda *a, **k: records.append(a))
        monkeypatch.setattr(sink, "log", lambda *a, **k: None)

        def fake_sink(cfg, cx, lake, lake_table, target):
            calls.append(lake_table)
            if error:
                raise error

        monkeypatch.setattr(sink, "sink_event_store", fake_sink)
        return calls, records
    return build


class TestRunEventStore:
    def test_catalog_missing_is_waiting_not_failure(self, cycle):
        calls, records = cycle(RuntimeError("TrinoUserError(name=CATALOG_NOT_FOUND)"))
        assert sink.run_event_store(CFG) == 0
        assert records == []                              # no FAIL row: alerts read those
        assert calls == ["customers"]                     # stopped at the first table
        assert sink.cycle_state["lake_unavailable"] is True

    def test_engine_down_is_waiting(self, cycle):
        calls, records = cycle(RuntimeError("TrinoConnectionError('refused')"))
        assert (sink.run_event_store(CFG), records, len(calls)) == (0, [], 1)

    def test_real_error_counts_logs_fail_and_continues(self, cycle):
        calls, records = cycle(RuntimeError("boom"))
        assert sink.run_event_store(CFG) == 2
        assert calls == ["customers", "orders"]           # one table failing does not stop the others
        assert [r[3] for r in records] == ["FAIL", "FAIL"]
        assert sink.cycle_state["lake_unavailable"] is False

    def test_only_filter(self, cycle):
        calls, _ = cycle()
        sink.run_event_store(CFG, only="Orders")
        assert calls == ["orders"]

    def test_not_enabled_writes_nothing(self, cycle):
        calls, _ = cycle(is_enabled=False)
        assert sink.run_event_store(CFG) == 0 and calls == []


class FakeCursor:
    def __init__(self, owner, rows):
        self.owner, self.rows = owner, list(rows)
        self.description = [(c,) for c in owner.lake_cols]

    def execute(self, sql, params=None):
        self.owner.sql.append(sql)

    def fetchmany(self, n):
        out, self.rows = self.rows[:n], self.rows[n:]
        return out

    def fetchall(self):
        return []

    def close(self):
        pass


class FakeLake(Fake):
    def __init__(self, rows, cols=("name", "tenant_id")):
        super().__init__()
        self.rows, self.lake_cols, self.sql = rows, cols, []

    def cursor(self):
        return FakeCursor(self, self.rows)


class TestExactlyOnce:
    def test_rows_and_checkpoint_commit_together(self, monkeypatch):
        order = []
        cx = Fake()
        monkeypatch.setattr(sink, "target_columns", lambda cx, t: ["name", "tenant_id"])
        monkeypatch.setattr(sink, "get_checkpoint", lambda cx, t, ten: b"\x01")
        monkeypatch.setattr(sink, "execute", lambda c, sql, params=None: order.append("insert" if sql.startswith("INSERT") else "other"))
        monkeypatch.setattr(sink, "save_checkpoint", lambda c, t, ten, lsn, n: order.append(("checkpoint", lsn, n)))
        monkeypatch.setattr(cx, "commit", lambda: order.append("commit"))
        lake = FakeLake([("a", 1, b"\x05"), ("b", 1, b"\x09")])
        monkeypatch.setattr(sink, "query", lambda c, sql, params=None: [(b"\x09",)] if "max(l)" in sql else [])
        cfg = {**CFG, "tenants": [1]}
        assert sink.sink_event_store(cfg, cx, lake, "customers", "Customers") == 2
        # insert happens before the checkpoint, and a single commit follows the checkpoint
        assert order == ["insert", ("checkpoint", b"\x09", 2), "commit"]

    def test_failure_before_commit_leaves_checkpoint_unsaved(self, monkeypatch):
        saved = []
        cx = Fake()
        monkeypatch.setattr(sink, "target_columns", lambda cx, t: ["name", "tenant_id"])
        monkeypatch.setattr(sink, "get_checkpoint", lambda cx, t, ten: b"\x01")
        monkeypatch.setattr(sink, "query", lambda c, sql, params=None: [(b"\x09",)] if "max(l)" in sql else [])

        def boom(c, sql, params=None):
            raise RuntimeError("insert failed")

        monkeypatch.setattr(sink, "execute", boom)
        monkeypatch.setattr(sink, "save_checkpoint", lambda *a: saved.append(a))
        lake = FakeLake([("a", 1, b"\x05")])
        with pytest.raises(RuntimeError):
            sink.sink_event_store({**CFG, "tenants": [1]}, cx, lake, "customers", "Customers")
        assert saved == [] and "commit" not in cx.calls

    def test_tenant_without_checkpoint_is_skipped(self, monkeypatch):
        cx = Fake()
        monkeypatch.setattr(sink, "target_columns", lambda cx, t: ["name"])
        monkeypatch.setattr(sink, "get_checkpoint", lambda cx, t, ten: None)
        monkeypatch.setattr(sink, "log", lambda *a, **k: None)
        lake = FakeLake([], cols=("name",))
        assert sink.sink_event_store({**CFG, "tenants": [1]}, cx, lake, "customers", "Customers") == 0
        assert lake.sql == [] or all("LIMIT 0" in s for s in lake.sql)


class TestSnapshot:
    def _patch(self, monkeypatch, executed):
        monkeypatch.setattr(sink, "target_columns", lambda cx, t: ["name", "tenant_id"])
        monkeypatch.setattr(sink, "execute", lambda c, sql, params=None: executed.append(sql))
        monkeypatch.setattr(sink, "log", lambda *a, **k: None)

    def test_empty_lake_never_empties_target(self, monkeypatch):
        executed = []
        self._patch(monkeypatch, executed)
        cx = Fake()
        with pytest.raises(RuntimeError, match="NOT emptied"):
            sink.sink_snapshot(CFG, cx, FakeLake([]), "customers_snapshot", "CustomersSnapshot")
        assert executed == [] and "commit" not in cx.calls

    def test_replaces_when_rows_present(self, monkeypatch):
        executed = []
        self._patch(monkeypatch, executed)
        cx = Fake()
        sink.sink_snapshot(CFG, cx, FakeLake([("a", 1), ("b", 2)]), "customers_snapshot", "CustomersSnapshot")
        assert executed[0] == "DELETE FROM dbo.[CustomersSnapshot]"
        assert executed[1].startswith("INSERT INTO dbo.[CustomersSnapshot]")
        assert cx.calls == ["commit"]


class TestSeed:
    def test_seeds_max_lsn_and_floor_for_empty(self, monkeypatch):
        saved = []
        cfg = {**CFG, "tenants": [1, 2], "event_store": [("customers", "Customers")]}
        monkeypatch.setattr(sink, "connect_target", lambda autocommit=False: Fake())
        monkeypatch.setattr(sink, "connect_engine", lambda: Fake())
        monkeypatch.setattr(sink, "get_checkpoint", lambda cx, t, ten: None)
        monkeypatch.setattr(sink, "save_checkpoint", lambda cx, t, ten, lsn, n: saved.append((ten, lsn)))
        monkeypatch.setattr(sink, "log", lambda *a, **k: None)
        monkeypatch.setattr(sink, "query", lambda c, sql, params=None: [(b"\x00\x07",)] if "= 1" in sql else [(None,)])
        sink.seed(cfg)
        assert saved == [(1, b"\x00\x07"), (2, sink.LSN_FLOOR)]

    def test_existing_checkpoint_is_kept(self, monkeypatch):
        saved = []
        cfg = {**CFG, "tenants": [1], "event_store": [("customers", "Customers")]}
        monkeypatch.setattr(sink, "connect_target", lambda autocommit=False: Fake())
        monkeypatch.setattr(sink, "connect_engine", lambda: Fake())
        monkeypatch.setattr(sink, "get_checkpoint", lambda cx, t, ten: b"\x05")
        monkeypatch.setattr(sink, "save_checkpoint", lambda *a: saved.append(a))
        sink.seed(cfg)
        assert saved == []

    def test_seed_requires_confirm(self):
        with pytest.raises(SystemExit):
            sink.main(["sink.py", "seed"])


class TestVerify:
    def test_reports_difference(self, monkeypatch, capsys):
        cfg = {**CFG, "event_store": [("customers", "Customers")]}
        monkeypatch.setattr(sink, "connect_target", lambda autocommit=False: Fake())
        monkeypatch.setattr(sink, "connect_engine", lambda: Fake())
        monkeypatch.setattr(sink, "query", lambda c, sql, params=None: [(1, 10)] if "dbo." in sql else [(1, 12)])
        assert sink.verify(cfg) == 1
        assert "DIFFERS" in capsys.readouterr().out

    def test_all_equal(self, monkeypatch):
        cfg = {**CFG, "event_store": [("customers", "Customers")]}
        monkeypatch.setattr(sink, "connect_target", lambda autocommit=False: Fake())
        monkeypatch.setattr(sink, "connect_engine", lambda: Fake())
        monkeypatch.setattr(sink, "query", lambda c, sql, params=None: [(1, 10)])
        assert sink.verify(cfg) == 0
