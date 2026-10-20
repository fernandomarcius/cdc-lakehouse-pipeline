import os
import shutil
import subprocess
import sys

import pytest

DBT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbt")


def _dbt():
    exe = shutil.which("dbt") or os.path.join(os.path.dirname(sys.executable), "dbt")
    return exe if os.path.exists(exe) else None


@pytest.mark.skipif(_dbt() is None, reason="dbt is not installed (pip install -r requirements-dev.txt)")
def test_dbt_parse(tmp_path):
    """The project must parse. `dbt parse` needs no database connection."""
    r = subprocess.run([_dbt(), "--no-use-colors", "parse", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR,
                        "--target-path", str(tmp_path / "target")], capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stdout + r.stderr


def test_high_watermark_macro_is_literal_not_correlated():
    sql = open(os.path.join(DBT_DIR, "macros", "high_watermark.sql"), encoding="utf-8").read()
    body = sql.split("{% macro")[1]
    assert "run_query" in body and "X'" in body   # watermark read at compile time and injected as a literal
    assert "t.{{" not in body                     # no correlated subquery alias
