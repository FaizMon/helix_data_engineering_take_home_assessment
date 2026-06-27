import os
import tempfile
import duckdb
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments
from src.assets.staging import stg_loans, stg_payments
from src.assets.modeled import dim_loans, dim_customers, dim_dates, fct_payments, rpt_delinquency


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def test_full_pipeline_e2e(fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "e2e.duckdb")
        resources = {"duckdb": DuckDBResource(database=db_path)}

        all_assets = [
            raw_loans, raw_payments,
            stg_loans, stg_payments,
            dim_loans, dim_customers, dim_dates, fct_payments, rpt_delinquency,
        ]
        result = materialize(all_assets, resources=resources)
        assert result.success

        conn = duckdb.connect(db_path)

        # All expected tables exist
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        expected = {
            "raw_loans", "raw_payments",
            "stg_loans", "stg_payments",
            "dim_loans", "dim_customers", "dim_dates",
            "fct_payments", "rpt_delinquency",
        }
        assert expected.issubset(tables)

        # Star schema integrity: all fct_payments.payment_date_key exist in dim_dates
        orphan_dates = conn.execute("""
            SELECT COUNT(*) FROM fct_payments f
            WHERE f.payment_date_key NOT IN (SELECT date_key FROM dim_dates)
        """).fetchone()[0]
        assert orphan_dates == 0

        # Delinquency report has data
        dq = conn.execute("SELECT COUNT(*) FROM rpt_delinquency").fetchone()[0]
        assert dq > 0

        # Business query 1: delinquency by product is queryable
        rows = conn.execute(
            "SELECT product_type, delinquency_rate_pct FROM rpt_delinquency"
        ).fetchall()
        assert len(rows) > 0
        for row in rows:
            assert row[0] in ("personal", "auto", "mortgage", "student")
            assert 0 <= row[1] <= 100

        # Business query 3: freshness is queryable
        freshness = conn.execute(
            "SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM fct_payments"
        ).fetchone()
        assert freshness[0] is not None
        assert freshness[1] is not None

        conn.close()
