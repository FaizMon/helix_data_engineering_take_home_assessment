import os
import duckdb
import pytest

from src.tasks.raw import load_raw_loans, load_raw_payments
from src.tasks.staging import transform_stg_loans, transform_stg_payments
from src.tasks.modeled import (
    build_dim_loans,
    build_dim_customers,
    build_dim_dates,
    build_fct_payments,
    build_rpt_delinquency,
)


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def test_full_pipeline_e2e(fixtures_dir):
    conn = duckdb.connect(":memory:")

    load_raw_loans(conn, data_dir=fixtures_dir)
    load_raw_payments(conn, data_dir=fixtures_dir)
    transform_stg_loans(conn)
    transform_stg_payments(conn)
    build_dim_loans(conn)
    build_dim_customers(conn)
    build_dim_dates(conn)
    build_fct_payments(conn)
    build_rpt_delinquency(conn)

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

    # Business query: delinquency by product is queryable
    rows = conn.execute(
        "SELECT product_type, delinquency_rate_pct FROM rpt_delinquency"
    ).fetchall()
    assert len(rows) > 0
    for row in rows:
        assert row[0] in ("personal", "auto", "mortgage", "student")
        assert 0 <= row[1] <= 100

    # Business query: freshness is queryable
    freshness = conn.execute(
        "SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM fct_payments"
    ).fetchone()
    assert freshness[0] is not None
    assert freshness[1] is not None

    conn.close()
