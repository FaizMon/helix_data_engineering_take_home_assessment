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


@pytest.fixture
def staged_conn(fixtures_dir):
    conn = duckdb.connect(":memory:")
    load_raw_loans(conn, data_dir=fixtures_dir)
    load_raw_payments(conn, data_dir=fixtures_dir)
    transform_stg_loans(conn)
    transform_stg_payments(conn)
    yield conn
    conn.close()


def test_dim_loans(staged_conn):
    build_dim_loans(staged_conn)
    count = staged_conn.execute("SELECT COUNT(*) FROM dim_loans").fetchone()[0]
    assert count == 14


def test_dim_customers(staged_conn):
    build_dim_customers(staged_conn)
    count = staged_conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    assert count == 13  # 14 loans but C000001 appears in 2 loans
    row = staged_conn.execute(
        "SELECT credit_score FROM dim_customers WHERE customer_id = 'C000001'"
    ).fetchone()
    assert row[0] == 672  # from L0000001 (most recent origination_date)


def test_dim_dates(staged_conn):
    build_dim_dates(staged_conn)
    count = staged_conn.execute("SELECT COUNT(*) FROM dim_dates").fetchone()[0]
    assert count > 0
    row = staged_conn.execute(
        "SELECT date_key, year, quarter, month, day FROM dim_dates LIMIT 1"
    ).fetchone()
    assert row is not None


def test_fct_payments(staged_conn):
    build_dim_loans(staged_conn)
    build_dim_dates(staged_conn)
    build_fct_payments(staged_conn)
    count = staged_conn.execute("SELECT COUNT(*) FROM fct_payments").fetchone()[0]
    assert count == 20
    orphan = staged_conn.execute(
        "SELECT customer_id FROM fct_payments WHERE loan_id = 'L9999999'"
    ).fetchone()
    assert orphan[0] is None


def test_rpt_delinquency(staged_conn):
    build_dim_loans(staged_conn)
    build_dim_dates(staged_conn)
    build_fct_payments(staged_conn)
    build_rpt_delinquency(staged_conn)
    rows = staged_conn.execute("SELECT * FROM rpt_delinquency").fetchall()
    assert len(rows) > 0
    cols = [desc[0] for desc in staged_conn.description]
    assert "product_type" in cols
    assert "delinquency_rate_pct" in cols
