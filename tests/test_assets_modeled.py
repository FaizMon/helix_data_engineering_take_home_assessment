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
def test_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        yield db_path


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture
def staged_db(test_db, fixtures_dir, monkeypatch):
    """Materialize raw + staging so modeled tests can build on top."""
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=test_db)}
    result = materialize(
        [raw_loans, raw_payments, stg_loans, stg_payments],
        resources=resources,
    )
    assert result.success
    return test_db


def test_dim_loans(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize([dim_loans], resources=resources)
    assert result.success
    conn = duckdb.connect(staged_db)
    count = conn.execute("SELECT COUNT(*) FROM dim_loans").fetchone()[0]
    assert count == 14  # matches stg_loans after dedup
    conn.close()


def test_dim_customers(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize([dim_customers], resources=resources)
    assert result.success
    conn = duckdb.connect(staged_db)
    count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    assert count == 13  # 14 loans but C000001 appears in 2 loans → 13 unique customers
    # C000001's latest loan is L0000001 (2023-05-03, credit_score=672)
    row = conn.execute(
        "SELECT credit_score FROM dim_customers WHERE customer_id = 'C000001'"
    ).fetchone()
    assert row[0] == 672  # from L0000001's borrower_info (most recent origination_date)
    conn.close()


def test_dim_dates(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize([dim_dates], resources=resources)
    assert result.success
    conn = duckdb.connect(staged_db)
    count = conn.execute("SELECT COUNT(*) FROM dim_dates").fetchone()[0]
    assert count > 0
    # Verify structure
    row = conn.execute(
        "SELECT date_key, year, quarter, month, day FROM dim_dates LIMIT 1"
    ).fetchone()
    assert row is not None
    conn.close()


def test_fct_payments(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize([dim_loans, dim_dates, fct_payments], resources=resources)
    assert result.success
    conn = duckdb.connect(staged_db)
    count = conn.execute("SELECT COUNT(*) FROM fct_payments").fetchone()[0]
    assert count == 20
    # Orphan payment (L9999999) should have NULL customer_id
    orphan = conn.execute(
        "SELECT customer_id FROM fct_payments WHERE loan_id = 'L9999999'"
    ).fetchone()
    assert orphan[0] is None
    conn.close()


def test_rpt_delinquency(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize(
        [dim_loans, dim_dates, fct_payments, rpt_delinquency],
        resources=resources,
    )
    assert result.success
    conn = duckdb.connect(staged_db)
    rows = conn.execute("SELECT * FROM rpt_delinquency").fetchall()
    assert len(rows) > 0
    # Verify columns exist
    cols = [desc[0] for desc in conn.description]
    assert "product_type" in cols
    assert "delinquency_rate_pct" in cols
    conn.close()
