import os
import tempfile
import duckdb
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments
from src.assets.staging import stg_loans, stg_payments


@pytest.fixture
def test_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        yield db_path


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def test_stg_loans(test_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=test_db)}
    result = materialize([raw_loans, stg_loans], resources=resources)
    assert result.success

    conn = duckdb.connect(test_db)
    count = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    assert count == 14  # 15 raw rows - 1 duplicate

    # Verify cleaning
    row = conn.execute(
        "SELECT product_type, principal_amount, origination_date FROM stg_loans WHERE loan_id = 'L0000004'"
    ).fetchone()
    assert row[0] == "auto"  # was AUTO
    assert row[1] == 33517.74  # was "$33,517.74"
    assert str(row[2]) == "2020-08-19"  # was 08/19/2020

    # Verify borrower_info extraction
    row = conn.execute(
        "SELECT credit_score, employment FROM stg_loans WHERE loan_id = 'L0000001'"
    ).fetchone()
    assert row[0] == 672
    assert row[1] == "unemployed"

    # Verify malformed JSON was fixed (trailing comma)
    row = conn.execute(
        "SELECT credit_score FROM stg_loans WHERE loan_id = 'L0000009'"
    ).fetchone()
    assert row[0] == 694

    # Verify missing brace was fixed
    row = conn.execute(
        "SELECT credit_score FROM stg_loans WHERE loan_id = 'L0000010'"
    ).fetchone()
    assert row[0] == 832

    conn.close()


def test_stg_payments(test_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=test_db)}
    result = materialize([raw_payments, stg_payments], resources=resources)
    assert result.success

    conn = duckdb.connect(test_db)
    count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    assert count == 20  # no duplicates in fixture

    # Verify string amount cast
    row = conn.execute(
        "SELECT amount FROM stg_payments WHERE payment_id = 'P000000007'"
    ).fetchone()
    assert row[0] == 712.83

    # Verify payment_method flattening
    row = conn.execute(
        "SELECT payment_method_type, payment_last_four, payment_bank FROM stg_payments WHERE payment_id = 'P000000001'"
    ).fetchone()
    assert row[0] == "card"
    assert row[1] == "3517"
    assert row[2] == "Union Mutual"

    # Verify missing metadata → NULLs
    row = conn.execute(
        "SELECT source, user_agent FROM stg_payments WHERE payment_id = 'P000000003'"
    ).fetchone()
    assert row[0] is None
    assert row[1] is None

    conn.close()
