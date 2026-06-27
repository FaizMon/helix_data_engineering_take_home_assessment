import os
import tempfile
import duckdb
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments


@pytest.fixture
def test_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        yield db_path


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def test_raw_loans_materializes(test_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    result = materialize(
        [raw_loans],
        resources={"duckdb": DuckDBResource(database=test_db)},
    )
    assert result.success
    conn = duckdb.connect(test_db)
    count = conn.execute("SELECT COUNT(*) FROM raw_loans").fetchone()[0]
    assert count == 15  # 15 rows in fixture (including duplicate)
    conn.close()


def test_raw_payments_materializes(test_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    result = materialize(
        [raw_payments],
        resources={"duckdb": DuckDBResource(database=test_db)},
    )
    assert result.success
    conn = duckdb.connect(test_db)
    count = conn.execute("SELECT COUNT(*) FROM raw_payments").fetchone()[0]
    assert count == 20
    conn.close()
