import os
import duckdb
import pytest

from src.tasks.raw import load_raw_loans, load_raw_payments


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def test_raw_loans(conn, fixtures_dir):
    result = load_raw_loans(conn, data_dir=fixtures_dir)
    count = conn.execute("SELECT COUNT(*) FROM raw_loans").fetchone()[0]
    assert count == 15
    assert result["row_count"] == 15


def test_raw_payments(conn, fixtures_dir):
    result = load_raw_payments(conn, data_dir=fixtures_dir)
    count = conn.execute("SELECT COUNT(*) FROM raw_payments").fetchone()[0]
    assert count == 20
    assert result["row_count"] == 20
