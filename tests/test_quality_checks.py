import os
import tempfile
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments
from src.assets.staging import stg_loans, stg_payments
from src.assets.modeled import dim_loans, dim_dates, fct_payments
from src.checks.quality_checks import (
    stg_loans_no_null_ids,
    stg_loans_unique_ids,
    stg_loans_positive_principal,
    stg_loans_valid_product_type,
    stg_payments_no_null_ids,
    stg_payments_unique_ids,
    fct_payments_referential_integrity,
)


@pytest.fixture
def full_db(monkeypatch):
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        resources = {"duckdb": DuckDBResource(database=db_path)}
        result = materialize(
            [raw_loans, raw_payments, stg_loans, stg_payments, dim_loans, dim_dates, fct_payments],
            resources=resources,
        )
        assert result.success
        yield db_path


def test_checks_pass_on_clean_fixture(full_db):
    checks = [
        stg_loans_no_null_ids,
        stg_loans_unique_ids,
        stg_loans_positive_principal,
        stg_loans_valid_product_type,
        stg_payments_no_null_ids,
        stg_payments_unique_ids,
        fct_payments_referential_integrity,
    ]

    for check_fn in checks:
        result = check_fn(duckdb=DuckDBResource(database=full_db))
        assert result.passed, f"Check {check_fn.__name__} failed: {result.metadata}"
