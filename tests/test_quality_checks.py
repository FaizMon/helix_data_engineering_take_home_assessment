import os
import duckdb
import pytest

from src.tasks.raw import load_raw_loans, load_raw_payments
from src.tasks.staging import transform_stg_loans, transform_stg_payments
from src.tasks.modeled import build_dim_loans, build_dim_dates, build_fct_payments
from src.tasks.quality_checks import (
    check_stg_loans_no_null_ids,
    check_stg_loans_unique_ids,
    check_stg_loans_positive_principal,
    check_stg_loans_valid_product_type,
    check_stg_payments_no_null_ids,
    check_stg_payments_unique_ids,
    check_fct_payments_referential_integrity,
)


@pytest.fixture
def full_conn():
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    conn = duckdb.connect(":memory:")
    load_raw_loans(conn, data_dir=fixtures_dir)
    load_raw_payments(conn, data_dir=fixtures_dir)
    transform_stg_loans(conn)
    transform_stg_payments(conn)
    build_dim_loans(conn)
    build_dim_dates(conn)
    build_fct_payments(conn)
    yield conn
    conn.close()


def test_checks_pass_on_clean_fixture(full_conn):
    checks = [
        check_stg_loans_no_null_ids,
        check_stg_loans_unique_ids,
        check_stg_loans_positive_principal,
        check_stg_loans_valid_product_type,
        check_stg_payments_no_null_ids,
        check_stg_payments_unique_ids,
        check_fct_payments_referential_integrity,
    ]

    for check_fn in checks:
        result = check_fn(full_conn)
        assert result["passed"], f"Check {result['check']} failed: {result}"
