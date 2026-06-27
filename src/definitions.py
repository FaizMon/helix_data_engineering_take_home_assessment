from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource

from src.assets import raw, staging, modeled
from src.checks.quality_checks import (
    stg_loans_no_null_ids,
    stg_loans_unique_ids,
    stg_loans_positive_principal,
    stg_loans_valid_interest_rate,
    stg_loans_valid_product_type,
    stg_loans_freshness,
    stg_loans_row_count,
    stg_payments_no_null_ids,
    stg_payments_unique_ids,
    stg_payments_positive_amount,
    stg_payments_freshness,
    stg_payments_row_count,
    fct_payments_referential_integrity,
)
from src.utils.logging import configure_logging

configure_logging()

all_assets = load_assets_from_modules([raw, staging, modeled])

all_checks = [
    stg_loans_no_null_ids,
    stg_loans_unique_ids,
    stg_loans_positive_principal,
    stg_loans_valid_interest_rate,
    stg_loans_valid_product_type,
    stg_loans_freshness,
    stg_loans_row_count,
    stg_payments_no_null_ids,
    stg_payments_unique_ids,
    stg_payments_positive_amount,
    stg_payments_freshness,
    stg_payments_row_count,
    fct_payments_referential_integrity,
]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "duckdb": DuckDBResource(database="output/helix.duckdb"),
    },
)
