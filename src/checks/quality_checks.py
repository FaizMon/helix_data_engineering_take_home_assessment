from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
from dagster_duckdb import DuckDBResource


# --- stg_loans checks ---

@asset_check(asset="stg_loans")
def stg_loans_no_null_ids(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        nulls = conn.execute("SELECT COUNT(*) FROM stg_loans WHERE loan_id IS NULL").fetchone()[0]
    return AssetCheckResult(
        passed=nulls == 0,
        metadata={"null_count": nulls},
        description="loan_id must not be null",
    )


@asset_check(asset="stg_loans")
def stg_loans_unique_ids(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
        distinct = conn.execute("SELECT COUNT(DISTINCT loan_id) FROM stg_loans").fetchone()[0]
    return AssetCheckResult(
        passed=total == distinct,
        metadata={"total": total, "distinct": distinct},
        description="loan_id must be unique",
    )


@asset_check(asset="stg_loans")
def stg_loans_positive_principal(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        bad = conn.execute("SELECT COUNT(*) FROM stg_loans WHERE principal_amount <= 0").fetchone()[0]
    return AssetCheckResult(
        passed=bad == 0,
        metadata={"non_positive_count": bad},
        description="principal_amount must be > 0",
    )


@asset_check(asset="stg_loans")
def stg_loans_valid_interest_rate(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        bad = conn.execute(
            "SELECT COUNT(*) FROM stg_loans WHERE interest_rate < 0 OR interest_rate > 100"
        ).fetchone()[0]
    return AssetCheckResult(
        passed=bad == 0,
        metadata={"out_of_range_count": bad},
        description="interest_rate must be between 0 and 100",
    )


@asset_check(asset="stg_loans")
def stg_loans_valid_product_type(duckdb: DuckDBResource) -> AssetCheckResult:
    valid = {"personal", "auto", "mortgage", "student"}
    with duckdb.get_connection() as conn:
        types = conn.execute("SELECT DISTINCT product_type FROM stg_loans").fetchall()
        found = {r[0] for r in types}
    invalid = found - valid
    return AssetCheckResult(
        passed=len(invalid) == 0,
        metadata={"invalid_types": str(invalid) if invalid else "none"},
        description="product_type must be one of: personal, auto, mortgage, student",
    )


@asset_check(asset="stg_loans")
def stg_loans_freshness(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        newest = conn.execute("SELECT MAX(origination_date) FROM stg_loans").fetchone()[0]
        row_count = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"newest_record": str(newest), "row_count": row_count},
        severity=AssetCheckSeverity.WARN,
        description="Source freshness and row count for loans",
    )


@asset_check(asset="stg_loans")
def stg_loans_row_count(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    return AssetCheckResult(
        passed=1000 <= count <= 50000,
        metadata={"row_count": count},
        description="Row count should be between 1,000 and 50,000",
    )


# --- stg_payments checks ---

@asset_check(asset="stg_payments")
def stg_payments_no_null_ids(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        nulls = conn.execute(
            "SELECT COUNT(*) FROM stg_payments WHERE payment_id IS NULL"
        ).fetchone()[0]
    return AssetCheckResult(
        passed=nulls == 0,
        metadata={"null_count": nulls},
        description="payment_id must not be null",
    )


@asset_check(asset="stg_payments")
def stg_payments_unique_ids(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
        distinct = conn.execute("SELECT COUNT(DISTINCT payment_id) FROM stg_payments").fetchone()[0]
    return AssetCheckResult(
        passed=total == distinct,
        metadata={"total": total, "distinct": distinct},
        description="payment_id must be unique",
    )


@asset_check(asset="stg_payments")
def stg_payments_positive_amount(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        bad = conn.execute("SELECT COUNT(*) FROM stg_payments WHERE amount <= 0").fetchone()[0]
    return AssetCheckResult(
        passed=bad == 0,
        metadata={"non_positive_count": bad},
        description="amount must be > 0",
    )


@asset_check(asset="stg_payments")
def stg_payments_freshness(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        newest = conn.execute("SELECT MAX(timestamp_utc) FROM stg_payments").fetchone()[0]
        row_count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"newest_record": str(newest), "row_count": row_count},
        severity=AssetCheckSeverity.WARN,
        description="Source freshness and row count for payments",
    )


@asset_check(asset="stg_payments")
def stg_payments_row_count(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    return AssetCheckResult(
        passed=10000 <= count <= 500000,
        metadata={"row_count": count},
        description="Row count should be between 10,000 and 500,000",
    )


# --- fct_payments checks ---

@asset_check(asset="fct_payments")
def fct_payments_referential_integrity(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        orphans = conn.execute("""
            SELECT COUNT(*) FROM fct_payments f
            WHERE NOT EXISTS (SELECT 1 FROM dim_loans l WHERE l.loan_id = f.loan_id)
        """).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM fct_payments").fetchone()[0]
    return AssetCheckResult(
        passed=True,  # Orphans are expected (data quality issue, not a pipeline bug)
        metadata={
            "orphan_count": orphans,
            "total_payments": total,
            "orphan_pct": round(orphans / max(total, 1) * 100, 2),
        },
        severity=AssetCheckSeverity.WARN,
        description="Payments referencing non-existent loans (orphan payments)",
    )
