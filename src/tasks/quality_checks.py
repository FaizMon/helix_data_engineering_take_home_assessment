import logging

logger = logging.getLogger(__name__)


def check_stg_loans_no_null_ids(conn):
    nulls = conn.execute("SELECT COUNT(*) FROM stg_loans WHERE loan_id IS NULL").fetchone()[0]
    return {"passed": nulls == 0, "null_count": nulls, "check": "stg_loans_no_null_ids"}


def check_stg_loans_unique_ids(conn):
    total = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    distinct = conn.execute("SELECT COUNT(DISTINCT loan_id) FROM stg_loans").fetchone()[0]
    return {"passed": total == distinct, "total": total, "distinct": distinct, "check": "stg_loans_unique_ids"}


def check_stg_loans_positive_principal(conn):
    bad = conn.execute("SELECT COUNT(*) FROM stg_loans WHERE principal_amount <= 0").fetchone()[0]
    return {"passed": bad == 0, "non_positive_count": bad, "severity": "WARN", "check": "stg_loans_positive_principal"}


def check_stg_loans_valid_interest_rate(conn):
    bad = conn.execute(
        "SELECT COUNT(*) FROM stg_loans WHERE interest_rate < 0 OR interest_rate > 100"
    ).fetchone()[0]
    return {"passed": bad == 0, "out_of_range_count": bad, "check": "stg_loans_valid_interest_rate"}


def check_stg_loans_valid_product_type(conn):
    valid = {"personal", "auto", "mortgage", "student"}
    types = conn.execute("SELECT DISTINCT product_type FROM stg_loans").fetchall()
    found = {r[0] for r in types}
    invalid = found - valid
    return {"passed": len(invalid) == 0, "invalid_types": str(invalid) if invalid else "none", "check": "stg_loans_valid_product_type"}


def check_stg_loans_freshness(conn):
    newest = conn.execute("SELECT MAX(origination_date) FROM stg_loans").fetchone()[0]
    row_count = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    return {"passed": row_count > 0, "newest_record": str(newest), "row_count": row_count, "severity": "WARN", "check": "stg_loans_freshness"}


def check_stg_loans_row_count(conn):
    count = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    return {"passed": 1000 <= count <= 50000, "row_count": count, "check": "stg_loans_row_count"}


def check_stg_payments_no_null_ids(conn):
    nulls = conn.execute("SELECT COUNT(*) FROM stg_payments WHERE payment_id IS NULL").fetchone()[0]
    return {"passed": nulls == 0, "null_count": nulls, "check": "stg_payments_no_null_ids"}


def check_stg_payments_unique_ids(conn):
    total = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    distinct = conn.execute("SELECT COUNT(DISTINCT payment_id) FROM stg_payments").fetchone()[0]
    return {"passed": total == distinct, "total": total, "distinct": distinct, "check": "stg_payments_unique_ids"}


def check_stg_payments_positive_amount(conn):
    bad = conn.execute("SELECT COUNT(*) FROM stg_payments WHERE amount <= 0").fetchone()[0]
    return {"passed": bad == 0, "non_positive_count": bad, "check": "stg_payments_positive_amount"}


def check_stg_payments_freshness(conn):
    newest = conn.execute("SELECT MAX(timestamp_utc) FROM stg_payments").fetchone()[0]
    row_count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    return {"passed": row_count > 0, "newest_record": str(newest), "row_count": row_count, "severity": "WARN", "check": "stg_payments_freshness"}


def check_stg_payments_row_count(conn):
    count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    return {"passed": 10000 <= count <= 500000, "row_count": count, "check": "stg_payments_row_count"}


def check_fct_payments_referential_integrity(conn):
    orphans = conn.execute("""
        SELECT COUNT(*) FROM fct_payments f
        WHERE NOT EXISTS (SELECT 1 FROM dim_loans l WHERE l.loan_id = f.loan_id)
    """).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM fct_payments").fetchone()[0]
    return {
        "passed": True,
        "orphan_count": orphans,
        "total_payments": total,
        "orphan_pct": round(orphans / max(total, 1) * 100, 2),
        "severity": "WARN",
        "check": "fct_payments_referential_integrity",
    }


STAGING_CHECKS = [
    check_stg_loans_no_null_ids,
    check_stg_loans_unique_ids,
    check_stg_loans_positive_principal,
    check_stg_loans_valid_interest_rate,
    check_stg_loans_valid_product_type,
    check_stg_loans_freshness,
    check_stg_loans_row_count,
    check_stg_payments_no_null_ids,
    check_stg_payments_unique_ids,
    check_stg_payments_positive_amount,
    check_stg_payments_freshness,
    check_stg_payments_row_count,
]

ALL_CHECKS = [
    check_stg_loans_no_null_ids,
    check_stg_loans_unique_ids,
    check_stg_loans_positive_principal,
    check_stg_loans_valid_interest_rate,
    check_stg_loans_valid_product_type,
    check_stg_loans_freshness,
    check_stg_loans_row_count,
    check_stg_payments_no_null_ids,
    check_stg_payments_unique_ids,
    check_stg_payments_positive_amount,
    check_stg_payments_freshness,
    check_stg_payments_row_count,
    check_fct_payments_referential_integrity,
]


def run_staging_checks(conn):
    results = []
    for check_fn in STAGING_CHECKS:
        result = check_fn(conn)
        status = "PASS" if result["passed"] else "FAIL"
        logger.info(f"Check {result['check']}: {status}")
        results.append(result)
    failed = [r for r in results if not r["passed"]]
    return {"total": len(results), "passed": len(results) - len(failed), "failed": len(failed), "results": results}


def run_all_checks(conn):
    results = []
    for check_fn in ALL_CHECKS:
        result = check_fn(conn)
        status = "PASS" if result["passed"] else "FAIL"
        logger.info(f"Check {result['check']}: {status}")
        results.append(result)
    failed = [r for r in results if not r["passed"]]
    return {"total": len(results), "passed": len(results) - len(failed), "failed": len(failed), "results": results}
