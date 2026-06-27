import os
import sys
import logging
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import duckdb
from airflow.decorators import dag, task

from src.tasks.raw import load_raw_loans, load_raw_payments
from src.tasks.staging import transform_stg_loans, transform_stg_payments
from src.tasks.modeled import (
    build_dim_loans,
    build_dim_customers,
    build_dim_dates,
    build_fct_payments,
    build_rpt_delinquency,
)
from src.tasks.quality_checks import run_staging_checks, check_fct_payments_referential_integrity

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("HELIX_DB_PATH", "output/helix.duckdb")
DATA_DIR = os.environ.get("HELIX_DATA_DIR", "data")


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    return duckdb.connect(DB_PATH)


@dag(
    dag_id="helix_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["helix", "data-pipeline"],
)
def helix_pipeline():

    @task()
    def ingest_loans():
        conn = get_conn()
        result = load_raw_loans(conn, data_dir=DATA_DIR)
        conn.close()
        return result

    @task()
    def ingest_payments(raw_loans_result):
        conn = get_conn()
        result = load_raw_payments(conn, data_dir=DATA_DIR)
        conn.close()
        return result

    @task()
    def stage_loans(raw_payments_result):
        conn = get_conn()
        result = transform_stg_loans(conn)
        conn.close()
        return result

    @task()
    def stage_payments(stg_loans_result):
        conn = get_conn()
        result = transform_stg_payments(conn)
        conn.close()
        return result

    @task()
    def quality_checks(stg_loans_result, stg_payments_result):
        conn = get_conn()
        result = run_staging_checks(conn)
        conn.close()
        failed = [r for r in result["results"] if not r["passed"] and r.get("severity") != "WARN"]
        if failed:
            raise RuntimeError(f"Quality checks failed: {[r['check'] for r in failed]}")
        return result

    @task()
    def build_dimensions(qc_result):
        conn = get_conn()
        loans = build_dim_loans(conn)
        customers = build_dim_customers(conn)
        dates = build_dim_dates(conn)
        conn.close()
        return {"dim_loans": loans, "dim_customers": customers, "dim_dates": dates}

    @task()
    def build_facts(dim_result):
        conn = get_conn()
        result = build_fct_payments(conn)
        conn.close()
        return result

    @task()
    def build_reports(fct_result):
        conn = get_conn()
        result = build_rpt_delinquency(conn)
        conn.close()
        return result

    @task()
    def final_quality_check(rpt_result):
        conn = get_conn()
        from src.tasks.quality_checks import check_fct_payments_referential_integrity
        result = check_fct_payments_referential_integrity(conn)
        conn.close()
        logger.info(f"Referential integrity: {result['orphan_count']} orphan payments "
                     f"({result['orphan_pct']}%)")
        return result

    raw_loans = ingest_loans()
    raw_payments = ingest_payments(raw_loans)

    stg_loans = stage_loans(raw_payments)
    stg_payments = stage_payments(stg_loans)

    qc = quality_checks(stg_loans, stg_payments)

    dims = build_dimensions(qc)
    facts = build_facts(dims)
    reports = build_reports(facts)
    final_quality_check(reports)


helix_pipeline()
