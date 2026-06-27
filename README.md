# Helix Lending Data Pipeline (Airflow Branch)

A production-quality data pipeline that ingests loan origination and payment data, cleans and transforms it into a star schema, and lands queryable analytical tables in DuckDB — orchestrated with Apache Airflow.

> **See also:** The `main` branch implements the same pipeline using Dagster.

## Quick Start

```bash
make setup    # Create venv, install dependencies, initialize Airflow DB
make test     # Run all tests (no Airflow required)
make run      # Execute the full pipeline via `airflow dags test`
make dev      # Launch Airflow standalone UI at http://localhost:8080
```

## Architecture

**Stack:** Apache Airflow (orchestration) + DuckDB (storage & query engine)

**Data Model:** Star schema with dimension tables (loans, customers, dates) and a fact table (payments), plus a pre-computed delinquency report.

### Why Airflow?
Industry-standard DAG orchestration with a mature scheduler, web UI, and broad ecosystem. The TaskFlow API provides a clean Python-native interface for defining task dependencies. The web UI shows DAG runs, task logs, and execution timelines.

### Why DuckDB?
Reads CSV and JSONL natively (no pandas needed). SQL is the natural language for star schema modeling. The single `.duckdb` file is immediately queryable. Zero-config embedded database.

### Design Pattern
Business logic lives in `src/tasks/*.py` as plain functions that accept a `duckdb.Connection` parameter. The DAG file (`dags/helix_pipeline.py`) wraps them with Airflow's `@task` decorator. This keeps all logic **testable without Airflow running** — tests call functions directly with an in-memory DuckDB connection.

## Pipeline DAG

```
Raw Layer          Staging Layer       Quality Checks    Modeled Layer
─────────          ─────────────       ──────────────    ─────────────
loans.csv    →     stg_loans      →   13 DQ checks  →   dim_loans
                                                    →   dim_customers
                                                    ↘
payments.jsonl →   stg_payments   →                 →   dim_dates
                                                    →   fct_payments   →   rpt_delinquency
```

## Project Structure

```
dags/
└── helix_pipeline.py          # Airflow DAG definition

src/
├── tasks/
│   ├── raw.py                 # load_raw_loans(), load_raw_payments()
│   ├── staging.py             # transform_stg_loans(), transform_stg_payments()
│   ├── modeled.py             # build_dim_*, build_fct_*, build_rpt_*
│   └── quality_checks.py      # 13 check functions + run_all_checks()
├── utils/
│   ├── cleaning.py            # parse_amount, parse_date, fix_borrower_json, normalize_product_type
│   └── logging.py             # structlog configuration
└── __init__.py

tests/
├── fixtures/                  # Sample CSV/JSONL with edge cases
├── test_assets_raw.py
├── test_assets_staging.py
├── test_assets_modeled.py
├── test_quality_checks.py
├── test_cleaning.py
└── test_e2e.py
```

## Data Quality Checks

| Check | Table | Category |
|-------|-------|----------|
| No null IDs | stg_loans, stg_payments | Completeness |
| Unique IDs | stg_loans, stg_payments | Uniqueness |
| Positive amounts | stg_loans, stg_payments | Range validity |
| Valid interest rate (0-100%) | stg_loans | Range validity |
| Valid product types | stg_loans | Validity |
| Referential integrity | fct_payments → dim_loans | Referential |
| Source freshness | stg_loans, stg_payments | Freshness |
| Row count bounds | stg_loans, stg_payments | Completeness |

## Observability

- **Airflow UI** (`make dev`): DAG graph view, task logs, execution timelines, run history
- **Structured logging**: JSON-formatted logs via structlog with row counts, durations, rejections
- **Task return values**: every task returns metadata (rows_in, rows_out, rows_rejected) via XCom
- **Lineage documentation**: see `docs/LINEAGE.md`

## Business Queries

After running the pipeline, query `output/helix.duckdb` directly:

```sql
-- 30-day delinquency rate by product
SELECT * FROM rpt_delinquency;

-- Payments inconsistent with loan terms (orphan payments)
SELECT * FROM fct_payments WHERE customer_id IS NULL;

-- Data freshness
SELECT 'loans' AS source, MAX(origination_date) AS newest FROM dim_loans
UNION ALL
SELECT 'payments', MAX(timestamp_utc)::DATE FROM fct_payments;
```

## Dependencies

| Package | Why |
|---------|-----|
| apache-airflow | DAG orchestration framework |
| duckdb | Embedded analytical database — reads CSV/JSONL natively |
| structlog | JSON-structured logging |
| pytest | Test framework |

## Known Limitations

- **Delinquency logic is simplified**: uses consecutive payment gaps > 60 days as proxy for 30-day delinquency. A production system would compare against the contractual payment schedule.
- **No incremental loads**: full refresh on every run. Fine for ~85K total records; would need partitioning at scale.
- **Timezone handling**: naive timestamps (no timezone) are assumed UTC.
- **Borrower info is point-in-time**: credit scores and employment status from loan origination, not current.
- **No schema evolution**: pipeline assumes fixed schemas for both sources.
- **Single DuckDB connection**: Airflow tasks connect/disconnect sequentially since DuckDB allows one writer at a time. This is correct for embedded use.

## What I Would Do With More Time

- **Incremental loads**: Use Airflow's data-aware scheduling and DuckDB partitioning for incremental processing.
- **dbt integration**: Move SQL transformations to dbt models for better SQL testing and documentation.
- **Alerting**: Configure Airflow alerting callbacks for task failures and quality check violations.
- **CI/CD**: GitHub Actions pipeline running tests on every push.
- **Data contracts**: Formal schema validation on ingestion using JSON Schema.
- **Payment schedule modeling**: Build expected payment schedule from loan terms for precise delinquency calculation.
- **Connection pooling**: Use Airflow's connection management and hooks for DuckDB access in a multi-worker setup.
