# Helix Lending Data Pipeline

A production-quality data pipeline that ingests loan origination and payment data, cleans and transforms it into a star schema, and lands queryable analytical tables in DuckDB.

## Quick Start

```bash
make setup    # Create venv and install dependencies
make test     # Run all tests
make run      # Execute the full pipeline
make dev      # Launch Dagster UI at http://localhost:3000
```

## Architecture

**Stack:** Dagster (orchestration) + DuckDB (storage & query engine)

**Data Model:** Star schema with dimension tables (loans, customers, dates) and a fact table (payments), plus a pre-computed delinquency report.

### Why Dagster?
Asset-based orchestration maps 1:1 to data artifacts. Built-in asset checks cover data quality. The web UI provides run history, asset lineage, and check results — production-grade observability with minimal code.

### Why DuckDB?
Reads CSV and JSONL natively (no pandas needed). SQL is the natural language for star schema modeling. The single `.duckdb` file is immediately queryable. Zero-config embedded database.

### Why Star Schema?
The business questions involve joining loan attributes with payment events and aggregating by dimensions (product type, customer, time). A star schema is the canonical model for this pattern. One-Big-Table would work at this scale but obscures the domain model.

## Pipeline DAG

```
Raw Layer          Staging Layer       Modeled Layer
─────────          ─────────────       ─────────────
loans.csv    →     stg_loans      →   dim_loans
                                  →   dim_customers
                                  ↘
payments.jsonl →   stg_payments   →   dim_dates
                                  →   fct_payments   →   rpt_delinquency
```

## Data Quality Checks

| Check | Asset | Category |
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

- **Dagster UI** (`make dev`): asset lineage graph, run history, check results dashboard
- **Structured logging**: JSON-formatted logs via structlog with row counts, durations, rejections
- **Asset metadata**: every materialization records rows_in, rows_out, rows_rejected
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
| dagster | Asset-based orchestration framework |
| dagster-webserver | Web UI for observability |
| dagster-duckdb | Native DuckDB resource for Dagster |
| duckdb | Embedded analytical database — reads CSV/JSONL natively |
| structlog | JSON-structured logging |
| pytest | Test framework |

## Known Limitations

- **Delinquency logic is simplified**: uses consecutive payment gaps > 60 days as proxy for 30-day delinquency. A production system would compare against the contractual payment schedule.
- **No incremental loads**: full refresh on every run. Fine for ~85K total records; would need partitioning at scale.
- **Timezone handling**: naive timestamps (no timezone) are assumed UTC.
- **Borrower info is point-in-time**: credit scores and employment status from loan origination, not current.
- **No schema evolution**: pipeline assumes fixed schemas for both sources.

## What I Would Do With More Time

- **Incremental materializations**: Dagster supports partitioned assets — partition payments by month for incremental processing.
- **dbt integration**: Move SQL transformations to dbt models for better SQL testing and documentation.
- **Schema contracts**: Add Dagster `TableSchema` definitions to enforce column-level contracts.
- **Alerting**: Configure Dagster sensors to alert on check failures.
- **CI/CD**: GitHub Actions pipeline running tests on every push.
- **Data contracts**: Formal schema validation on ingestion using JSON Schema.
- **Payment schedule modeling**: Build expected payment schedule from loan terms for precise delinquency calculation.
