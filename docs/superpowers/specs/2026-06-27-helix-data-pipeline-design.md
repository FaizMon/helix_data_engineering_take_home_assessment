# Helix Lending Data Pipeline — Design Spec

## Context

Helix Lending needs a production-quality data pipeline to ingest two messy sources (loans.csv, payments.jsonl), clean and transform them, and land modeled, queryable data. The business wants to answer:

1. What is our 30-day delinquency rate by loan product?
2. Which customers have payments inconsistent with their loan terms?
3. What is the data freshness and completeness for each source?

## Architecture

**Stack:** Dagster (orchestration + observability) + DuckDB (query engine + output storage)

**Why Dagster:** Asset-based framework maps 1:1 to data artifacts. Built-in asset checks cover all required DQ checks. Web UI provides run history, asset lineage, and check results — observability with no extra code. Sensibly scoped for this data size while being production-realistic.

**Why DuckDB:** Reads CSV/JSONL natively (no pandas needed). SQL is the natural language for star schema modeling. Single `.duckdb` file is immediately queryable. Zero-config embedded database.

**Why Star Schema:** The three business questions all involve joining loan attributes with payment events and aggregating by dimensions (product type, customer, time). A star schema with dimension and fact tables is the canonical model for this. One-Big-Table would work at this scale but obscures the domain model; normalized + views adds complexity without benefit.

## Data Sources

### loans.csv (10,030 records)

| Column | Raw Type | Issues |
|--------|----------|--------|
| loan_id | string (L0009053) | PK, expected unique |
| customer_id | string (C004089) | FK to customer |
| product_type | string | Mixed case: PERSONAL/personal/Personal |
| principal_amount | string/number | Some have $ and commas: "$33,517.74" |
| interest_rate | number | Annual %, range ~3-24% |
| term_months | integer | 12, 36, 48, 60, 72, 84, 120, 180, 240, 360 |
| origination_date | string | 3 formats: YYYY-MM-DD, DD-Mon-YYYY, MM/DD/YYYY |
| origination_channel | string | partner, branch, online, broker |
| status | string | active, closed, default, charged_off |
| borrower_info | JSON string | Embedded JSON with credit_score, employment, annual_income, years_employed. Sometimes malformed (trailing commas, missing braces) |

### payments.jsonl (74,786 records)

| Field | Type | Issues |
|-------|------|--------|
| payment_id | string (P000027450) | PK, expected unique |
| loan_id | string | FK to loans |
| amount | number or string | Some amounts stored as strings |
| timestamp | string | ISO-8601, mixed timezones: Z, -05:00, -08:00, or missing |
| payment_method | object | Nested: {type, details: {last_four, bank}} |
| metadata | object (optional) | Optional: {source, user_agent}. Missing on some records |

## DAG (Asset Graph)

```
Raw Layer
├── raw_loans         ← CSV into DuckDB raw table
└── raw_payments      ← JSONL into DuckDB raw table

Staging Layer
├── stg_loans         ← Cleaned, typed, normalized
└── stg_payments      ← Cleaned, UTC timestamps, flattened

Modeled Layer
├── dim_loans         ← Loan dimension
├── dim_customers     ← Customer dimension (from borrower_info)
├── dim_dates         ← Date dimension (generated)
├── fct_payments      ← Payment fact table
└── rpt_delinquency   ← 30-day delinquency by product
```

## Cleaning Rules

### loans.csv → stg_loans

- `product_type`: lowercase (PERSONAL → personal)
- `principal_amount`: strip $, strip commas, cast to DOUBLE
- `origination_date`: parse all 3 formats → DATE
- `borrower_info`: fix trailing commas and missing braces, parse JSON, extract credit_score (INT), employment (VARCHAR), annual_income (DOUBLE), years_employed (INT)
- `interest_rate`: cast to DOUBLE
- Deduplicate on `loan_id` (keep first occurrence)
- Reject rows where loan_id is null

### payments.jsonl → stg_payments

- `amount`: cast to DOUBLE
- `timestamp`: parse to TIMESTAMP WITH TIME ZONE, normalize to UTC. Naive timestamps treated as UTC
- `payment_method`: flatten to payment_method_type, payment_last_four, payment_bank
- `metadata`: flatten to source, user_agent (NULL if metadata absent)
- Deduplicate on `payment_id`
- Reject rows where payment_id is null

## Star Schema

### dim_loans
| Column | Type | Source |
|--------|------|--------|
| loan_id (PK) | VARCHAR | stg_loans |
| customer_id | VARCHAR | stg_loans |
| product_type | VARCHAR | stg_loans |
| principal_amount | DOUBLE | stg_loans |
| interest_rate | DOUBLE | stg_loans |
| term_months | INTEGER | stg_loans |
| origination_date | DATE | stg_loans |
| origination_channel | VARCHAR | stg_loans |
| status | VARCHAR | stg_loans |
| credit_score | INTEGER | stg_loans.borrower_info |
| employment | VARCHAR | stg_loans.borrower_info |
| annual_income | DOUBLE | stg_loans.borrower_info |
| years_employed | INTEGER | stg_loans.borrower_info |

### dim_customers
| Column | Type | Notes |
|--------|------|-------|
| customer_id (PK) | VARCHAR | Deduplicated |
| credit_score | INTEGER | From latest loan's borrower_info |
| employment | VARCHAR | From latest loan |
| annual_income | DOUBLE | From latest loan |
| years_employed | INTEGER | From latest loan |

### dim_dates
| Column | Type | Notes |
|--------|------|-------|
| date_key (PK) | INTEGER | YYYYMMDD format |
| date | DATE | |
| year | INTEGER | |
| quarter | INTEGER | 1-4 |
| month | INTEGER | 1-12 |
| day | INTEGER | 1-31 |
| day_of_week | INTEGER | 0=Mon, 6=Sun |

Generated from min(origination_date, payment_date) to max(origination_date, payment_date).

### fct_payments
| Column | Type | Source |
|--------|------|--------|
| payment_id (PK) | VARCHAR | stg_payments |
| loan_id (FK) | VARCHAR | → dim_loans |
| customer_id (FK) | VARCHAR | → dim_customers (via dim_loans) |
| payment_date_key (FK) | INTEGER | → dim_dates |
| amount | DOUBLE | stg_payments |
| payment_method_type | VARCHAR | stg_payments |
| payment_last_four | VARCHAR | stg_payments |
| payment_bank | VARCHAR | stg_payments |
| source | VARCHAR | stg_payments.metadata |
| user_agent | VARCHAR | stg_payments.metadata |
| timestamp_utc | TIMESTAMP | stg_payments |

### rpt_delinquency
Pre-computed: for each loan, determine if any payment gap exceeds 30 days relative to the expected payment schedule (derived from term_months). Aggregate delinquency rate by product_type.

## Data Quality Checks

All implemented as Dagster `@asset_check` decorators:

| Check | Asset | Category |
|-------|-------|----------|
| loan_id not null | stg_loans | Completeness |
| loan_id unique | stg_loans | Uniqueness |
| principal_amount > 0 | stg_loans | Range validity |
| interest_rate 0-100 | stg_loans | Range validity |
| product_type in valid set | stg_loans | Validity |
| payment_id not null | stg_payments | Completeness |
| payment_id unique | stg_payments | Uniqueness |
| amount > 0 | stg_payments | Range validity |
| All fct_payments.loan_id in dim_loans | fct_payments | Referential integrity |
| No orphan payments | fct_payments | Referential integrity |
| Source freshness (newest record age) | stg_loans, stg_payments | Freshness |
| Row count in expected range | stg_loans, stg_payments | Completeness |

## Observability

- **Structured logging:** `structlog` with JSON output. Each asset logs rows_in, rows_out, rows_rejected, duration, and any cleaning actions taken
- **Dagster metadata:** Asset materializations annotated with row counts, schema info, data quality results
- **Dagster UI:** `dagster dev` provides web-based run history, asset lineage graph, check results dashboard
- **Lineage doc:** `docs/LINEAGE.md` documents data flow from source to output for offline reference

## Project Structure

```
src/
├── __init__.py
├── definitions.py              # Dagster Definitions entry point
├── assets/
│   ├── __init__.py
│   ├── raw.py                  # raw_loans, raw_payments
│   ├── staging.py              # stg_loans, stg_payments
│   └── modeled.py              # dim_*, fct_*, rpt_*
├── checks/
│   ├── __init__.py
│   └── quality_checks.py       # @asset_check definitions
├── resources/
│   ├── __init__.py
│   └── duckdb.py               # DuckDB resource
└── utils/
    ├── __init__.py
    ├── cleaning.py             # Parse/clean functions
    └── logging.py              # structlog config

tests/
├── __init__.py
├── fixtures/
│   ├── loans_sample.csv
│   └── payments_sample.jsonl
├── test_cleaning.py            # Unit tests for cleaning functions
├── test_assets.py              # Asset tests with fixtures
└── test_e2e.py                 # Full pipeline E2E test

output/
└── helix.duckdb                # Final star schema database
```

## Testing Strategy

- **Unit tests (test_cleaning.py):** Test each cleaning function in isolation — date parsing (all 3 formats + malformed), amount parsing ($ + commas + plain), JSON fixing (trailing commas, missing braces), case normalization
- **Asset tests (test_assets.py):** Use Dagster test utilities to materialize individual assets against fixture data, verify schema and row counts
- **E2E test (test_e2e.py):** Materialize the full pipeline against small fixture files (~20 loans, ~50 payments covering all edge cases), query the output DuckDB to verify star schema integrity and that the delinquency query returns expected results
- **Framework:** pytest

## Dependencies

| Package | Justification |
|---------|---------------|
| dagster | Asset-based orchestration framework |
| dagster-webserver | Web UI for observability |
| dagster-duckdb | Native DuckDB integration for Dagster |
| duckdb | Embedded analytical database, reads CSV/JSONL natively |
| structlog | JSON-structured logging |
| pytest | Test framework |

Six packages total. No pandas — DuckDB handles all data manipulation via SQL.

## Verification Plan

1. `make setup` — install deps in venv
2. `make test` — run pytest (unit + asset + E2E)
3. `make run` — execute full pipeline via Dagster
4. `dagster dev` — launch UI, inspect asset graph, check results
5. Query output/helix.duckdb directly to verify business questions are answerable:
   - `SELECT product_type, delinquency_rate FROM rpt_delinquency`
   - `SELECT * FROM fct_payments WHERE loan_id NOT IN (SELECT loan_id FROM dim_loans)` → should be empty
   - `SELECT COUNT(*), MIN(timestamp_utc), MAX(timestamp_utc) FROM fct_payments` → freshness check
