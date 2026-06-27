# Data Lineage

## Source → Output Flow

```
data/loans.csv
  → raw_loans (DuckDB, all columns as VARCHAR)
  → stg_loans (cleaned: normalized types, parsed dates, extracted borrower_info JSON)
  → dim_loans (loan dimension)
  → dim_customers (customer dimension, deduplicated by latest loan)

data/payments.jsonl
  → raw_payments (DuckDB, nested JSON preserved)
  → stg_payments (cleaned: UTC timestamps, flattened payment_method/metadata)
  → fct_payments (fact table, joined to dim_loans for customer_id)

stg_loans + stg_payments
  → dim_dates (generated date dimension spanning full date range)

fct_payments + dim_loans
  → rpt_delinquency (30-day delinquency rate by product type)
```

## Transformations by Layer

### Raw → Staging (stg_loans)
- product_type: lowercased
- principal_amount: stripped $ and commas, cast to DOUBLE
- origination_date: parsed from 3 formats to DATE
- borrower_info: JSON fixed (trailing commas, missing braces), parsed, columns extracted
- Deduplicated on loan_id (first occurrence kept)

### Raw → Staging (stg_payments)
- amount: cast string amounts to DOUBLE
- timestamp: normalized to UTC (naive timestamps assumed UTC)
- payment_method: flattened to type, last_four, bank columns
- metadata: flattened to source, user_agent (NULL if absent)
- Deduplicated on payment_id

### Staging → Modeled
- dim_loans: direct projection from stg_loans
- dim_customers: deduplicated by customer_id, keeping latest loan's borrower attributes
- dim_dates: generated daily from min to max date across both sources
- fct_payments: LEFT JOIN to dim_loans for customer_id, date_key computed from timestamp
- rpt_delinquency: consecutive payment gap analysis, aggregated by product_type
