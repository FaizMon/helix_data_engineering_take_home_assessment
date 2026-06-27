import os
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource

from src.utils.cleaning import parse_amount, parse_date, fix_borrower_json, normalize_product_type


@asset(deps=["raw_loans"], group_name="staging")
def stg_loans(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        rows = conn.execute("SELECT * FROM raw_loans").fetchall()
        columns = [desc[0] for desc in conn.description]
        raw_count = len(rows)

        conn.execute("DROP TABLE IF EXISTS stg_loans")
        conn.execute("""
            CREATE TABLE stg_loans (
                loan_id VARCHAR PRIMARY KEY,
                customer_id VARCHAR,
                product_type VARCHAR,
                principal_amount DOUBLE,
                interest_rate DOUBLE,
                term_months INTEGER,
                origination_date DATE,
                origination_channel VARCHAR,
                status VARCHAR,
                credit_score INTEGER,
                employment VARCHAR,
                annual_income DOUBLE,
                years_employed INTEGER
            )
        """)

        seen_ids = set()
        rejected = 0
        import json

        for row in rows:
            rec = dict(zip(columns, row))
            loan_id = rec["loan_id"]
            if not loan_id or loan_id in seen_ids:
                rejected += 1
                continue
            seen_ids.add(loan_id)

            try:
                amount = parse_amount(rec["principal_amount"])
                date_str = parse_date(rec["origination_date"])
                product = normalize_product_type(rec["product_type"])
                borrower_raw = fix_borrower_json(rec["borrower_info"])
                borrower = json.loads(borrower_raw)
            except (ValueError, json.JSONDecodeError) as e:
                context.log.warning(f"Rejecting loan {loan_id}: {e}")
                rejected += 1
                continue

            conn.execute("""
                INSERT INTO stg_loans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                loan_id,
                rec["customer_id"],
                product,
                amount,
                float(rec["interest_rate"]),
                int(rec["term_months"]),
                date_str,
                rec["origination_channel"],
                rec["status"],
                borrower.get("credit_score"),
                borrower.get("employment"),
                borrower.get("annual_income"),
                borrower.get("years_employed"),
            ])

        inserted = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]

    context.log.info(f"stg_loans: {raw_count} in, {inserted} out, {rejected} rejected")
    return MaterializeResult(
        metadata={
            "rows_in": MetadataValue.int(raw_count),
            "rows_out": MetadataValue.int(inserted),
            "rows_rejected": MetadataValue.int(rejected),
        }
    )


@asset(deps=["raw_payments"], group_name="staging")
def stg_payments(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        raw_count = conn.execute("SELECT COUNT(*) FROM raw_payments").fetchone()[0]

        conn.execute("DROP TABLE IF EXISTS stg_payments")
        conn.execute("""
            CREATE TABLE stg_payments AS
            SELECT
                payment_id,
                loan_id,
                CAST(amount AS DOUBLE) AS amount,
                -- Normalize timestamps: append UTC if no timezone info
                CASE
                    WHEN timestamp LIKE '%Z'
                         OR timestamp LIKE '%+%'
                         OR timestamp LIKE '%-__:__'
                    THEN CAST(timestamp AS TIMESTAMPTZ)
                    ELSE CAST(timestamp || '+00:00' AS TIMESTAMPTZ)
                END AT TIME ZONE 'UTC' AS timestamp_utc,
                json_extract_string(payment_method, '$.type') AS payment_method_type,
                json_extract_string(payment_method, '$.details.last_four') AS payment_last_four,
                json_extract_string(payment_method, '$.details.bank') AS payment_bank,
                json_extract_string(metadata, '$.source') AS source,
                json_extract_string(metadata, '$.user_agent') AS user_agent
            FROM raw_payments
            QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY timestamp) = 1
        """)

        inserted = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
        rejected = raw_count - inserted

    context.log.info(f"stg_payments: {raw_count} in, {inserted} out, {rejected} rejected")
    return MaterializeResult(
        metadata={
            "rows_in": MetadataValue.int(raw_count),
            "rows_out": MetadataValue.int(inserted),
            "rows_rejected": MetadataValue.int(rejected),
        }
    )
