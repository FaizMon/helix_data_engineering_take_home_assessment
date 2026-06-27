import os
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource


@asset(group_name="raw")
def raw_loans(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    csv_path = os.path.join(os.environ.get("HELIX_DATA_DIR", "data"), "loans.csv")
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw_loans AS
            SELECT * FROM read_csv('{csv_path}', all_varchar=true, header=true)
        """)
        count = conn.execute("SELECT COUNT(*) FROM raw_loans").fetchone()[0]
    context.log.info(f"Loaded {count} rows into raw_loans")
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(count),
            "source": MetadataValue.text(csv_path),
        }
    )


@asset(group_name="raw")
def raw_payments(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    jsonl_path = os.path.join(os.environ.get("HELIX_DATA_DIR", "data"), "payments.jsonl")
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw_payments AS
            SELECT * FROM read_json('{jsonl_path}',
                format='newline_delimited',
                columns={{
                    payment_id: 'VARCHAR',
                    loan_id: 'VARCHAR',
                    amount: 'VARCHAR',
                    timestamp: 'VARCHAR',
                    payment_method: 'JSON',
                    metadata: 'JSON'
                }}
            )
        """)
        count = conn.execute("SELECT COUNT(*) FROM raw_payments").fetchone()[0]
    context.log.info(f"Loaded {count} rows into raw_payments")
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(count),
            "source": MetadataValue.text(jsonl_path),
        }
    )
