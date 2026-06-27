import os
import logging

logger = logging.getLogger(__name__)


def load_raw_loans(conn, data_dir=None):
    csv_path = os.path.join(data_dir or os.environ.get("HELIX_DATA_DIR", "data"), "loans.csv")
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw_loans AS
        SELECT * FROM read_csv('{csv_path}', all_varchar=true, header=true)
    """)
    count = conn.execute("SELECT COUNT(*) FROM raw_loans").fetchone()[0]
    logger.info(f"Loaded {count} rows into raw_loans from {csv_path}")
    return {"row_count": count, "source": csv_path}


def load_raw_payments(conn, data_dir=None):
    jsonl_path = os.path.join(data_dir or os.environ.get("HELIX_DATA_DIR", "data"), "payments.jsonl")
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
    logger.info(f"Loaded {count} rows into raw_payments from {jsonl_path}")
    return {"row_count": count, "source": jsonl_path}
