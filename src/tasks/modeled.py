import logging

logger = logging.getLogger(__name__)


def build_dim_loans(conn):
    conn.execute("""
        CREATE OR REPLACE TABLE dim_loans AS
        SELECT
            loan_id,
            customer_id,
            product_type,
            principal_amount,
            interest_rate,
            term_months,
            origination_date,
            origination_channel,
            status,
            credit_score,
            employment,
            annual_income,
            years_employed
        FROM stg_loans
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_loans").fetchone()[0]
    return {"row_count": count}


def build_dim_customers(conn):
    conn.execute("""
        CREATE OR REPLACE TABLE dim_customers AS
        SELECT
            customer_id,
            credit_score,
            employment,
            annual_income,
            years_employed
        FROM stg_loans
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY origination_date DESC
        ) = 1
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    return {"row_count": count}


def build_dim_dates(conn):
    conn.execute("""
        CREATE OR REPLACE TABLE dim_dates AS
        WITH bounds AS (
            SELECT
                LEAST(
                    (SELECT MIN(origination_date) FROM stg_loans),
                    (SELECT MIN(CAST(timestamp_utc AS DATE)) FROM stg_payments)
                ) AS min_date,
                GREATEST(
                    (SELECT MAX(origination_date) FROM stg_loans),
                    (SELECT MAX(CAST(timestamp_utc AS DATE)) FROM stg_payments)
                ) AS max_date
        ),
        date_range AS (
            SELECT UNNEST(generate_series(
                (SELECT min_date FROM bounds),
                (SELECT max_date FROM bounds),
                INTERVAL 1 DAY
            )) AS date
        )
        SELECT
            CAST(strftime(CAST(date AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
            CAST(date AS DATE) AS date,
            EXTRACT(YEAR FROM date)::INTEGER AS year,
            EXTRACT(QUARTER FROM date)::INTEGER AS quarter,
            EXTRACT(MONTH FROM date)::INTEGER AS month,
            EXTRACT(DAY FROM date)::INTEGER AS day,
            EXTRACT(DOW FROM date)::INTEGER AS day_of_week
        FROM date_range
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_dates").fetchone()[0]
    return {"row_count": count}


def build_fct_payments(conn):
    conn.execute("""
        CREATE OR REPLACE TABLE fct_payments AS
        SELECT
            p.payment_id,
            p.loan_id,
            l.customer_id,
            CAST(strftime(CAST(p.timestamp_utc AS DATE), '%Y%m%d') AS INTEGER) AS payment_date_key,
            p.amount,
            p.payment_method_type,
            p.payment_last_four,
            p.payment_bank,
            p.source,
            p.user_agent,
            p.timestamp_utc
        FROM stg_payments p
        LEFT JOIN dim_loans l ON p.loan_id = l.loan_id
    """)
    count = conn.execute("SELECT COUNT(*) FROM fct_payments").fetchone()[0]
    orphans = conn.execute(
        "SELECT COUNT(*) FROM fct_payments WHERE customer_id IS NULL"
    ).fetchone()[0]
    logger.info(f"fct_payments: {count} rows, {orphans} orphan payments")
    return {"row_count": count, "orphan_payments": orphans}


def build_rpt_delinquency(conn):
    conn.execute("""
        CREATE OR REPLACE TABLE rpt_delinquency AS
        WITH payment_gaps AS (
            SELECT
                f.loan_id,
                l.product_type,
                l.term_months,
                f.timestamp_utc,
                LAG(f.timestamp_utc) OVER (
                    PARTITION BY f.loan_id ORDER BY f.timestamp_utc
                ) AS prev_payment,
                DATEDIFF('day',
                    LAG(f.timestamp_utc) OVER (
                        PARTITION BY f.loan_id ORDER BY f.timestamp_utc
                    ),
                    f.timestamp_utc
                ) AS days_since_prev
            FROM fct_payments f
            JOIN dim_loans l ON f.loan_id = l.loan_id
            WHERE l.status IN ('active', 'default', 'charged_off')
        ),
        loan_delinquency AS (
            SELECT
                loan_id,
                product_type,
                MAX(CASE WHEN days_since_prev > 60 THEN 1 ELSE 0 END) AS is_delinquent_30
            FROM payment_gaps
            WHERE prev_payment IS NOT NULL
            GROUP BY loan_id, product_type
        )
        SELECT
            product_type,
            COUNT(*) AS total_loans,
            SUM(is_delinquent_30) AS delinquent_loans,
            ROUND(SUM(is_delinquent_30)::DOUBLE / COUNT(*) * 100, 2) AS delinquency_rate_pct
        FROM loan_delinquency
        GROUP BY product_type
        ORDER BY product_type
    """)
    count = conn.execute("SELECT COUNT(*) FROM rpt_delinquency").fetchone()[0]
    return {"row_count": count}
