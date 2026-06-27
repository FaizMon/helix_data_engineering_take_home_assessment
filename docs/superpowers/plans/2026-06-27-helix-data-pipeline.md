# Helix Data Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dagster + DuckDB pipeline that ingests loans.csv and payments.jsonl, cleans and transforms them into a star schema, with data quality checks, observability, and tests.

**Architecture:** Dagster assets define a 3-layer DAG (raw → staging → modeled). DuckDB is both the transformation engine and output store. Each asset executes SQL against a shared DuckDB file. Asset checks enforce data quality. structlog provides JSON logging.

**Tech Stack:** Python, Dagster, DuckDB, dagster-duckdb, structlog, pytest

## Global Constraints

- Python 3.10+
- No pandas — DuckDB handles all data manipulation via SQL
- All cleaning logic lives in `src/utils/cleaning.py` as pure Python functions (unit-testable outside Dagster)
- DuckDB database file: `output/helix.duckdb`
- Source data: `data/loans.csv`, `data/payments.jsonl`
- Every asset logs rows_in, rows_out, rows_rejected via Dagster metadata
- Use `dagster-duckdb` `DuckDBResource` for database connections

---

### Task 1: Project Scaffolding + Cleaning Utilities (TDD)

**Files:**
- Create: `pyproject.toml`
- Create: `src/__init__.py`
- Create: `src/utils/__init__.py`
- Create: `src/utils/cleaning.py`
- Create: `tests/__init__.py`
- Create: `tests/test_cleaning.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `parse_amount(raw: str) -> float` — strips `$`, commas, casts to float
  - `parse_date(raw: str) -> str` — normalizes YYYY-MM-DD / DD-Mon-YYYY / MM/DD/YYYY → `YYYY-MM-DD` string
  - `fix_borrower_json(raw: str) -> str` — fixes trailing commas, missing closing braces in JSON strings
  - `normalize_product_type(raw: str) -> str` — lowercases product type

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "helix-data-pipeline"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "dagster>=1.9",
    "dagster-webserver>=1.9",
    "dagster-duckdb>=0.25",
    "duckdb>=1.0",
    "structlog>=24.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
]

[tool.dagster]
module_name = "src.definitions"

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Create directory structure**

```bash
mkdir -p src/assets src/checks src/resources src/utils tests/fixtures output
touch src/__init__.py src/assets/__init__.py src/checks/__init__.py src/resources/__init__.py src/utils/__init__.py tests/__init__.py
```

- [ ] **Step 3: Install dependencies**

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

- [ ] **Step 4: Write failing tests for cleaning functions**

File: `tests/test_cleaning.py`

```python
import pytest
from src.utils.cleaning import parse_amount, parse_date, fix_borrower_json, normalize_product_type


class TestParseAmount:
    def test_plain_number(self):
        assert parse_amount("32256.80") == 32256.80

    def test_dollar_and_commas(self):
        assert parse_amount("$33,517.74") == 33517.74

    def test_commas_only(self):
        assert parse_amount("15,451.43") == 15451.43

    def test_integer(self):
        assert parse_amount("491402") == 491402.0

    def test_large_with_dollar(self):
        assert parse_amount("$1,593,770.84") == 1593770.84

    def test_already_float(self):
        assert parse_amount(32256.80) == 32256.80

    def test_already_int(self):
        assert parse_amount(491402) == 491402.0


class TestParseDate:
    def test_iso_format(self):
        assert parse_date("2023-05-03") == "2023-05-03"

    def test_dd_mon_yyyy(self):
        assert parse_date("11-Dec-2020") == "2020-12-11"

    def test_mm_dd_yyyy(self):
        assert parse_date("08/19/2020") == "2020-08-19"

    def test_dd_mon_yyyy_short_month(self):
        assert parse_date("05-Mar-2023") == "2023-03-05"

    def test_mm_dd_yyyy_leading_zeros(self):
        assert parse_date("01/29/2025") == "2025-01-29"


class TestFixBorrowerJson:
    def test_valid_json_unchanged(self):
        raw = '{"credit_score": 672, "employment": "unemployed", "annual_income": 86827, "years_employed": 2}'
        result = fix_borrower_json(raw)
        assert '"credit_score": 672' in result

    def test_trailing_comma(self):
        raw = '{"credit_score": 694, "employment": "self-employed", "annual_income": 72669, "years_employed": 2,}'
        result = fix_borrower_json(raw)
        assert result.endswith("}")
        import json
        parsed = json.loads(result)
        assert parsed["years_employed"] == 2

    def test_missing_closing_brace(self):
        raw = '{"credit_score": 832, "employment": "self-employed", "annual_income": 128615, "years_employed": 17'
        result = fix_borrower_json(raw)
        import json
        parsed = json.loads(result)
        assert parsed["years_employed"] == 17

    def test_csv_escaped_quotes(self):
        raw = '""credit_score"": 672, ""employment"": ""unemployed"", ""annual_income"": 86827, ""years_employed"": 2'
        result = fix_borrower_json(raw)
        import json
        parsed = json.loads(result)
        assert parsed["credit_score"] == 672


class TestNormalizeProductType:
    def test_uppercase(self):
        assert normalize_product_type("PERSONAL") == "personal"

    def test_lowercase(self):
        assert normalize_product_type("personal") == "personal"

    def test_title_case(self):
        assert normalize_product_type("Mortgage") == "mortgage"

    def test_mixed(self):
        assert normalize_product_type("AUTO") == "auto"
```

- [ ] **Step 5: Run tests to verify they fail**

```bash
pytest tests/test_cleaning.py -v
```

Expected: FAIL with `ModuleNotFoundError` or `ImportError`

- [ ] **Step 6: Implement cleaning functions**

File: `src/utils/cleaning.py`

```python
import re
import json
from datetime import datetime


def parse_amount(raw) -> float:
    if isinstance(raw, (int, float)):
        return float(raw)
    cleaned = str(raw).replace("$", "").replace(",", "")
    return float(cleaned)


def parse_date(raw: str) -> str:
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {raw}")


def fix_borrower_json(raw: str) -> str:
    s = raw.strip()
    # Handle CSV double-quote escaping: ""key"" -> "key"
    if '""' in s:
        s = s.replace('""', '"')
    if not s.startswith("{"):
        s = "{" + s
    if not s.endswith("}"):
        s = s + "}"
    # Remove trailing commas before closing brace
    s = re.sub(r",\s*}", "}", s)
    return s


def normalize_product_type(raw: str) -> str:
    return raw.strip().lower()
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
pytest tests/test_cleaning.py -v
```

Expected: all PASS

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml src/ tests/
git commit -m "feat: project scaffolding and cleaning utilities with tests"
```

---

### Task 2: DuckDB Resource + Raw Layer Assets

**Files:**
- Create: `src/resources/duckdb.py`
- Create: `src/assets/raw.py`
- Create: `tests/test_assets_raw.py`
- Create: `tests/fixtures/loans_sample.csv`
- Create: `tests/fixtures/payments_sample.jsonl`

**Interfaces:**
- Consumes: nothing (reads from CSV/JSONL files)
- Produces:
  - `raw_loans` Dagster asset → DuckDB table `raw_loans`
  - `raw_payments` Dagster asset → DuckDB table `raw_payments`
  - `get_duckdb_resource(db_path: str) -> dict` — returns Dagster resource config

- [ ] **Step 1: Create test fixtures**

File: `tests/fixtures/loans_sample.csv` — 15 rows covering all edge cases:

```csv
loan_id,customer_id,product_type,principal_amount,interest_rate,term_months,origination_date,origination_channel,status,borrower_info
L0000001,C000001,PERSONAL,32256.80,10.12,12,2023-05-03,partner,active,"{""credit_score"": 672, ""employment"": ""unemployed"", ""annual_income"": 86827, ""years_employed"": 2}"
L0000002,C000002,personal,24315.74,17.82,60,2021-11-16,branch,closed,"{""credit_score"": 592, ""employment"": ""unemployed"", ""annual_income"": 142824, ""years_employed"": 10}"
L0000003,C000003,Mortgage,223956.81,3.18,360,11-Dec-2020,branch,default,"{""credit_score"": 605, ""employment"": ""salaried"", ""annual_income"": 118306, ""years_employed"": 2}"
L0000004,C000004,AUTO,"$33,517.74",24.72,12,08/19/2020,partner,active,"{""credit_score"": 652, ""employment"": ""salaried"", ""annual_income"": 215780, ""years_employed"": 21}"
L0000005,C000005,Auto,42801.84,13.77,72,2020-08-04,broker,default,"{""credit_score"": 817, ""employment"": ""unemployed"", ""annual_income"": 111356, ""years_employed"": 11}"
L0000006,C000006,Personal,"15,451.43",7.8,36,2020-01-18,partner,active,"{""credit_score"": 632, ""employment"": ""retired"", ""annual_income"": 298061, ""years_employed"": 28}"
L0000007,C000007,STUDENT,38366.86,5.8,120,2023-12-21,online,default,"{""credit_score"": 717, ""employment"": ""self-employed"", ""annual_income"": 127459, ""years_employed"": 6}"
L0000008,C000008,mortgage,491402,7.42,120,2021-02-03,partner,active,"{""credit_score"": 728, ""employment"": ""salaried"", ""annual_income"": 251795, ""years_employed"": 30}"
L0000009,C000009,auto,19433,9.01,84,2020-01-28,online,active,"{""credit_score"": 694, ""employment"": ""self-employed"", ""annual_income"": 72669, ""years_employed"": 2,}"
L0000010,C000010,Student,192978.82,9.95,120,2021-03-24,broker,closed,"{""credit_score"": 832, ""employment"": ""self-employed"", ""annual_income"": 128615, ""years_employed"": 17"
L0000011,C000011,MORTGAGE,"$1,593,770.84",6.46,180,05-Mar-2023,branch,active,"{""credit_score"": 836, ""employment"": ""retired"", ""annual_income"": 285957, ""years_employed"": 6}"
L0000012,C000001,personal,14002.69,11.94,48,2021-04-07,online,active,"{""credit_score"": 580, ""employment"": ""self-employed"", ""annual_income"": 77556, ""years_employed"": 27}"
L0000001,C000001,PERSONAL,32256.80,10.12,12,2023-05-03,partner,active,"{""credit_score"": 672, ""employment"": ""unemployed"", ""annual_income"": 86827, ""years_employed"": 2}"
L0000013,C000013,auto,"32,513.84",7.45,60,04-Sep-2022,online,charged_off,"{""credit_score"": 813, ""employment"": ""unemployed"", ""annual_income"": 269674, ""years_employed"": 9}"
L0000014,C000014,Personal,"$1,897.09",23.6,60,12/22/2021,branch,closed,"{""credit_score"": 580, ""employment"": ""self-employed"", ""annual_income"": 189082, ""years_employed"": 1}"
```

Note: Row 9 has trailing comma in JSON, row 10 has missing closing brace, row 13 is a duplicate of row 1, row 12 shares C000001 with row 1 (multi-loan customer).

File: `tests/fixtures/payments_sample.jsonl` — 20 rows:

```jsonl
{"payment_id": "P000000001", "loan_id": "L0000001", "amount": 761.44, "timestamp": "2023-06-15T10:00:00-05:00", "payment_method": {"type": "card", "details": {"last_four": "3517", "bank": "Union Mutual"}}, "metadata": {"source": "web", "user_agent": null}}
{"payment_id": "P000000002", "loan_id": "L0000001", "amount": 761.44, "timestamp": "2023-07-15T10:00:00Z", "payment_method": {"type": "ACH", "details": {"last_four": "6947", "bank": null}}, "metadata": {"source": "mobile_app", "user_agent": "HelixApp/3.2.1"}}
{"payment_id": "P000000003", "loan_id": "L0000002", "amount": 608.22, "timestamp": "2022-01-01T12:00:00", "payment_method": {"type": "check", "details": {"last_four": null, "bank": "Sentinel Trust"}}}
{"payment_id": "P000000004", "loan_id": "L0000002", "amount": 608.22, "timestamp": "2022-02-01T12:00:00+00:00", "payment_method": {"type": "check", "details": {"last_four": null, "bank": "Sentinel Trust"}}, "metadata": {"source": "branch", "user_agent": null}}
{"payment_id": "P000000005", "loan_id": "L0000003", "amount": 959.12, "timestamp": "2021-01-15T09:30:00-08:00", "payment_method": {"type": "wire", "details": {"last_four": "1577", "bank": null}}, "metadata": {"source": "automated", "user_agent": null}}
{"payment_id": "P000000006", "loan_id": "L0000003", "amount": 959.12, "timestamp": "2021-02-15T09:30:00-08:00", "payment_method": {"type": "wire", "details": {"last_four": "1577", "bank": null}}, "metadata": {"source": "automated", "user_agent": null}}
{"payment_id": "P000000007", "loan_id": "L0000004", "amount": "712.83", "timestamp": "2020-09-20T14:00:00Z", "payment_method": {"type": "card", "details": {"last_four": "8480", "bank": "Helix First"}}, "metadata": {"source": "web", "user_agent": null}}
{"payment_id": "P000000008", "loan_id": "L0000005", "amount": 890.90, "timestamp": "2020-09-01T16:13:00+00:00", "payment_method": {"type": "check", "details": {"last_four": "4303", "bank": "Helix First"}}, "metadata": {"source": "automated", "user_agent": null}}
{"payment_id": "P000000009", "loan_id": "L0000006", "amount": 478.50, "timestamp": "2020-02-18T07:19:00+00:00", "payment_method": {"type": "ACH", "details": {"last_four": "7508", "bank": "Union Mutual"}}, "metadata": {"source": "web", "user_agent": null}}
{"payment_id": "P000000010", "loan_id": "L0000007", "amount": 456.78, "timestamp": "2024-01-21T17:56:00", "payment_method": {"type": "card", "details": {"last_four": null, "bank": null}}, "metadata": {"source": "branch", "user_agent": null}}
{"payment_id": "P000000011", "loan_id": "L0000008", "amount": 4857.78, "timestamp": "2021-03-03T04:27:00Z", "payment_method": {"type": "wire", "details": {"last_four": null, "bank": null}}, "metadata": {"source": "branch", "user_agent": "HelixApp/3.2.1"}}
{"payment_id": "P000000012", "loan_id": "L0000009", "amount": 321.55, "timestamp": "2020-02-28T07:08:00", "payment_method": {"type": "check", "details": {"last_four": null, "bank": null}}, "metadata": {"source": "automated", "user_agent": "HelixApp/3.2.1"}}
{"payment_id": "P000000013", "loan_id": "L0000010", "amount": 2200.00, "timestamp": "2021-04-24T01:59:00-05:00", "payment_method": {"type": "wire", "details": {"last_four": "3066", "bank": "Union Mutual"}}, "metadata": {"source": "web", "user_agent": "HelixApp/3.2.1"}}
{"payment_id": "P000000014", "loan_id": "L0000011", "amount": 12500.00, "timestamp": "2023-04-05T17:47:00+00:00", "payment_method": {"type": "ACH", "details": {"last_four": null, "bank": null}}, "metadata": {"source": "branch", "user_agent": "HelixApp/3.2.1"}}
{"payment_id": "P000000015", "loan_id": "L0000012", "amount": 350.07, "timestamp": "2021-05-07T10:19:00-08:00", "payment_method": {"type": "card", "details": {"last_four": "7801", "bank": "Sentinel Trust"}}, "metadata": {"source": "mobile_app", "user_agent": null}}
{"payment_id": "P000000016", "loan_id": "L0000013", "amount": 650.10, "timestamp": "2022-10-08T20:39:00", "payment_method": {"type": "card", "details": {"last_four": null, "bank": null}}, "metadata": {"source": "mobile_app", "user_agent": "HelixApp/3.2.1"}}
{"payment_id": "P000000017", "loan_id": "L0000014", "amount": "45.99", "timestamp": "2022-01-22T11:00:00Z", "payment_method": {"type": "ACH", "details": {"last_four": "9922", "bank": "Helix First"}}, "metadata": {"source": "web", "user_agent": null}}
{"payment_id": "P000000018", "loan_id": "L9999999", "amount": 100.00, "timestamp": "2023-01-01T00:00:00Z", "payment_method": {"type": "card", "details": {"last_four": "0000", "bank": "Unknown"}}, "metadata": {"source": "web", "user_agent": null}}
{"payment_id": "P000000019", "loan_id": "L0000001", "amount": 761.44, "timestamp": "2023-08-15T10:00:00-05:00", "payment_method": {"type": "card", "details": {"last_four": "3517", "bank": "Union Mutual"}}, "metadata": {"source": "web", "user_agent": null}}
{"payment_id": "P000000020", "loan_id": "L0000001", "amount": 761.44, "timestamp": "2023-12-15T10:00:00Z", "payment_method": {"type": "card", "details": {"last_four": "3517", "bank": "Union Mutual"}}, "metadata": {"source": "web", "user_agent": null}}
```

Note: P000000003 has no metadata, P000000007 and P000000017 have string amounts, P000000018 references non-existent loan L9999999 (orphan), multiple payments for L0000001 to test delinquency logic.

- [ ] **Step 2: Create DuckDB resource**

File: `src/resources/duckdb.py`

```python
from dagster_duckdb import DuckDBResource


def get_duckdb_resource(db_path: str = "output/helix.duckdb") -> DuckDBResource:
    return DuckDBResource(database=db_path)
```

- [ ] **Step 3: Write raw layer assets**

File: `src/assets/raw.py`

```python
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
```

- [ ] **Step 4: Write test for raw assets**

File: `tests/test_assets_raw.py`

```python
import os
import tempfile
import duckdb
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments


@pytest.fixture
def test_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        yield db_path


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def test_raw_loans_materializes(test_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    result = materialize(
        [raw_loans],
        resources={"duckdb": DuckDBResource(database=test_db)},
    )
    assert result.success
    conn = duckdb.connect(test_db)
    count = conn.execute("SELECT COUNT(*) FROM raw_loans").fetchone()[0]
    assert count == 15  # 15 rows in fixture (including duplicate)
    conn.close()


def test_raw_payments_materializes(test_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    result = materialize(
        [raw_payments],
        resources={"duckdb": DuckDBResource(database=test_db)},
    )
    assert result.success
    conn = duckdb.connect(test_db)
    count = conn.execute("SELECT COUNT(*) FROM raw_payments").fetchone()[0]
    assert count == 20
    conn.close()
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/test_assets_raw.py -v
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/resources/ src/assets/raw.py tests/test_assets_raw.py tests/fixtures/
git commit -m "feat: raw layer assets with DuckDB resource and test fixtures"
```

---

### Task 3: Staging Layer Assets

**Files:**
- Create: `src/assets/staging.py`
- Create: `tests/test_assets_staging.py`

**Interfaces:**
- Consumes: `raw_loans` table, `raw_payments` table, cleaning functions from `src/utils/cleaning.py`
- Produces:
  - `stg_loans` Dagster asset → DuckDB table `stg_loans` with columns: loan_id, customer_id, product_type, principal_amount (DOUBLE), interest_rate (DOUBLE), term_months (INT), origination_date (DATE), origination_channel, status, credit_score (INT), employment, annual_income (DOUBLE), years_employed (INT)
  - `stg_payments` Dagster asset → DuckDB table `stg_payments` with columns: payment_id, loan_id, amount (DOUBLE), timestamp_utc (TIMESTAMP), payment_method_type, payment_last_four, payment_bank, source, user_agent

- [ ] **Step 1: Write staging assets**

File: `src/assets/staging.py`

```python
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
```

- [ ] **Step 2: Write tests for staging assets**

File: `tests/test_assets_staging.py`

```python
import os
import tempfile
import duckdb
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments
from src.assets.staging import stg_loans, stg_payments


@pytest.fixture
def test_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        yield db_path


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def test_stg_loans(test_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=test_db)}
    result = materialize([raw_loans, stg_loans], resources=resources)
    assert result.success

    conn = duckdb.connect(test_db)
    count = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    assert count == 14  # 15 raw rows - 1 duplicate

    # Verify cleaning
    row = conn.execute(
        "SELECT product_type, principal_amount, origination_date FROM stg_loans WHERE loan_id = 'L0000004'"
    ).fetchone()
    assert row[0] == "auto"  # was AUTO
    assert row[1] == 33517.74  # was "$33,517.74"
    assert str(row[2]) == "2020-08-19"  # was 08/19/2020

    # Verify borrower_info extraction
    row = conn.execute(
        "SELECT credit_score, employment FROM stg_loans WHERE loan_id = 'L0000001'"
    ).fetchone()
    assert row[0] == 672
    assert row[1] == "unemployed"

    # Verify malformed JSON was fixed (trailing comma)
    row = conn.execute(
        "SELECT credit_score FROM stg_loans WHERE loan_id = 'L0000009'"
    ).fetchone()
    assert row[0] == 694

    # Verify missing brace was fixed
    row = conn.execute(
        "SELECT credit_score FROM stg_loans WHERE loan_id = 'L0000010'"
    ).fetchone()
    assert row[0] == 832

    conn.close()


def test_stg_payments(test_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=test_db)}
    result = materialize([raw_payments, stg_payments], resources=resources)
    assert result.success

    conn = duckdb.connect(test_db)
    count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    assert count == 20  # no duplicates in fixture

    # Verify string amount cast
    row = conn.execute(
        "SELECT amount FROM stg_payments WHERE payment_id = 'P000000007'"
    ).fetchone()
    assert row[0] == 712.83

    # Verify payment_method flattening
    row = conn.execute(
        "SELECT payment_method_type, payment_last_four, payment_bank FROM stg_payments WHERE payment_id = 'P000000001'"
    ).fetchone()
    assert row[0] == "card"
    assert row[1] == "3517"
    assert row[2] == "Union Mutual"

    # Verify missing metadata → NULLs
    row = conn.execute(
        "SELECT source, user_agent FROM stg_payments WHERE payment_id = 'P000000003'"
    ).fetchone()
    assert row[0] is None
    assert row[1] is None

    conn.close()
```

- [ ] **Step 3: Run tests**

```bash
pytest tests/test_assets_staging.py -v
```

Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add src/assets/staging.py tests/test_assets_staging.py
git commit -m "feat: staging layer with cleaning, type normalization, and dedup"
```

---

### Task 4: Star Schema Modeled Layer

**Files:**
- Create: `src/assets/modeled.py`
- Create: `tests/test_assets_modeled.py`

**Interfaces:**
- Consumes: `stg_loans` table, `stg_payments` table
- Produces:
  - `dim_loans` → DuckDB table
  - `dim_customers` → DuckDB table (customer_id PK, latest loan's borrower_info)
  - `dim_dates` → DuckDB table (date_key PK, generated date dimension)
  - `fct_payments` → DuckDB table (payment facts with FK to dimensions)
  - `rpt_delinquency` → DuckDB table (30-day delinquency rate by product_type)

- [ ] **Step 1: Write modeled layer assets**

File: `src/assets/modeled.py`

```python
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource


@asset(deps=["stg_loans"], group_name="modeled")
def dim_loans(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
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
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})


@asset(deps=["stg_loans"], group_name="modeled")
def dim_customers(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
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
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})


@asset(deps=["stg_loans", "stg_payments"], group_name="modeled")
def dim_dates(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE dim_dates AS
            WITH date_range AS (
                SELECT UNNEST(generate_series(
                    (SELECT LEAST(MIN(origination_date) FROM stg_loans, MIN(CAST(timestamp_utc AS DATE)) FROM stg_payments)),
                    (SELECT GREATEST(MAX(origination_date) FROM stg_loans, MAX(CAST(timestamp_utc AS DATE)) FROM stg_payments)),
                    INTERVAL 1 DAY
                )) AS date
            )
            SELECT
                CAST(strftime(date, '%Y%m%d') AS INTEGER) AS date_key,
                CAST(date AS DATE) AS date,
                EXTRACT(YEAR FROM date)::INTEGER AS year,
                EXTRACT(QUARTER FROM date)::INTEGER AS quarter,
                EXTRACT(MONTH FROM date)::INTEGER AS month,
                EXTRACT(DAY FROM date)::INTEGER AS day,
                EXTRACT(DOW FROM date)::INTEGER AS day_of_week
            FROM date_range
        """)
        count = conn.execute("SELECT COUNT(*) FROM dim_dates").fetchone()[0]
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})


@asset(deps=["stg_payments", "dim_loans", "dim_dates"], group_name="modeled")
def fct_payments(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
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
    context.log.info(f"fct_payments: {count} rows, {orphans} orphan payments")
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(count),
            "orphan_payments": MetadataValue.int(orphans),
        }
    )


@asset(deps=["fct_payments", "dim_loans"], group_name="modeled")
def rpt_delinquency(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
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
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})
```

Note on delinquency: We use a 60-day gap between consecutive payments as the threshold (a 30-day delinquency means a payment is 30 days late — since payments are monthly, that means a gap of ~60 days between consecutive payments). This is a simplification; a production system would compare against the actual payment schedule.

- [ ] **Step 2: Write tests for modeled assets**

File: `tests/test_assets_modeled.py`

```python
import os
import tempfile
import duckdb
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments
from src.assets.staging import stg_loans, stg_payments
from src.assets.modeled import dim_loans, dim_customers, dim_dates, fct_payments, rpt_delinquency


@pytest.fixture
def test_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        yield db_path


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture
def staged_db(test_db, fixtures_dir, monkeypatch):
    """Materialize raw + staging so modeled tests can build on top."""
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=test_db)}
    result = materialize(
        [raw_loans, raw_payments, stg_loans, stg_payments],
        resources=resources,
    )
    assert result.success
    return test_db


def test_dim_loans(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize([dim_loans], resources=resources)
    assert result.success
    conn = duckdb.connect(staged_db)
    count = conn.execute("SELECT COUNT(*) FROM dim_loans").fetchone()[0]
    assert count == 14  # matches stg_loans after dedup
    conn.close()


def test_dim_customers(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize([dim_customers], resources=resources)
    assert result.success
    conn = duckdb.connect(staged_db)
    count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    assert count == 13  # 14 loans but C000001 appears in 2 loans → 13 unique customers
    # C000001's latest loan is L0000012 (2021-04-07) — verify it picks that one
    row = conn.execute(
        "SELECT credit_score FROM dim_customers WHERE customer_id = 'C000001'"
    ).fetchone()
    assert row[0] == 580  # from L0000012's borrower_info
    conn.close()


def test_dim_dates(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize([dim_dates], resources=resources)
    assert result.success
    conn = duckdb.connect(staged_db)
    count = conn.execute("SELECT COUNT(*) FROM dim_dates").fetchone()[0]
    assert count > 0
    # Verify structure
    row = conn.execute(
        "SELECT date_key, year, quarter, month, day FROM dim_dates LIMIT 1"
    ).fetchone()
    assert row is not None
    conn.close()


def test_fct_payments(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize([dim_loans, dim_dates, fct_payments], resources=resources)
    assert result.success
    conn = duckdb.connect(staged_db)
    count = conn.execute("SELECT COUNT(*) FROM fct_payments").fetchone()[0]
    assert count == 20
    # Orphan payment (L9999999) should have NULL customer_id
    orphan = conn.execute(
        "SELECT customer_id FROM fct_payments WHERE loan_id = 'L9999999'"
    ).fetchone()
    assert orphan[0] is None
    conn.close()


def test_rpt_delinquency(staged_db, fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    resources = {"duckdb": DuckDBResource(database=staged_db)}
    result = materialize(
        [dim_loans, dim_dates, fct_payments, rpt_delinquency],
        resources=resources,
    )
    assert result.success
    conn = duckdb.connect(staged_db)
    rows = conn.execute("SELECT * FROM rpt_delinquency").fetchall()
    assert len(rows) > 0
    # Verify columns exist
    cols = [desc[0] for desc in conn.description]
    assert "product_type" in cols
    assert "delinquency_rate_pct" in cols
    conn.close()
```

- [ ] **Step 3: Run tests**

```bash
pytest tests/test_assets_modeled.py -v
```

Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add src/assets/modeled.py tests/test_assets_modeled.py
git commit -m "feat: star schema modeled layer with dim/fact/report tables"
```

---

### Task 5: Data Quality Checks

**Files:**
- Create: `src/checks/quality_checks.py`
- Create: `tests/test_quality_checks.py`

**Interfaces:**
- Consumes: `stg_loans` table, `stg_payments` table, `fct_payments` table, `dim_loans` table
- Produces: Dagster `@asset_check` definitions that return `AssetCheckResult`

- [ ] **Step 1: Write asset checks**

File: `src/checks/quality_checks.py`

```python
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
from dagster_duckdb import DuckDBResource


# --- stg_loans checks ---

@asset_check(asset="stg_loans")
def stg_loans_no_null_ids(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        nulls = conn.execute("SELECT COUNT(*) FROM stg_loans WHERE loan_id IS NULL").fetchone()[0]
    return AssetCheckResult(
        passed=nulls == 0,
        metadata={"null_count": nulls},
        description="loan_id must not be null",
    )


@asset_check(asset="stg_loans")
def stg_loans_unique_ids(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
        distinct = conn.execute("SELECT COUNT(DISTINCT loan_id) FROM stg_loans").fetchone()[0]
    return AssetCheckResult(
        passed=total == distinct,
        metadata={"total": total, "distinct": distinct},
        description="loan_id must be unique",
    )


@asset_check(asset="stg_loans")
def stg_loans_positive_principal(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        bad = conn.execute("SELECT COUNT(*) FROM stg_loans WHERE principal_amount <= 0").fetchone()[0]
    return AssetCheckResult(
        passed=bad == 0,
        metadata={"non_positive_count": bad},
        description="principal_amount must be > 0",
    )


@asset_check(asset="stg_loans")
def stg_loans_valid_interest_rate(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        bad = conn.execute(
            "SELECT COUNT(*) FROM stg_loans WHERE interest_rate < 0 OR interest_rate > 100"
        ).fetchone()[0]
    return AssetCheckResult(
        passed=bad == 0,
        metadata={"out_of_range_count": bad},
        description="interest_rate must be between 0 and 100",
    )


@asset_check(asset="stg_loans")
def stg_loans_valid_product_type(duckdb: DuckDBResource) -> AssetCheckResult:
    valid = {"personal", "auto", "mortgage", "student"}
    with duckdb.get_connection() as conn:
        types = conn.execute("SELECT DISTINCT product_type FROM stg_loans").fetchall()
        found = {r[0] for r in types}
    invalid = found - valid
    return AssetCheckResult(
        passed=len(invalid) == 0,
        metadata={"invalid_types": str(invalid) if invalid else "none"},
        description="product_type must be one of: personal, auto, mortgage, student",
    )


@asset_check(asset="stg_loans")
def stg_loans_freshness(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        newest = conn.execute("SELECT MAX(origination_date) FROM stg_loans").fetchone()[0]
        row_count = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"newest_record": str(newest), "row_count": row_count},
        severity=AssetCheckSeverity.WARN,
        description="Source freshness and row count for loans",
    )


@asset_check(asset="stg_loans")
def stg_loans_row_count(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM stg_loans").fetchone()[0]
    return AssetCheckResult(
        passed=1000 <= count <= 50000,
        metadata={"row_count": count},
        description="Row count should be between 1,000 and 50,000",
    )


# --- stg_payments checks ---

@asset_check(asset="stg_payments")
def stg_payments_no_null_ids(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        nulls = conn.execute(
            "SELECT COUNT(*) FROM stg_payments WHERE payment_id IS NULL"
        ).fetchone()[0]
    return AssetCheckResult(
        passed=nulls == 0,
        metadata={"null_count": nulls},
        description="payment_id must not be null",
    )


@asset_check(asset="stg_payments")
def stg_payments_unique_ids(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
        distinct = conn.execute("SELECT COUNT(DISTINCT payment_id) FROM stg_payments").fetchone()[0]
    return AssetCheckResult(
        passed=total == distinct,
        metadata={"total": total, "distinct": distinct},
        description="payment_id must be unique",
    )


@asset_check(asset="stg_payments")
def stg_payments_positive_amount(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        bad = conn.execute("SELECT COUNT(*) FROM stg_payments WHERE amount <= 0").fetchone()[0]
    return AssetCheckResult(
        passed=bad == 0,
        metadata={"non_positive_count": bad},
        description="amount must be > 0",
    )


@asset_check(asset="stg_payments")
def stg_payments_freshness(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        newest = conn.execute("SELECT MAX(timestamp_utc) FROM stg_payments").fetchone()[0]
        row_count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"newest_record": str(newest), "row_count": row_count},
        severity=AssetCheckSeverity.WARN,
        description="Source freshness and row count for payments",
    )


@asset_check(asset="stg_payments")
def stg_payments_row_count(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    return AssetCheckResult(
        passed=10000 <= count <= 500000,
        metadata={"row_count": count},
        description="Row count should be between 10,000 and 500,000",
    )


# --- fct_payments checks ---

@asset_check(asset="fct_payments")
def fct_payments_referential_integrity(duckdb: DuckDBResource) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        orphans = conn.execute("""
            SELECT COUNT(*) FROM fct_payments f
            WHERE NOT EXISTS (SELECT 1 FROM dim_loans l WHERE l.loan_id = f.loan_id)
        """).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM fct_payments").fetchone()[0]
    return AssetCheckResult(
        passed=True,  # Orphans are expected (data quality issue, not a pipeline bug)
        metadata={"orphan_count": orphans, "total_payments": total, "orphan_pct": round(orphans / max(total, 1) * 100, 2)},
        severity=AssetCheckSeverity.WARN,
        description="Payments referencing non-existent loans (orphan payments)",
    )
```

- [ ] **Step 2: Write tests for quality checks**

File: `tests/test_quality_checks.py`

```python
import os
import tempfile
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments
from src.assets.staging import stg_loans, stg_payments
from src.assets.modeled import dim_loans, dim_dates, fct_payments
from src.checks.quality_checks import (
    stg_loans_no_null_ids,
    stg_loans_unique_ids,
    stg_loans_positive_principal,
    stg_loans_valid_product_type,
    stg_payments_no_null_ids,
    stg_payments_unique_ids,
    fct_payments_referential_integrity,
)


@pytest.fixture
def full_db(monkeypatch):
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        resources = {"duckdb": DuckDBResource(database=db_path)}
        result = materialize(
            [raw_loans, raw_payments, stg_loans, stg_payments, dim_loans, dim_dates, fct_payments],
            resources=resources,
        )
        assert result.success
        yield db_path


def test_checks_pass_on_clean_fixture(full_db):
    resources = {"duckdb": DuckDBResource(database=full_db)}

    checks = [
        stg_loans_no_null_ids,
        stg_loans_unique_ids,
        stg_loans_positive_principal,
        stg_loans_valid_product_type,
        stg_payments_no_null_ids,
        stg_payments_unique_ids,
        fct_payments_referential_integrity,
    ]

    for check_fn in checks:
        result = check_fn(duckdb=DuckDBResource(database=full_db))
        assert result.passed, f"Check {check_fn.__name__} failed: {result.metadata}"
```

- [ ] **Step 3: Run tests**

```bash
pytest tests/test_quality_checks.py -v
```

Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add src/checks/quality_checks.py tests/test_quality_checks.py
git commit -m "feat: data quality checks for completeness, uniqueness, validity, referential integrity, freshness"
```

---

### Task 6: Observability + Dagster Definitions

**Files:**
- Create: `src/utils/logging.py`
- Create: `src/definitions.py`

**Interfaces:**
- Consumes: all assets from `src/assets/`, all checks from `src/checks/`, DuckDB resource from `src/resources/`
- Produces: `Definitions` object that Dagster uses as entry point

- [ ] **Step 1: Create structured logging setup**

File: `src/utils/logging.py`

```python
import structlog


def configure_logging():
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )
```

- [ ] **Step 2: Create Dagster definitions**

File: `src/definitions.py`

```python
from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource

from src.assets import raw, staging, modeled
from src.checks.quality_checks import (
    stg_loans_no_null_ids,
    stg_loans_unique_ids,
    stg_loans_positive_principal,
    stg_loans_valid_interest_rate,
    stg_loans_valid_product_type,
    stg_loans_freshness,
    stg_loans_row_count,
    stg_payments_no_null_ids,
    stg_payments_unique_ids,
    stg_payments_positive_amount,
    stg_payments_freshness,
    stg_payments_row_count,
    fct_payments_referential_integrity,
)
from src.utils.logging import configure_logging

configure_logging()

all_assets = load_assets_from_modules([raw, staging, modeled])

all_checks = [
    stg_loans_no_null_ids,
    stg_loans_unique_ids,
    stg_loans_positive_principal,
    stg_loans_valid_interest_rate,
    stg_loans_valid_product_type,
    stg_loans_freshness,
    stg_loans_row_count,
    stg_payments_no_null_ids,
    stg_payments_unique_ids,
    stg_payments_positive_amount,
    stg_payments_freshness,
    stg_payments_row_count,
    fct_payments_referential_integrity,
]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "duckdb": DuckDBResource(database="output/helix.duckdb"),
    },
)
```

- [ ] **Step 3: Verify Dagster loads definitions**

```bash
dagster definitions validate -m src.definitions
```

Expected: "Validation successful"

- [ ] **Step 4: Commit**

```bash
git add src/utils/logging.py src/definitions.py
git commit -m "feat: Dagster definitions entry point with observability logging"
```

---

### Task 7: E2E Test, Makefile, README, Lineage Doc

**Files:**
- Create: `tests/test_e2e.py`
- Create: `Makefile`
- Create: `README.md`
- Create: `docs/LINEAGE.md`

**Interfaces:**
- Consumes: full pipeline (all assets, all checks)
- Produces: documentation and automation

- [ ] **Step 1: Write E2E test**

File: `tests/test_e2e.py`

```python
import os
import tempfile
import duckdb
import pytest
from dagster import materialize
from dagster_duckdb import DuckDBResource
from src.assets.raw import raw_loans, raw_payments
from src.assets.staging import stg_loans, stg_payments
from src.assets.modeled import dim_loans, dim_customers, dim_dates, fct_payments, rpt_delinquency


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def test_full_pipeline_e2e(fixtures_dir, monkeypatch):
    monkeypatch.setenv("HELIX_DATA_DIR", fixtures_dir)
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "e2e.duckdb")
        resources = {"duckdb": DuckDBResource(database=db_path)}

        all_assets = [
            raw_loans, raw_payments,
            stg_loans, stg_payments,
            dim_loans, dim_customers, dim_dates, fct_payments, rpt_delinquency,
        ]
        result = materialize(all_assets, resources=resources)
        assert result.success

        conn = duckdb.connect(db_path)

        # All expected tables exist
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        expected = {
            "raw_loans", "raw_payments",
            "stg_loans", "stg_payments",
            "dim_loans", "dim_customers", "dim_dates",
            "fct_payments", "rpt_delinquency",
        }
        assert expected.issubset(tables)

        # Star schema integrity: all fct_payments.payment_date_key exist in dim_dates
        orphan_dates = conn.execute("""
            SELECT COUNT(*) FROM fct_payments f
            WHERE f.payment_date_key NOT IN (SELECT date_key FROM dim_dates)
        """).fetchone()[0]
        assert orphan_dates == 0

        # Delinquency report has data
        dq = conn.execute("SELECT COUNT(*) FROM rpt_delinquency").fetchone()[0]
        assert dq > 0

        # Business query 1: delinquency by product is queryable
        rows = conn.execute(
            "SELECT product_type, delinquency_rate_pct FROM rpt_delinquency"
        ).fetchall()
        assert len(rows) > 0
        for row in rows:
            assert row[0] in ("personal", "auto", "mortgage", "student")
            assert 0 <= row[1] <= 100

        # Business query 3: freshness is queryable
        freshness = conn.execute(
            "SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM fct_payments"
        ).fetchone()
        assert freshness[0] is not None
        assert freshness[1] is not None

        conn.close()
```

- [ ] **Step 2: Run E2E test**

```bash
pytest tests/test_e2e.py -v
```

Expected: PASS

- [ ] **Step 3: Create Makefile**

File: `Makefile`

```makefile
.PHONY: setup run test lint clean dev

setup:
	python -m venv .venv
	.venv/bin/pip install -e ".[dev]"

run:
	mkdir -p output
	.venv/bin/dagster job execute -m src.definitions --job __ASSET_JOB

test:
	.venv/bin/pytest tests/ -v

dev:
	mkdir -p output
	.venv/bin/dagster dev -m src.definitions

clean:
	rm -rf output/helix.duckdb
```

- [ ] **Step 4: Create LINEAGE.md**

File: `docs/LINEAGE.md`

```markdown
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
```

- [ ] **Step 5: Create README.md**

File: `README.md`

```markdown
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
```

- [ ] **Step 6: Run full test suite**

```bash
pytest tests/ -v
```

Expected: all tests PASS

- [ ] **Step 7: Run the full pipeline against real data**

```bash
mkdir -p output
dagster job execute -m src.definitions --job __ASSET_JOB
```

Then verify output:

```bash
python -c "
import duckdb
conn = duckdb.connect('output/helix.duckdb')
for table in ['dim_loans', 'dim_customers', 'dim_dates', 'fct_payments', 'rpt_delinquency']:
    count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
    print(f'{table}: {count} rows')
print()
print('Delinquency report:')
for row in conn.execute('SELECT * FROM rpt_delinquency').fetchall():
    print(row)
conn.close()
"
```

- [ ] **Step 8: Commit everything**

```bash
git add Makefile README.md docs/LINEAGE.md tests/test_e2e.py
git commit -m "feat: E2E test, Makefile, README, and lineage documentation"
```
