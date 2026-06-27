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
