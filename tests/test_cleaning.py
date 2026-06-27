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
