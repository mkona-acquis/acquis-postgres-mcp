"""Tests for CsvLoader — unit tests for validation and column name handling."""

from unittest.mock import Mock

import pytest

from postgres_mcp.csv_loader import CsvLoader
from postgres_mcp.csv_loader.csv_loader import IDENTIFIER_PATTERN
from postgres_mcp.csv_loader.csv_loader import _deduplicate_columns
from postgres_mcp.csv_loader.csv_loader import _sanitize_column_name
from postgres_mcp.csv_loader.csv_loader import _validate_identifier


class TestValidateIdentifier:
    def test_valid_identifiers(self):
        _validate_identifier("public", "schema")
        _validate_identifier("my_table", "table")
        _validate_identifier("_private", "table")
        _validate_identifier("Table123", "table")

    def test_invalid_identifiers(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("123start", "table")
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("has space", "table")
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("has-dash", "table")
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("", "table")
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("drop;table", "table")

    def test_identifier_pattern(self):
        assert IDENTIFIER_PATTERN.match("valid_name")
        assert IDENTIFIER_PATTERN.match("_underscore")
        assert not IDENTIFIER_PATTERN.match("1digit")
        assert not IDENTIFIER_PATTERN.match("has space")


class TestSanitizeColumnName:
    def test_simple_name(self):
        assert _sanitize_column_name("name") == "name"

    def test_spaces_replaced(self):
        assert _sanitize_column_name("first name") == "first_name"

    def test_special_chars(self):
        assert _sanitize_column_name("price($)") == "price"

    def test_digit_prefix(self):
        assert _sanitize_column_name("1st_col") == "col_1st_col"

    def test_uppercase_lowered(self):
        assert _sanitize_column_name("MyColumn") == "mycolumn"

    def test_multiple_underscores_collapsed(self):
        assert _sanitize_column_name("a   b") == "a_b"

    def test_whitespace_stripped(self):
        assert _sanitize_column_name("  name  ") == "name"


class TestDeduplicateColumns:
    def test_no_duplicates(self):
        assert _deduplicate_columns(["a", "b", "c"]) == ["a", "b", "c"]

    def test_duplicates(self):
        assert _deduplicate_columns(["a", "a", "a"]) == ["a", "a_1", "a_2"]

    def test_mixed(self):
        assert _deduplicate_columns(["name", "age", "name"]) == ["name", "age", "name_1"]


class TestCsvLoaderInit:
    def test_init(self):
        mock_driver = Mock()
        loader = CsvLoader(mock_driver)
        assert loader.sql_driver == mock_driver


class TestCsvLoaderImports:
    def test_imports(self):
        from postgres_mcp.csv_loader import CsvLoader
        from postgres_mcp.csv_loader import DateDetector

        assert CsvLoader is not None
        assert DateDetector is not None
