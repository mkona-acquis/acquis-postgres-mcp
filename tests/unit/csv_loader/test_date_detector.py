"""Tests for DateDetector."""

import os
import tempfile

import pytest

from postgres_mcp.csv_loader.date_detector import DateDetector

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "fixtures", "csv")


class TestParseDate:
    def test_iso_date(self):
        result = DateDetector.parse_date("2024-01-15")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_iso_datetime(self):
        result = DateDetector.parse_date("2024-01-15 14:30:00")
        assert result is not None
        assert result.hour == 14
        assert result.minute == 30

    def test_iso_datetime_t(self):
        result = DateDetector.parse_date("2024-01-15T14:30:00")
        assert result is not None

    def test_us_date_slash(self):
        result = DateDetector.parse_date("01/15/2024")
        assert result is not None
        assert result.month == 1
        assert result.day == 15

    def test_us_date_dash(self):
        result = DateDetector.parse_date("01-15-2024")
        assert result is not None

    def test_eu_date_slash(self):
        result = DateDetector.parse_date("15/01/2024")
        assert result is not None

    def test_eu_date_dash(self):
        result = DateDetector.parse_date("15-01-2024")
        assert result is not None

    def test_non_date(self):
        assert DateDetector.parse_date("hello") is None
        assert DateDetector.parse_date("12345") is None
        assert DateDetector.parse_date("") is None

    def test_whitespace_stripped(self):
        result = DateDetector.parse_date("  2024-01-15  ")
        assert result is not None


class TestDetectDateColumns:
    def test_with_dates_fixture(self):
        path = os.path.join(FIXTURES_DIR, "with_dates.csv")
        result = DateDetector.detect_date_columns(path)
        assert "created_at" in result
        assert "updated_at" in result
        assert "name" not in result
        assert "score" not in result

    def test_simple_no_dates(self):
        path = os.path.join(FIXTURES_DIR, "simple.csv")
        result = DateDetector.detect_date_columns(path)
        assert len(result) == 0

    def test_no_header(self):
        path = os.path.join(FIXTURES_DIR, "no_header.csv")
        result = DateDetector.detect_date_columns(path, has_header=False)
        # col_3 should be detected as date
        assert "col_3" in result
        assert "col_1" not in result
        assert "col_2" not in result

    def test_threshold_behavior(self):
        """With a high threshold, mixed columns should not be detected."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            f.write("date_col,text_col\n")
            f.write("2024-01-15,hello\n")
            f.write("not-a-date,world\n")
            f.write("2024-03-10,foo\n")
            tmp_path = f.name

        try:
            # With default 0.8 threshold, 2/3 = 0.667 should not qualify
            result = DateDetector.detect_date_columns(tmp_path, threshold=0.8)
            assert "date_col" not in result

            # With lower threshold, it should qualify
            result = DateDetector.detect_date_columns(tmp_path, threshold=0.5)
            assert "date_col" in result
        finally:
            os.unlink(tmp_path)

    def test_empty_column_not_detected(self):
        """Columns that are all empty should not be detected as dates."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            f.write("date_col,empty_col\n")
            f.write("2024-01-15,\n")
            f.write("2024-02-28,\n")
            tmp_path = f.name

        try:
            result = DateDetector.detect_date_columns(tmp_path)
            assert "date_col" in result
            assert "empty_col" not in result
        finally:
            os.unlink(tmp_path)

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            DateDetector.detect_date_columns("/nonexistent/path.csv")

    def test_numeric_not_date(self):
        """Numeric values should not be detected as dates."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            f.write("id,amount\n")
            f.write("1,100.50\n")
            f.write("2,200.75\n")
            f.write("3,300.00\n")
            tmp_path = f.name

        try:
            result = DateDetector.detect_date_columns(tmp_path)
            assert len(result) == 0
        finally:
            os.unlink(tmp_path)
