"""Date column detection for CSV files."""

import csv
from datetime import datetime
from pathlib import Path

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%d/%m/%Y",
    "%d-%m-%Y",
]


class DateDetector:
    """Detects which CSV columns contain date/timestamp values by sampling rows."""

    @staticmethod
    def parse_date(value: str) -> datetime | None:
        """Try parsing a string as a date using known formats. Returns first match or None."""
        value = value.strip()
        if not value:
            return None
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def detect_date_columns(
        csv_path: str | Path,
        has_header: bool = True,
        delimiter: str = ",",
        encoding: str = "utf-8",
        sample_size: int = 100,
        threshold: float = 0.8,
    ) -> set[str]:
        """Detect columns that contain date values by sampling rows.

        Args:
            csv_path: Path to CSV file.
            has_header: Whether the CSV has a header row.
            delimiter: CSV delimiter character.
            encoding: File encoding.
            sample_size: Number of rows to sample.
            threshold: Fraction of non-empty values that must parse as dates (0.0-1.0).

        Returns:
            Set of column names detected as date columns.
        """
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        with open(path, newline="", encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)

            rows_sampled: list[list[str]] = []

            if has_header:
                header = next(reader, None)
                if header is None:
                    return set()
                columns = header
            else:
                first_row = next(reader, None)
                if first_row is None:
                    return set()
                columns = [f"col_{i + 1}" for i in range(len(first_row))]
                rows_sampled.append(first_row)

            # Sample rows
            for row in reader:
                rows_sampled.append(row)
                if len(rows_sampled) >= sample_size:
                    break

        if not rows_sampled:
            return set()

        num_cols = len(columns)
        date_columns: set[str] = set()

        for col_idx in range(num_cols):
            non_empty = 0
            parsed = 0
            for row in rows_sampled:
                if col_idx >= len(row):
                    continue
                val = row[col_idx].strip()
                if not val:
                    continue
                non_empty += 1
                if DateDetector.parse_date(val) is not None:
                    parsed += 1

            if non_empty > 0 and (parsed / non_empty) >= threshold:
                date_columns.add(columns[col_idx])

        return date_columns
