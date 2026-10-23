"""Shared CSV escaping for Excel. No formulas, including after whitespace."""

import csv
from pathlib import Path
import os
import tempfile


def sanitize_excel_cell(value) -> str:
    text = "" if value is None else str(value)
    text = "".join(" " if ord(char) < 32 else char for char in text)
    if text.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + text
    return text


def write_private_csv(path: Path, headers, rows, *, overwrite=False) -> None:
    """Write exclusively by default; replace atomically only on explicit request."""
    descriptor, temporary = tempfile.mkstemp(
        prefix=".export-", suffix=".csv", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.writer(stream, delimiter=";")
            writer.writerow(headers)
            writer.writerows([sanitize_excel_cell(v) for v in row] for row in rows)
        if overwrite:
            os.replace(temporary, path)
        else:
            # Hard link is an atomic no-clobber operation (same filesystem).
            os.link(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
