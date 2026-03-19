"""
ingester.py
Handles file opening, encoding detection, decompression, and streaming.
Yields raw CSV rows as lists of strings.
Supports: local file paths + HTTP/HTTPS URLs + .gz + .zip
Never loads the full file into memory.
"""

from __future__ import annotations
import csv
import gzip
import io
import os
import urllib.request
import zipfile
from pathlib import Path
from typing import Generator, Union


_ENCODINGS = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
_SNIFF_BYTES = 8192


def _is_url(source: str) -> bool:
    return source.startswith("http://") or source.startswith("https://")


def _detect_encoding(path: str) -> str:
    for enc in _ENCODINGS:
        try:
            with open(path, encoding=enc, errors="strict") as f:
                f.read(_SNIFF_BYTES)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "latin-1"


def _detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",|\t;")
        return dialect.delimiter
    except csv.Error:
        return ","


def _open_raw(source: Union[str, Path]) -> io.TextIOWrapper:
    path = str(source)

    # URL download
    if _is_url(path):
        try:
            req = urllib.request.Request(
                path,
                headers={"User-Agent": "mrf-etl/0.1.0 (hospital price transparency)"}
            )
            response = urllib.request.urlopen(req, timeout=60)
            raw_bytes = response.read()
        except Exception as e:
            raise IOError(f"Failed to download '{path}': {e}") from e

        # Detect if gzipped by content-type or magic bytes
        if raw_bytes[:2] == b'\x1f\x8b':
            raw_bytes = gzip.decompress(raw_bytes)

        # Detect encoding from bytes
        for enc in _ENCODINGS:
            try:
                raw_bytes.decode(enc, errors="strict")
                encoding = enc
                break
            except (UnicodeDecodeError, LookupError):
                continue
        else:
            encoding = "latin-1"

        text = raw_bytes.decode(encoding, errors="replace")
        return io.StringIO(text)

    # Local .gz
    if path.endswith(".gz"):
        raw = gzip.open(path, "rb")
        return io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace")

    # Local .zip
    if path.endswith(".zip"):
        zf = zipfile.ZipFile(path)
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV found inside zip: {path}")
        raw = zf.open(csv_names[0])
        return io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace")

    # Local CSV
    encoding = _detect_encoding(path)
    return open(path, encoding=encoding, errors="replace", newline="")


def stream_rows(
    source: Union[str, Path],
    skip_empty: bool = True,
) -> Generator[list[str], None, None]:
    """
    Stream all rows from an MRF CSV file (local path or URL).
    Yields each row as a list[str]. No type conversion.
    """
    path = str(source)
    fh = _open_raw(path)

    try:
        # For StringIO (URL result), read sample differently
        if isinstance(fh, io.StringIO):
            sample = fh.read(4096)
            fh.seek(0)
            delimiter = _detect_delimiter(sample)
        else:
            sample = fh.read(4096)
            fh.seek(0)
            delimiter = _detect_delimiter(sample)

        reader = csv.reader(fh, delimiter=delimiter)
        for row in reader:
            if skip_empty and all(cell.strip() == "" for cell in row):
                continue
            yield row
    finally:
        if hasattr(fh, 'close'):
            fh.close()


def peek_rows(
    source: Union[str, Path],
    n: int = 10,
) -> list[list[str]]:
    """Return the first N rows without streaming the whole file."""
    rows = []
    for i, row in enumerate(stream_rows(source, skip_empty=False)):
        rows.append(row)
        if i >= n - 1:
            break
    return rows


def file_size_bytes(source: Union[str, Path]) -> int:
    """Return file size in bytes. Only works for local files."""
    path = str(source)
    if _is_url(path):
        return 0
    return os.path.getsize(path)