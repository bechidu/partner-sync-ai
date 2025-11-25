# handlers/sftp_handler.py
import pandas as pd
import json
import re
import io
from pathlib import Path

def _try_read_csv(path):
    """
    Read CSV robustly: detect utf-16/utf-8 BOMs, decode to text, remove embedded NULs,
    then parse via pandas.read_csv from a StringIO. Tries several encodings/delims.
    """
    p = Path(path)
    raw = p.open("rb").read()
    # detect BOM for utf-16 little or big
    if raw.startswith(b'\xff\xfe') or raw.startswith(b'\xfe\xff'):
        # UTF-16 with BOM
        try:
            text = raw.decode('utf-16')
        except Exception:
            text = raw.decode('utf-16-le', errors='replace')
    else:
        # try common encodings
        tried = False
        for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
            try:
                text = raw.decode(enc)
                tried = True
                break
            except Exception:
                continue
        if not tried:
            # last resort
            text = raw.decode("latin-1", errors="replace")

    # remove embedded NULs that come from UTF-16 when interpreted as bytes
    if '\x00' in text:
        text = text.replace('\x00', '')

    # Now try various delimiters if necessary (primary is comma)
    for sep in [',', ';', '\t', '|']:
        try:
            df = pd.read_csv(io.StringIO(text), sep=sep, engine="python")
            # if df has multiple columns, assume this worked
            if df.shape[1] > 1:
                return df
        except Exception:
            continue

    # fallback: try default comma with C engine
    return pd.read_csv(io.StringIO(text), sep=',')

def _clean_column_name(name: str) -> str:
    if name is None:
        return ""
    # remove common BOMs / weird unicode controls and replace non-printables
    # normalize whitespace, remove zero-width / control characters
    s = str(name)
    # Remove BYTE ORDER MARKs and other marks
    s = s.replace("\ufeff", "").replace("\ufffe", "")
    # Remove non-printable control chars except newline/tab
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)
    # replace runs of whitespace with single space
    s = re.sub(r"\s+", " ", s).strip()
    # if after cleaning it's empty, return placeholder (we'll rename later)
    return s if s else ""

def handle_sftp_sample(file_path):
    """
    Expects a CSV or TXT file path. Returns normalized output contract.
    Sanitizes column names and attempts to recover header from first row if header appears missing.
    """
    p = Path(file_path)
    raw_text = ""
    sample_records = []

    if not p.exists():
        raise FileNotFoundError(f"{file_path} not found")

    if p.suffix.lower() in [".csv", ".txt"]:
        df = _try_read_csv(p)

        # sanitize column names
        new_cols = []
        for col in df.columns:
            cleaned = _clean_column_name(col)
            new_cols.append(cleaned)
        df.columns = new_cols
        # ensure column names are unique (append suffixes for duplicates)
        cols = list(df.columns)
        seen = {}
        unique_cols = []
        for c in cols:
            key = c if c else ""
            if key in seen:
                seen[key] += 1
                new_name = f"{key}_{seen[key]}"
            else:
                seen[key] = 0
                new_name = key
            # if column name is empty, give fallback like col_0
            if not new_name:
                new_name = f"col_{len(unique_cols)}"
            unique_cols.append(new_name)

        df.columns = unique_cols
        # now convert first 3 rows to dicts safely
        sample_records = df.head(3).fillna("").to_dict(orient="records")

        # If many columns are empty or start with 'Unnamed', attempt to use first row as header
        unnamed_count = sum(1 for c in df.columns if (not c) or c.lower().startswith("unnamed"))
        if unnamed_count >= max(1, len(df.columns)//2):
            # try promoting first row to header if it looks textual
            first_row = df.iloc[0].astype(str).tolist()
            first_row_clean = [_clean_column_name(x) for x in first_row]
            # only promote if a majority of first_row values look non-empty
            non_empty = sum(1 for x in first_row_clean if x)
            if non_empty >= max(1, len(first_row_clean)//2):
                df = df[1:].copy()
                df.columns = first_row_clean

        # attempt to read raw text safely
        try:
            raw_text = p.read_text(encoding="utf-8")
        except Exception:
            try:
                raw_text = p.read_text(encoding="utf-8-sig")
            except Exception:
                raw_text = p.read_text(encoding="latin-1", errors="replace")

        # convert first 3 rows to dicts, fill NaN with empty string
        sample_records = df.head(3).fillna("").to_dict(orient="records")

        # final safety: if column names are still empty, give them a fallback: col_0, col_1...
        if any((not c) for c in df.columns):
            final = {}
            for idx, row in enumerate(sample_records):
                new_row = {}
                for i, (k, v) in enumerate(row.items()):
                    key = k if k else f"col_{i}"
                    new_row[key] = v
                sample_records[idx] = new_row

    else:
        # fallback: read text
        try:
            raw_text = p.read_text(encoding="utf-8")
        except Exception:
            raw_text = p.read_text(encoding="latin-1", errors="replace")
        sample_records = [{"raw_line": raw_text[:200]}]

    return {
        "transport": "sftp",
        "sample_records": sample_records,
        "raw_text": raw_text,
        "metadata": {"filename": str(p)}
    }
