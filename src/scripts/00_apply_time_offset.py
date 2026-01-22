# src/scripts/00_apply_time_offset.py
"""
Apply a permanent timestamp offset to a raw Report History CSV and write a processed CSV.

Your rule:
- Report times are +2h vs chart time (e.g., 13:00 in CSV = 11:00 on chart)
- So we apply offset_hours = -2
- Apply consistently to OpenTime + CloseTime (and any common variants)

Input : <PROJECT_ROOT>/data/raw/<your_file>.csv
Output: <PROJECT_ROOT>/data/processed/<stem>__offset-2h.csv

Notes:
- Raw stays untouched.
- Processed overwrites the timestamp columns to be "chart time".
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import timedelta
import re
import json

import pandas as pd


# ====== CONFIG (only change these) ======
RAW_CSV_REL = Path("data/raw/ReportHistory-52651106.csv")
OFFSET_HOURS = -2  # report is +2h vs chart, so subtract 2h
# =======================================


@dataclass
class RunMeta:
    input_file: str
    output_file: str
    offset_hours: int
    rows_in: int
    rows_out: int
    columns: list[str]
    adjusted_columns: list[str]


def _project_root() -> Path:
    """
    This script is at: <root>/src/scripts/00_apply_time_offset.py
    So project root is 2 parents up from this file's folder:
      src/scripts -> src -> <root>
    """
    return Path(__file__).resolve().parents[2]


def _ensure_dirs(root: Path) -> None:
    (root / "data/raw").mkdir(parents=True, exist_ok=True)
    (root / "data/processed").mkdir(parents=True, exist_ok=True)


def _extract_account_id_from_filename(p: Path) -> str | None:
    """
    Extract acct id from names like: ReportHistory-52652352.csv
    Returns None if not found.
    """
    m = re.search(r"(\d{5,})", p.stem)
    return m.group(1) if m else None


def _find_time_columns(df: pd.DataFrame) -> list[str]:
    """
    Prefer exact expected columns. Fall back to common variants.
    """
    candidates_priority = [
        "OpenTime",
        "CloseTime",
        "Open Time",
        "Close Time",
        "open_time",
        "close_time",
        "opentime",
        "closetime",
    ]

    cols_lower = {c.lower(): c for c in df.columns}
    found: list[str] = []

    for c in candidates_priority:
        key = c.lower()
        if key in cols_lower:
            found.append(cols_lower[key])

    # De-dup while preserving order
    seen = set()
    found = [c for c in found if not (c in seen or seen.add(c))]
    return found


def _parse_and_shift_datetime_series(
    s: pd.Series,
    offset_hours: int,
    fmt: str = "%Y.%m.%d %H:%M:%S",
) -> tuple[pd.Series, bool]:
    """
    Parse MT-style timestamps (YYYY.MM.DD HH:MM:SS),
    shift by offset_hours, and return in SAME format.
    """
    try:
        dt = pd.to_datetime(s, format=fmt, errors="coerce")
    except Exception:
        return s, False

    if dt.notna().sum() == 0:
        return s, False

    dt = dt + timedelta(hours=offset_hours)

    # IMPORTANT: return in ORIGINAL format (with seconds + dots)
    return dt.dt.strftime(fmt), True


def main() -> None:
    root = _project_root()
    _ensure_dirs(root)

    raw_path = (root / RAW_CSV_REL).resolve()

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Raw CSV not found: {raw_path}\n"
            f"Tip: confirm your project root is {root} and the file is in data/raw/"
        )

    df = pd.read_csv(raw_path)

    time_cols = _find_time_columns(df)
    if not time_cols:
        raise ValueError(
            f"Couldn't find OpenTime/CloseTime columns in {raw_path.name}. "
            f"Available columns: {list(df.columns)}"
        )

    adjusted_cols: list[str] = []
    for col in time_cols:
        shifted, ok = _parse_and_shift_datetime_series(df[col], OFFSET_HOURS)
        if ok:
            df[col] = shifted
            adjusted_cols.append(col)

    if not adjusted_cols:
        raise ValueError(
            f"Found candidate time columns {time_cols}, but none parsed as datetimes. "
            f"Check your CSV timestamp format in OpenTime/CloseTime."
        )

    acct = _extract_account_id_from_filename(raw_path) or "unknown"
    out_name = f"{raw_path.stem}__acct-{acct}__offset{OFFSET_HOURS:+d}h.csv"
    out_path = (root / "data/processed" / out_name).resolve()

    df.to_csv(out_path, index=False)

    meta = RunMeta(
        input_file=str(raw_path),
        output_file=str(out_path),
        offset_hours=OFFSET_HOURS,
        rows_in=int(len(df)),
        rows_out=int(len(df)),
        columns=list(df.columns),
        adjusted_columns=adjusted_cols,
    )

    meta_path = out_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta.__dict__, indent=2), encoding="utf-8")

    print("[OK] Time offset applied and saved.")
    print(f"Project root     : {root}")
    print(f"Input (raw)      : {raw_path}")
    print(f"Output (processed): {out_path}")
    print(f"Adjusted columns : {adjusted_cols}")
    print(f"Meta             : {meta_path}")


if __name__ == "__main__":
    main()
