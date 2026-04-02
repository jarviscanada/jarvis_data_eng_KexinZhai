from __future__ import annotations

import os
import csv
from typing import List, Tuple, Dict

from pyspark.sql import functions as F
from src.utils.spark_session import get_spark
from src.utils.config import Paths


class DataQualityError(RuntimeError):
    pass


def _norm_col(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("_", " ")
    s = " ".join(s.split())
    return s


def _read_header_and_has_data_row(path: str) -> Tuple[List[str], bool]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None) or []
        has_row = False
        for row in reader:
            if row and any(str(x).strip() != "" for x in row):
                has_row = True
                break
        return header, has_row


def _list_csv_files(dir_path: str) -> List[str]:
    return sorted(
        os.path.join(dir_path, f)
        for f in os.listdir(dir_path)
        if f.lower().endswith(".csv")
    )


def _write_skipped_report(audit_dir: str, skipped: List[str]) -> None:
    os.makedirs(audit_dir, exist_ok=True)
    report_path = os.path.join(audit_dir, "skipped_prices_files.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        for line in skipped:
            f.write(line + "\n")
    print(f"[ingest_prices_to_bronze] Wrote skipped report: {report_path}")


def ingest_prices_to_bronze() -> None:
    p = Paths()
    spark = get_spark()

    try:
        if not os.path.exists(p.RAW_PRICES_DIR):
            raise ValueError(f"RAW prices dir not found: {p.RAW_PRICES_DIR}")

        files = sorted(_list_csv_files(p.RAW_PRICES_DIR))
        if not files:
            raise ValueError(f"No CSV files found under: {p.RAW_PRICES_DIR}")

        required_cols = {"date", "open", "high", "low", "close", "adj close", "volume"}

        valid: list[tuple[str, str]] = []   # (ticker, path)
        skipped: list[str] = []             # empty/no rows, etc (non-fatal per your requirement)
        fatal: list[str] = []               # structural problems -> must halt

        for path in files:
            base = os.path.basename(path)

            # empty file -> skip (your rule)
            if os.path.getsize(path) <= 0:
                skipped.append(f"{base}\tempty_file")
                continue

            stem = os.path.splitext(base)[0].strip()
            if stem == "":
                fatal.append(f"{base}\tbad_filename")
                continue
            ticker = stem.upper()

            try:
                header, has_row = _read_header_and_has_data_row(path)
            except Exception as e:
                fatal.append(f"{base}\tread_failed={type(e).__name__}")
                continue

            if not header:
                fatal.append(f"{base}\tmissing_header")
                continue

            if not has_row:
                skipped.append(f"{base}\tno_data_rows")
                continue

            cols = {_norm_col(c) for c in header}
            if not required_cols.issubset(cols):
                missing = sorted(required_cols - cols)
                fatal.append(f"{base}\tmissing_cols={missing}")
                continue

            valid.append((ticker, path))

        audit_dir = f"{p.BRONZE_PRICES_DIR}_audit"
        if skipped or fatal:
            _write_skipped_report(audit_dir, skipped + [f"FATAL\t{x}" for x in fatal])

        if fatal:
            raise ValueError(f"Malformed price files found. See audit: {audit_dir}")

        if not valid:
            raise ValueError(f"All price files were skipped. See audit: {audit_dir}")

        # enforce: one CSV per ticker -> fatal if duplicates exist
        seen: set[str] = set()
        unique_valid: list[tuple[str, str]] = []
        dup_fatal: list[str] = []
        for ticker, path in valid:
            if ticker in seen:
                dup_fatal.append(f"{os.path.basename(path)}\tduplicate_ticker_file={ticker}")
                continue
            seen.add(ticker)
            unique_valid.append((ticker, path))

        if dup_fatal:
            _write_skipped_report(audit_dir, skipped + [f"FATAL\t{x}" for x in dup_fatal])
            raise ValueError(f"Duplicate ticker files found. See audit: {audit_dir}")

        df_out = None
        for ticker, path in unique_valid:
            df = (
                spark.read.option("header", True)
                .option("inferSchema", False)  # bronze: schema-on-read
                .csv(path)
                .withColumn("ticker", F.lit(ticker))
            )
            df_out = df if df_out is None else df_out.unionByName(df, allowMissingColumns=True)

        (
            df_out.write.mode("overwrite")
            .partitionBy("ticker")
            .parquet(p.BRONZE_PRICES_DIR)
        )

        if skipped:
            print(f"[ingest_prices_to_bronze] Skipped {len(skipped)} files (see audit: {audit_dir}).")

    finally:
        spark.stop()






