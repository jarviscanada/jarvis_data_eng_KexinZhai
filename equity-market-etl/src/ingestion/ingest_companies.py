from __future__ import annotations

import os
import csv
from typing import Dict, List, Tuple

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


def _assert_required_header(header: List[str]) -> None:
    header_norm = {_norm_col(c) for c in header}

    # ticker required (symbol fallback accepted)
    if ("ticker" not in header_norm) and ("symbol" not in header_norm):
        raise DataQualityError("companies.csv must contain 'ticker' (or 'symbol' as fallback) in header")

    # per your current bronze_checks requirement
    required = ["company name", "sector", "industry"]
    missing = [c for c in required if _norm_col(c) not in header_norm]
    if missing:
        raise DataQualityError(f"companies.csv missing required columns in header: {missing}")


def ingest_companies_to_bronze() -> None:
    """
    Raw -> Bronze (companies):
    - only structural validation (file exists, non-empty, header ok, required columns exist)
    - allow ticker fallback from 'symbol'
    - do NOT do business cleaning here (leave to bronze_to_silver)
    """
    p = Paths()
    spark = get_spark()

    try:
        if not os.path.exists(p.RAW_COMPANIES_CSV):
            raise DataQualityError(f"companies.csv not found: {p.RAW_COMPANIES_CSV}")
        if os.path.getsize(p.RAW_COMPANIES_CSV) <= 0:
            raise DataQualityError("companies.csv is empty")

        header, has_row = _read_header_and_has_data_row(p.RAW_COMPANIES_CSV)
        if not header:
            raise DataQualityError("companies.csv missing header")
        if not has_row:
            raise DataQualityError("companies.csv has no data rows")

        _assert_required_header(header)

        df = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .csv(p.RAW_COMPANIES_CSV)
        )

        # Map normalized -> actual column name
        norm_to_actual: Dict[str, str] = {_norm_col(c): c for c in df.columns}

        # Ensure we have a 'ticker' column (rename symbol -> ticker if needed)
        if "ticker" in norm_to_actual:
            actual = norm_to_actual["ticker"]
            if actual != "ticker":
                df = df.withColumnRenamed(actual, "ticker")
        elif "symbol" in norm_to_actual:
            df = df.withColumnRenamed(norm_to_actual["symbol"], "ticker")
        else:
            # should never happen because we validated header
            raise DataQualityError("companies.csv missing ticker/symbol after Spark read (unexpected)")

        df.write.mode("overwrite").parquet(p.BRONZE_COMPANIES_DIR)

    finally:
        spark.stop()


if __name__ == "__main__":
    ingest_companies_to_bronze()
