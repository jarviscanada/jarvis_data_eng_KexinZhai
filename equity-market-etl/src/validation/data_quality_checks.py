from __future__ import annotations

import os
import math
from typing import List, Sequence, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, FloatType, DateType, TimestampType, NumericType, StringType
from src.utils.spark_session import get_spark
from src.utils.config import Paths
import exchange_calendars as xcals


class DataQualityError(RuntimeError):
    pass





def _fail_if(df: DataFrame, condition, dataset_name: str, rule: str, detail: str) -> None:
    bad = df.filter(condition).limit(1).count()
    if bad > 0:
        raise DataQualityError(f"[{dataset_name}] Rule={rule}. {detail}")
    

def _assert_non_empty(df: DataFrame, dataset_name: str) -> None:
    if df.limit(1).count() == 0:
        raise DataQualityError(f"[{dataset_name}] Rule=non_empty. No rows found")
    

def _norm_name(s: str) -> str:
    return " ".join(str(s).strip().lower().replace("_", " ").split())


def _require_columns(df: DataFrame, required: Sequence[str], dataset_name: str) -> None:
    cols = set(_norm_name(c) for c in df.columns)
    missing = [c for c in required if _norm_name(c) not in cols]
    if missing:
        raise DataQualityError(f"[{dataset_name}] Rule=required_columns. Missing={missing}")
    


def _col_map(df: DataFrame) -> Dict[str, str]:
    cmap: Dict[str, str] = {}
    for c in df.columns:
        k = _norm_name(c)
        if k in cmap and cmap[k] != c:
            raise DataQualityError(
                f"[unknown_dataset] Rule=ambiguous_columns. Normalized={k} Columns=({cmap[k]},{c})"
            )
        cmap[k] = c
    return cmap


def _resolve(df: DataFrame, name: str) -> str:
    cmap = _col_map(df)
    key = _norm_name(name)
    if key not in cmap:
        raise DataQualityError(f"[unknown_dataset] Rule=resolve_column. Missing column={name}")
    return cmap[key]


def _is_null_or_invalid(df: DataFrame, col_name: str):
    actual = _resolve(df, col_name)
    dtype = next(f.dataType for f in df.schema.fields if f.name == actual)
    c = F.col(actual)

    if isinstance(dtype, (DoubleType, FloatType)):
        return c.isNull() | F.isnan(c)
    if isinstance(dtype, NumericType):
        return c.isNull()
    if isinstance(dtype, StringType):
        return c.isNull() | (F.trim(c) == "")
    return c.isNull()

def _assert_not_null(df: DataFrame, cols: Sequence[str], dataset_name: str) -> None:
    for c in cols:
        _fail_if(
            df,
            _is_null_or_invalid(df, c),
            dataset_name,
            rule="not_null",
            detail=f"Column={c}",
        )

def _assert_unique_keys(df: DataFrame, keys: Sequence[str], dataset_name: str) -> None:
    actual_keys = [_resolve(df, k) for k in keys]
    dup = (
        df.groupBy(*[F.col(k) for k in actual_keys])
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if dup > 0:
        raise DataQualityError(f"[{dataset_name}] Rule=unique_keys. Keys={list(keys)}")


def _assert_date_type(df: DataFrame, dataset_name: str, date_col: str) -> None:
    actual = _resolve(df, date_col)
    dtype = next(f.dataType for f in df.schema.fields if f.name == actual)
    if not isinstance(dtype, (DateType, TimestampType)):
        raise DataQualityError(f"[{dataset_name}] Rule=date_type. Column={date_col} Type={dtype}")


def _assert_non_negative(
    df: DataFrame,
    cols: Sequence[str],
    dataset_name: str,
) -> None:
    for c in cols:
        actual = _resolve(df, c)
        _fail_if(
            df,
            F.col(actual) < 0,
            dataset_name,
            rule="non_negative",
            detail=f"Column={c}",
        )


def _assert_positive(
    df: DataFrame,
    cols: Sequence[str],
    dataset_name: str,
) -> None:
    for c in cols:
        actual = _resolve(df, c)
        _fail_if(
            df,
            F.col(actual) <= 0,
            dataset_name,
            rule="positive",
            detail=f"Column={c}",
        )


def _assert_high_ge_low(df: DataFrame, dataset_name: str, high_col: str, low_col: str) -> None:
    high_actual = _resolve(df, high_col)
    low_actual = _resolve(df, low_col)
    _fail_if(
        df,
        F.col(high_actual) < F.col(low_actual),
        dataset_name,
        rule="high_ge_low",
        detail=f"Columns=({high_col},{low_col})",
    )


def _assert_prices_have_companies(prices: DataFrame, companies: DataFrame, dataset_name: str) -> None:
    pt = _resolve(prices, "ticker")
    ct = _resolve(companies, "ticker")

    missing = (
        prices.select(F.col(pt).alias("ticker"))
        .distinct()
        .join(companies.select(F.col(ct).alias("ticker")).distinct(), on="ticker", how="left_anti")
        .limit(1)
        .count()
    )
    if missing > 0:
        raise DataQualityError(f"[{dataset_name}] Rule=referential_integrity. Column=ticker MissingIn=companies")


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return float(v)




def bronze_checks() -> None:
    """
    Structural checks on bronze (can it be processed?).
    Bronze should be "raw-as-is" converted to parquet, so only structural checks here.
    """
    p = Paths()
    spark = get_spark()
    try:
        prices = spark.read.parquet(p.BRONZE_PRICES_DIR)
        companies = spark.read.parquet(p.BRONZE_COMPANIES_DIR)

        _col_map(prices)
        _col_map(companies)

        _require_columns(
            prices,
            ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "ticker"],
            "bronze_prices",
        )
        _require_columns(
            companies,
            ["ticker", "company name", "sector", "industry"],
            "bronze_companies",
        )

        # Basic emptiness checks
        _assert_non_empty(prices, "bronze_prices")
        _assert_non_empty(companies, "bronze_companies")


        # Core key fields should exist (not null) so downstream can work
        _assert_not_null(prices, ["Date", "ticker"], "bronze_prices")
        _assert_not_null(companies, ["ticker"], "bronze_companies")


    finally:
        spark.stop()



def silver_checks() -> None:
    """
    Trustworthiness checks on silver outputs (post-cleaning, bronze->silver result).

    Silver invariants (must hold):
    - schema + non-empty for prices & companies
    - core null/type checks
    - financial correctness on remaining rows:
        * close, adj_close, volume must be > 0 (and non-null)
        * open/high/low must be >= 0 if present
        * high >= low if both present
    - uniqueness: (ticker, date) unique
    - referential integrity: every prices.ticker exists in companies
    - NO remaining "bad rows" for:
        * basic rules (invalid prices/volume/high<low/negative)
        * outliers (return spike, volume jump, volume level)
      (Transform should have quarantined these rows already; if they appear here => fail.)
    - trading-day completeness (tolerant, exchange-calendar based):
        missing = expected_sessions - actual_distinct_dates
        allowed_missing = max(TRADING_MAX_MISSING_DAYS_ABS, ceil(expected_sessions * TRADING_MAX_MISSING_RATIO))
        fail if missing > allowed_missing
      OTC: weak completeness only (min distinct dates).
    """

    p = Paths()
    spark = get_spark()
    try:
        prices = spark.read.parquet(p.SILVER_PRICES_DIR)
        companies = spark.read.parquet(p.SILVER_COMPANIES_DIR)

        # -----------------------
        # Schema / emptiness
        # -----------------------
        _require_columns(
            prices,
            ["date", "open", "high", "low", "close", "adj_close", "volume", "ticker"],
            "silver_prices",
        )
        _require_columns(
            companies,
            ["ticker", "company_name", "sector", "industry"],
            "silver_companies",
        )

        _assert_non_empty(prices, "silver_prices")
        _assert_non_empty(companies, "silver_companies")

        # -----------------------
        # Core null / type checks
        # -----------------------
        _assert_not_null(prices, ["date", "ticker", "close", "adj_close", "volume"], "silver_prices")
        _assert_date_type(prices, "silver_prices", "date")

        _assert_unique_keys(prices, ["ticker", "date"], "silver_prices")
        _assert_unique_keys(companies, ["ticker"], "silver_companies")
        _assert_not_null(companies, ["ticker", "company_name"], "silver_companies")

        # Referential integrity
        _assert_prices_have_companies(prices, companies, "silver_prices")

        # -----------------------
        # Basic financial correctness (row-level must be clean)
        # -----------------------
        _assert_positive(prices, ["close", "adj_close", "volume"], "silver_prices")
        _assert_non_negative(prices, ["open", "high", "low"], "silver_prices")
        _assert_high_ge_low(prices, "silver_prices", "high", "low")

        # Extra: mirror transform's basic bad-row predicate (must be zero rows)
        basic_bad = (
            F.col("close").isNull() | (F.col("close") <= 0)
            | F.col("adj_close").isNull() | (F.col("adj_close") <= 0)
            | F.col("volume").isNull() | (F.col("volume") <= 0)
            | (F.col("open").isNotNull() & (F.col("open") < 0))
            | (F.col("high").isNotNull() & (F.col("high") < 0))
            | (F.col("low").isNotNull() & (F.col("low") < 0))
            | (
                F.col("high").isNotNull()
                & F.col("low").isNotNull()
                & (F.col("high") < F.col("low"))
            )
        )
        _fail_if(
            prices,
            basic_bad,
            "silver_prices",
            "basic_rules_bad_row_remaining",
            "Basic invalid row exists in silver (transform should have quarantined it)",
        )


        # -----------------------
        # Outliers (single-pass aligned with transform)
        # We do NOT recompute lag-based outliers on the cleaned table (chain effect).
        # Instead, we verify that any rows flagged by the transform's outlier quarantine
        # (ticker,date) do NOT remain in silver.
        # -----------------------
        audit_dir = f"{p.SILVER_PRICES_DIR}_audit"

        # Support both new single-pass name and old multi-pass names (in case you still have old runs)
        candidate_dirs = [
            "bad_rows_outliers",
            "bad_rows_outliers_pass1",
            "bad_rows_outliers_pass2",
            "bad_rows_outliers_pass3",
        ]

        outlier_keys = None
        for d in candidate_dirs:
            path = f"{audit_dir}/{d}"
            try:
                dfk = spark.read.parquet(path).select("ticker", "date").distinct()
                outlier_keys = dfk if outlier_keys is None else outlier_keys.unionByName(dfk)
            except Exception as e:
                msg = str(e).lower()
                # path missing => means transform had no outliers for that pass/name; that's OK
                if ("path does not exist" in msg) or ("no such file" in msg) or ("does not exist" in msg):
                    continue
                # other errors are real errors
                raise

        if outlier_keys is not None:
            outlier_keys = outlier_keys.distinct()

            remaining = prices.join(F.broadcast(outlier_keys), on=["ticker", "date"], how="inner")
            if remaining.limit(1).count() > 0:
                ex = (
                    remaining.select("ticker", "date")
                    .orderBy("ticker", "date")
                    .limit(10)
                    .collect()
                )
                examples = [(r["ticker"], str(r["date"])) for r in ex]
                raise DataQualityError(
                    f"[silver_prices] Rule=outlier_rows_remaining_after_quarantine. Examples={examples}"
                )



        # -----------------------
        # Trading-day completeness (tolerant; aligns with transform)
        # -----------------------
        abs_cap = int(os.getenv("TRADING_MAX_MISSING_DAYS_ABS", "30"))
        ratio_cap = float(os.getenv("TRADING_MAX_MISSING_RATIO", "0.02"))
        lookback_days = int(os.getenv("TRADING_DAY_LOOKBACK_DAYS", "3650"))  # 10y default
        otc_min_dates = int(os.getenv("OTC_MIN_DISTINCT_DATES", "2"))

        # calendar name mapping
        try:
            xcals.get_calendar("XNAS")
            NASDAQ_CAL = "XNAS"
        except Exception:
            NASDAQ_CAL = "NASDAQ"

        if "exchange" in companies.columns:
            comp_ex = companies.select(
                F.col("ticker").alias("ticker"),
                F.lower(F.trim(F.col("exchange").cast("string"))).alias("exchange_norm"),
            )
        else:
            comp_ex = companies.select(
                F.col("ticker").alias("ticker"),
                F.lit(None).cast("string").alias("exchange_norm"),
            )


        # IMPORTANT: transform computes this on prices_after_basic (pre-outlier),
        # so checks must NOT treat outlier-removed dates as "missing".
        # We reconstruct the pre-outlier date set by unioning back outlier keys from audit.

        prices_dates = prices.select("ticker", "date").distinct()

        if outlier_keys is not None:
            # only keep outlier keys for tickers that still exist in silver
            present_tickers = prices.select("ticker").distinct()
            outlier_dates = (
                outlier_keys.select("ticker", "date").distinct()
                .join(F.broadcast(present_tickers), on="ticker", how="inner")
            )
            effective_dates = prices_dates.unionByName(outlier_dates).distinct()
        else:
            effective_dates = prices_dates

        prices_ex = (
            effective_dates
            .join(F.broadcast(comp_ex), on="ticker", how="left")
        )


        cal_name_col = (
            F.when(F.col("exchange_norm").isNotNull() & F.col("exchange_norm").contains("otc"), F.lit(None))
            .when(F.col("exchange_norm").isNotNull() & F.col("exchange_norm").contains("nasdaq"), F.lit(NASDAQ_CAL))
            .otherwise(F.lit("XNYS"))
        )
        prices_ex = prices_ex.withColumn("cal_name", cal_name_col)

        # lookback window per ticker
        mx = prices_ex.groupBy("ticker").agg(F.max("date").alias("_mx"))
        prices_ex = (
            prices_ex.join(mx, on="ticker", how="left")
            .withColumn("_cutoff", F.date_sub(F.col("_mx"), lookback_days))
            .filter(F.col("date") >= F.col("_cutoff"))
            .drop("_mx", "_cutoff")
        )

        # OTC weak check
        otc_bad = (
            prices_ex.filter(F.col("cal_name").isNull())
            .groupBy("ticker")
            .agg(F.count(F.lit(1)).alias("n_dates"))
            .filter(F.col("n_dates") < F.lit(otc_min_dates))
            .limit(10)
            .collect()
        )
        if otc_bad:
            examples = [(r["ticker"], int(r["n_dates"])) for r in otc_bad]
            raise DataQualityError(
                f"[silver_prices] Rule=otc_weak_completeness. MinDistinctDates={otc_min_dates} Examples={examples}"
            )

        # Non-OTC tolerant check (python loop due to exchange_calendars)
        stats = (
            prices_ex.filter(F.col("cal_name").isNotNull())
            .groupBy("cal_name", "ticker")
            .agg(
                F.min("date").alias("min_date"),
                F.max("date").alias("max_date"),
                F.count(F.lit(1)).alias("n_dates"),
            )
            .collect()
        )

        cal_cache = {}
        bad_examples = []

        for r in stats:
            cal_name = r["cal_name"]
            if cal_name not in cal_cache:
                cal_cache[cal_name] = xcals.get_calendar(cal_name)
            cal = cal_cache[cal_name]

            ticker = r["ticker"]
            min_d = r["min_date"]
            max_d = r["max_date"]
            actual = int(r["n_dates"]) if r["n_dates"] is not None else 0

            if min_d is None or max_d is None:
                bad_examples.append((ticker, cal_name, "missing_min_or_max"))
                if len(bad_examples) >= 10:
                    break
                continue

            first_sess = cal.first_session.date()
            last_sess = cal.last_session.date()

            if max_d < first_sess or min_d > last_sess:
                bad_examples.append((ticker, cal_name, "outside_calendar_bounds", str(min_d), str(max_d)))
                if len(bad_examples) >= 10:
                    break
                continue

            start_d = max(min_d, first_sess)
            end_d = min(max_d, last_sess)
            if start_d > end_d:
                bad_examples.append((ticker, cal_name, "invalid_clamped_range", str(min_d), str(max_d)))
                if len(bad_examples) >= 10:
                    break
                continue

            expected = len(cal.sessions_in_range(start_d.isoformat(), end_d.isoformat()))
            missing = max(0, expected - actual)
            allowed = max(abs_cap, int(math.ceil(expected * ratio_cap)))

            if missing > allowed:
                bad_examples.append((ticker, cal_name, actual, expected, missing, allowed, str(start_d), str(end_d)))
                if len(bad_examples) >= 10:
                    break

        if bad_examples:
            raise DataQualityError(
                f"[silver_prices] Rule=missing_trading_days_tolerant. "
                f"AbsCap={abs_cap} RatioCap={ratio_cap} LookbackDays={lookback_days} Examples={bad_examples}"
            )

    finally:
        spark.stop()


def gold_checks() -> None:
    """
    Analytical reliability checks on gold outputs:
    - enriched has required columns
    - join success (company_name not null)
    - instrument_type / label_quality consistency (should not contradict transform)
    - daily_return valid (computable, not exploding)
    - rolling metrics valid after window
    - no systematic data loss after join
    - gold analytics outputs exist and satisfy sample-size guarantees (or are empty by design)
    """
    p = Paths()
    spark = get_spark()
    try:
        enriched = spark.read.parquet(p.GOLD_ENRICHED_DIR)

        _assert_non_empty(enriched, "gold_prices_enriched")

        # --- Required columns (updated for new transform outputs) ---
        _require_columns(
            enriched,
            [
                "ticker","date","adj_close","company_name","sector","industry",
                "daily_return","daily_return_quality",
                "rolling_avg_return","rolling_vol_return",
                "is_etf","etf_scope","etf_hint","instrument_type","label_quality",
            ],
            "gold_prices_enriched",
        )

        _assert_not_null(enriched, ["ticker", "date", "adj_close"], "gold_prices_enriched")
        _assert_unique_keys(enriched, ["ticker", "date"], "gold_prices_enriched")

        # Normalize blanks to null (defensive, align to transform)
        enriched = (
            enriched
            .withColumn("sector", F.when(F.trim(F.col("sector").cast("string")) == F.lit(""), F.lit(None)).otherwise(F.col("sector")))
            .withColumn("industry", F.when(F.trim(F.col("industry").cast("string")) == F.lit(""), F.lit(None)).otherwise(F.col("industry")))
        )

        # -----------------------
        # 1) Join success (hard)
        # -----------------------
        _fail_if(
            enriched,
            F.col(_resolve(enriched, "company_name")).isNull(),
            "gold_prices_enriched",
            rule="join_success",
            detail="company_name is null (ticker not matched in companies)",
        )

        # -----------------------
        # 2) instrument_type / label_quality consistency (hard)
        # These should NEVER be violated if create_gold_enriched() is correct.
        # -----------------------
        sector_missing = F.col("sector").isNull()
        industry_missing = F.col("industry").isNull()

        # instrument_type vs is_etf consistency
        _fail_if(
            enriched,
            (F.col("instrument_type") == F.lit("etf")) & (F.col("is_etf") != F.lit(True)),
            "gold_prices_enriched",
            rule="instrument_type_etf_consistency",
            detail="instrument_type='etf' but is_etf != true",
        )
        _fail_if(
            enriched,
            (F.col("instrument_type").isin(["stock", "etf_suspect"])) & (F.col("is_etf") != F.lit(False)),
            "gold_prices_enriched",
            rule="instrument_type_stock_consistency",
            detail="instrument_type in {stock, etf_suspect} but is_etf != false",
        )

        # etf_suspect must match your definition: etf_hint=true AND sector/industry both missing
        _fail_if(
            enriched,
            (F.col("instrument_type") == F.lit("etf_suspect"))
            & (
                (F.col("etf_hint") != F.lit(True))
                | (~sector_missing)
                | (~industry_missing)
            ),
            "gold_prices_enriched",
            rule="etf_suspect_definition",
            detail="etf_suspect must have etf_hint=true and sector/industry both null",
        )

        # label_quality must match your definition
        _fail_if(
            enriched,
            (F.col("label_quality") == F.lit("missing_sector_industry"))
            & (
                (F.col("instrument_type") != F.lit("stock"))
                | (~sector_missing)
                | (~industry_missing)
            ),
            "gold_prices_enriched",
            rule="label_quality_definition",
            detail="label_quality='missing_sector_industry' must be stock with sector/industry both null",
        )

        # If stock AND (sector & industry both missing), label_quality MUST flag it (otherwise transform bug)
        _fail_if(
            enriched,
            (F.col("instrument_type") == F.lit("stock"))
            & sector_missing
            & industry_missing
            & (F.col("label_quality") != F.lit("missing_sector_industry")),
            "gold_prices_enriched",
            rule="label_quality_missed_flag",
            detail="stock with both labels missing must be labeled missing_sector_industry",
        )

        # -----------------------
        # 3) Sector/industry availability (audit-only, NO FAIL)
        # We no longer fail the pipeline on missing labels because transform intentionally quarantines via label_quality.
        # -----------------------
        audit_base = f"{p.GOLD_ENRICHED_DIR}_audit"

        bad_stock_labels_any = (
            enriched
            .filter((F.col("instrument_type") == F.lit("stock")) & (sector_missing | industry_missing))
            .select("ticker", "company_name", "exchange", "sector", "industry", "label_quality")
            .distinct()
        )
        if bad_stock_labels_any.limit(1).count() > 0:
            bad_stock_labels_any.limit(5000).write.mode("overwrite").parquet(
                f"{audit_base}/stocks_with_missing_labels_any"
            )

        # -----------------------
        # 4) Validate daily_return (hard, Scheme A compatible)
        # -----------------------
        MAX_ABS_DAILY_RETURN_GOLD = float(os.getenv("MAX_ABS_DAILY_RETURN_GOLD", "3.0"))
        MAX_GAP_DAYS = int(os.getenv("MAX_GAP_DAYS_FOR_DAILY_RETURN", "7"))

        w = Window.partitionBy("ticker").orderBy("date")
        enriched_w = (
            enriched
            .withColumn("_prev_adj", F.lag(F.col("adj_close"), 1).over(w))
            .withColumn("_prev_date", F.lag(F.col("date"), 1).over(w))
            .withColumn("_gap_days", F.datediff(F.col("date"), F.col("_prev_date")))
            .withColumn(
                "_is_adjacent",
                F.col("_prev_date").isNotNull()
                & (F.col("_gap_days") >= F.lit(1))
                & (F.col("_gap_days") <= F.lit(MAX_GAP_DAYS))
            )
        )

        # When we have an "opportunity" to compute returns (adjacent + positive prices)
        opp = (
            F.col("_is_adjacent")
            & (F.col("_prev_adj") > 0)
            & (F.col("adj_close") > 0)
        )

        # 4a) If quality is ok, daily_return must exist and be bounded
        _fail_if(
            enriched_w,
            opp
            & (F.col("daily_return_quality") == F.lit("ok"))
            & (F.col("daily_return").isNull() | F.isnan(F.col("daily_return"))),
            "gold_prices_enriched",
            rule="daily_return_required_ok",
            detail="daily_return is null/NaN but daily_return_quality='ok' under adjacent positive prices",
        )

        _fail_if(
            enriched_w,
            opp
            & (F.col("daily_return_quality") == F.lit("ok"))
            & (F.abs(F.col("daily_return")) > F.lit(MAX_ABS_DAILY_RETURN_GOLD)),
            "gold_prices_enriched",
            rule="daily_return_out_of_bounds_ok",
            detail=f"Threshold={MAX_ABS_DAILY_RETURN_GOLD} when daily_return_quality='ok'",
        )

        # 4b) If quality is outlier/missing, daily_return must be NULL (Scheme A contract)
        _fail_if(
            enriched_w,
            opp
            & (F.col("daily_return_quality").isin(["outlier", "missing"]))
            & (F.col("daily_return").isNotNull() | F.isnan(F.col("daily_return"))),
            "gold_prices_enriched",
            rule="daily_return_should_be_null_when_flagged",
            detail="daily_return must be NULL when daily_return_quality in {outlier, missing}",
        )


        # 4c) quality values sanity (hard)
        _fail_if(
            enriched_w,
            opp
            & (~F.col("daily_return_quality").isin(["ok", "outlier", "missing"])),
            "gold_prices_enriched",
            rule="daily_return_quality_invalid_value",
            detail="daily_return_quality must be one of {ok, outlier, missing} under opportunity rows",
        )


        # -----------------------
        # 5) Rolling metrics validity (hard)
        # -----------------------
        ROLL_N = int(os.getenv("ROLLING_WINDOW_DAYS", "20"))
        MIN_ROLL_OBS = int(os.getenv("MIN_ROLLING_RETURN_OBS", str(ROLL_N)))

        w_roll = Window.partitionBy("ticker").orderBy("date").rowsBetween(-(ROLL_N - 1), 0)

        enriched_w = enriched_w.withColumn("_roll_n_ret", F.count("daily_return").over(w_roll))

        _fail_if(
            enriched_w,
            (F.col("_roll_n_ret") >= F.lit(MIN_ROLL_OBS))
            & (F.col("rolling_avg_return").isNull() | F.isnan(F.col("rolling_avg_return"))),
            "gold_prices_enriched",
            rule="rolling_avg_missing",
            detail=f"rolling_avg_return is null/NaN when _roll_n_ret >= {MIN_ROLL_OBS}",
        )

        MIN_VOL_OBS = max(MIN_ROLL_OBS, 2)

        _fail_if(
            enriched_w,
            (F.col("_roll_n_ret") >= F.lit(MIN_VOL_OBS))
            & (F.col("rolling_vol_return").isNull() | F.isnan(F.col("rolling_vol_return"))),
            "gold_prices_enriched",
            rule="rolling_vol_missing",
            detail=f"rolling_vol_return is null/NaN when _roll_n_ret >= {MIN_VOL_OBS}",
        )


       # -----------------------
        # 6) No systematic data loss after join (hard, Scheme A aware)
        # Allow loss ONLY for intentionally dropped tickers recorded in audit.
        # -----------------------
        silver_prices = spark.read.parquet(p.SILVER_PRICES_DIR)

        sp_t = _resolve(silver_prices, "ticker")
        sp_d = _resolve(silver_prices, "date")
        ge_t = _resolve(enriched, "ticker")
        ge_d = _resolve(enriched, "date")

        silver_keys = (
            silver_prices.select(
                F.col(sp_t).alias("ticker"),
                F.col(sp_d).cast("string").alias("date"),
            )
            .distinct()
        )

        gold_keys = (
            enriched.select(
                F.col(ge_t).alias("ticker"),
                F.col(ge_d).cast("string").alias("date"),
            )
            .distinct()
        )

        # If drop file exists, exclude those tickers from the loss check
        audit_base = f"{p.GOLD_ENRICHED_DIR}_audit"
        dropped_path = f"{audit_base}/dropped_tickers_bad_returns"

        if os.path.exists(dropped_path):
            dropped = spark.read.parquet(dropped_path).select("ticker").distinct()
            silver_keys_chk = silver_keys.join(dropped, on="ticker", how="left_anti")
        else:
            silver_keys_chk = silver_keys

        lost_cnt = silver_keys_chk.join(gold_keys, on=["ticker", "date"], how="left_anti").limit(1).count()
        if lost_cnt > 0:
            raise DataQualityError("[gold_prices_enriched] Rule=join_data_loss. Keys=(ticker,date) not explained by dropped_tickers_bad_returns")

        # -----------------------
        # 7) Gold analytics outputs checks
        # - Must exist (hard)
        # - Sample-size guarantee should already hold due to transform filters
        # - If empty, we DO NOT fail (audit-only), because small datasets can produce empty after min_group_n filtering.
        # -----------------------
        min_group_n = int(os.getenv("MIN_GROUP_SAMPLE_SIZE", "5"))

        def _pick_existing_path(base_dir: str, name: str) -> str:
            candidates = [
                f"{base_dir}/{name}",
                f"{base_dir}/{name}.parquet",
            ]
            for pth in candidates:
                if os.path.exists(pth):
                    return pth
            raise DataQualityError(
                f"[gold_analytics] Rule=missing_output. Expected={name} Candidates={candidates}"
            )

        sector_path = _pick_existing_path(p.GOLD_ANALYTICS_DIR, "sector_daily_returns")
        industry_path = _pick_existing_path(p.GOLD_ANALYTICS_DIR, "industry_volatility")

        # --- sector_daily_returns ---
        sector_df = spark.read.parquet(sector_path)

        _require_columns(
            sector_df,
            ["date", "sector", "avg_daily_return", "n_tickers"],
            "sector_daily_returns",
        )

        if sector_df.limit(1).count() == 0:
            # audit-only
            spark.createDataFrame(
                [("sector_daily_returns_empty", sector_path, min_group_n)],
                ["note", "path", "min_group_n"],
            ).write.mode("overwrite").parquet(f"{audit_base}/sector_daily_returns_empty_note")
        else:
            # if non-empty, it should satisfy min_group_n (transform already filtered)
            _fail_if(
                sector_df,
                F.col("n_tickers") < F.lit(min_group_n),
                "sector_daily_returns",
                rule="sample_size",
                detail=f"MinN={min_group_n} Column=n_tickers",
            )

        # --- industry_volatility ---
        industry_df = spark.read.parquet(industry_path)

        _require_columns(
            industry_df,
            ["date", "industry", "avg_volatility", "n_tickers"],
            "industry_volatility",
        )

        if industry_df.limit(1).count() == 0:
            # audit-only
            spark.createDataFrame(
                [("industry_volatility_empty", industry_path, min_group_n)],
                ["note", "path", "min_group_n"],
            ).write.mode("overwrite").parquet(f"{audit_base}/industry_volatility_empty_note")
        else:
            _fail_if(
                industry_df,
                F.col("n_tickers") < F.lit(min_group_n),
                "industry_volatility",
                rule="sample_size",
                detail=f"MinN={min_group_n} Column=n_tickers",
            )

    finally:
        spark.stop()
