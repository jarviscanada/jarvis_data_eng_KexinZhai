from __future__ import annotations

import os
import math
from typing import Dict, Sequence, List, Tuple

import exchange_calendars as xcals
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, StringType

from src.utils.spark_session import get_spark
from src.utils.config import Paths


# ---------------------------
# Helpers
# ---------------------------

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _norm_name(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("_", " ")
    s = " ".join(s.split())
    return s


def _col_map(df: DataFrame) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for c in df.columns:
        key = _norm_name(c)
        if key not in m:
            m[key] = c
    return m


def _resolve_col(df: DataFrame, candidates: Sequence[str], dataset: str) -> str:
    cmap = _col_map(df)
    for name in candidates:
        key = _norm_name(name)
        if key in cmap:
            return cmap[key]
    raise ValueError(f"[{dataset}] Missing required column. Candidates={list(candidates)} Columns={df.columns}")


def _has_any_col(df: DataFrame, candidates: Sequence[str]) -> bool:
    cmap = _col_map(df)
    return any(_norm_name(c) in cmap for c in candidates)


def _col_or_null(df: DataFrame, candidates: Sequence[str], dataset: str, cast_to: str = "string"):
    if _has_any_col(df, candidates):
        actual = _resolve_col(df, candidates, dataset)
        return F.col(actual)
    return F.lit(None).cast(cast_to)


def _null_if_blank(c):
    return F.when(F.trim(c) == "", F.lit(None)).otherwise(F.trim(c))


def _standardize_ticker(c):
    t = F.upper(F.trim(c.cast("string")))
    return F.when(t.isNull() | (t == ""), F.lit(None)).otherwise(t)


def _to_double(c):
    return F.regexp_replace(F.trim(c.cast(StringType())), ",", "").cast(DoubleType())


def _to_long(c):
    return F.regexp_replace(F.trim(c.cast(StringType())), ",", "").cast(LongType())


def _parse_date(c):
    s = F.trim(c.cast(StringType()))
    return F.coalesce(
        F.to_date(s, "yyyy-MM-dd"),
        F.to_date(s, "MM/dd/yyyy"),
        F.to_date(s, "yyyy/MM/dd"),
        F.to_date(s),  # fallback (Spark default)
    )


def _audit_write(df: DataFrame, path: str, limit_rows: int = 20000) -> None:
    df.limit(limit_rows).write.mode("overwrite").parquet(path)


def _empty_ticker_df(spark) -> DataFrame:
    return spark.createDataFrame([], "ticker string")


def _ticker_df(spark, tickers: List[str]) -> DataFrame:
    if not tickers:
        return _empty_ticker_df(spark)
    return spark.createDataFrame([(t,) for t in tickers], ["ticker"])


# ---------------------------
# Existing ticker-level hard drop: conflicting duplicates
# ---------------------------

def _conflicting_version_tickers(prices: DataFrame, audit_dir: str) -> DataFrame:
    """
    Identify tickers that have conflicting OHLCV versions for the same (ticker, date).
    Those tickers will be dropped (ticker-level exclusion).
    """
    stats = (
        prices.groupBy("ticker", "date")
        .agg(
            F.count(F.lit(1)).alias("n_rows"),
            F.countDistinct("open").alias("open_versions"),
            F.countDistinct("high").alias("high_versions"),
            F.countDistinct("low").alias("low_versions"),
            F.countDistinct("close").alias("close_versions"),
            F.countDistinct("adj_close").alias("adj_close_versions"),
            F.countDistinct("volume").alias("volume_versions"),
        )
        .filter(F.col("n_rows") > 1)
    )

    conflicts = stats.filter(
        (F.col("open_versions") > 1)
        | (F.col("high_versions") > 1)
        | (F.col("low_versions") > 1)
        | (F.col("close_versions") > 1)
        | (F.col("adj_close_versions") > 1)
        | (F.col("volume_versions") > 1)
    )

    if conflicts.limit(1).count() > 0:
        _audit_write(conflicts, f"{audit_dir}/conflicting_price_versions")

    return conflicts.select("ticker").distinct()


# ---------------------------
# NEW (1): Basic rules -> row-level quarantine + drop ticker only if ratio/abs too high
# Defaults per your spec:
#   BASIC_MAX_BAD_RATIO=0.01
#   BASIC_BAD_RATIO_MIN_OBS=100
#   BASIC_MAX_BAD_ROWS_ABS=20
# ---------------------------

def _apply_basic_rules_rowlevel(prices: DataFrame, audit_dir: str) -> tuple[DataFrame, DataFrame]:
    max_bad_ratio = float(os.getenv("BASIC_MAX_BAD_RATIO", "0.01"))
    min_obs_for_ratio = int(os.getenv("BASIC_BAD_RATIO_MIN_OBS", "100"))
    max_bad_abs = int(os.getenv("BASIC_MAX_BAD_ROWS_ABS", "20"))

    bad_row = (
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

    prices_w = prices.withColumn("_basic_bad", bad_row.cast("int"))

    bad_rows = prices_w.filter(F.col("_basic_bad") == 1)
    if bad_rows.limit(1).count() > 0:
        _audit_write(
            bad_rows.select("ticker", "date", "open", "high", "low", "close", "adj_close", "volume"),
            f"{audit_dir}/bad_rows_basic_rules",
        )

    stats = (
        prices_w.groupBy("ticker")
        .agg(
            F.count(F.lit(1)).alias("n_total"),
            F.sum(F.col("_basic_bad")).alias("n_bad"),
        )
        .withColumn("bad_ratio", F.col("n_bad") / F.col("n_total"))
    )

    drop_tickers = stats.filter(
        (F.col("n_bad") > F.lit(max_bad_abs))
        | (
            (F.col("n_total") >= F.lit(min_obs_for_ratio))
            & (F.col("bad_ratio") > F.lit(max_bad_ratio))
        )
    ).select("ticker").distinct()

    if drop_tickers.limit(1).count() > 0:
        _audit_write(stats.join(drop_tickers, on="ticker", how="inner"), f"{audit_dir}/bad_tickers_basic_rules_stats")
        drop_tickers.write.mode("overwrite").parquet(f"{audit_dir}/bad_tickers_basic_rules")

    prices_clean = prices_w.filter(F.col("_basic_bad") == 0).drop("_basic_bad")
    return prices_clean, drop_tickers


# ---------------------------
# NEW (2): Outliers -> row-level quarantine + drop ticker only if:
#   - max consecutive outliers >= OUTLIER_MAX_CONSECUTIVE (default 3)
#   - OR outlier_ratio > OUTLIER_MAX_RATIO (default 0.02) AND n_total >= OUTLIER_RATIO_MIN_OBS (default 100)
#   - OR n_outlier > OUTLIER_MAX_OUTLIER_ROWS_ABS (default 30)
# ---------------------------

# NEW helper: one outlier pass (same logic as your current function body)
def _apply_outliers_rowlevel(prices: DataFrame, audit_dir: str) -> tuple[DataFrame, DataFrame]:
    """
    Single-pass outlier quarantine:
    - Compute ALL outlier flags ONCE on the same base dataset (with lag/median computed from base)
    - Remove flagged rows (row-level quarantine)
    - Drop tickers only if consecutive/ratio/abs thresholds are violated (also computed from base)
    """
    max_abs_ret = _env_float("MAX_ABS_DAILY_RETURN_SILVER", 3.0)
    max_abs_log_vol_change = _env_float("MAX_ABS_LOG_VOL_CHANGE_SILVER", 5.0)
    vol_median_mult = _env_float("VOLUME_MEDIAN_MULT_SILVER", 50.0)
    min_median_volume = _env_float("MIN_MEDIAN_VOLUME_SILVER", 1000.0)

    max_consec = int(os.getenv("OUTLIER_MAX_CONSECUTIVE", "3"))
    max_ratio = float(os.getenv("OUTLIER_MAX_RATIO", "0.02"))
    min_obs_for_ratio = int(os.getenv("OUTLIER_RATIO_MIN_OBS", "100"))
    max_abs = int(os.getenv("OUTLIER_MAX_OUTLIER_ROWS_ABS", "30"))

    # ---- base snapshot: compute all outlier signals ONCE here ----
    w = Window.partitionBy("ticker").orderBy("date")
    max_gap_days = int(os.getenv("MAX_GAP_DAYS_FOR_OUTLIER", "7"))

    base = (
        prices
        .withColumn("_prev_date", F.lag(F.col("date"), 1).over(w))
        .withColumn("_gap_days", F.datediff(F.col("date"), F.col("_prev_date")))
        .withColumn(
            "_is_adjacent",
            F.col("_prev_date").isNotNull()
            & (F.col("_gap_days") >= F.lit(1))
            & (F.col("_gap_days") <= F.lit(max_gap_days))
        )
        .withColumn("_prev_adj", F.lag(F.col("adj_close"), 1).over(w))
        .withColumn("_prev_vol", F.lag(F.col("volume"), 1).over(w))
    )
    # return spike
    daily_ret = (F.col("adj_close") / F.col("_prev_adj")) - F.lit(1.0)
    bad_ret = (
        F.col("_is_adjacent")                    
        & F.col("_prev_adj").isNotNull()
        & (F.col("adj_close") > 0) & (F.col("_prev_adj") > 0)
        & (F.abs(daily_ret) > F.lit(max_abs_ret))
    )

    # volume jump
    log_vol_change = F.log(F.col("volume") / F.col("_prev_vol"))
    bad_vol_jump = (
        F.col("_is_adjacent")                      
        & F.col("_prev_vol").isNotNull()
        & (F.col("volume") > 0) & (F.col("_prev_vol") > 0)
        & (F.abs(log_vol_change) > F.lit(max_abs_log_vol_change))
    )


    # volume level vs median (median computed from the same base prices table)
    med_vol = prices.groupBy("ticker").agg(
        F.expr("percentile_approx(volume, 0.5)").alias("_median_volume")
    )
    base = base.join(F.broadcast(med_vol), on="ticker", how="left")

    bad_vol_level = (
        (F.col("_median_volume") >= F.lit(min_median_volume))
        & (F.col("volume") > F.col("_median_volume") * F.lit(vol_median_mult))
    )

    base = (
        base
        .withColumn("_daily_ret", daily_ret)
        .withColumn("_log_vol_change", log_vol_change)
        .withColumn("_bad_ret", bad_ret.cast("int"))
        .withColumn("_bad_vol_jump", bad_vol_jump.cast("int"))
        .withColumn("_bad_vol_level", bad_vol_level.cast("int"))
        .withColumn(
            "_outlier_bad",
            ((F.col("_bad_ret") == 1) | (F.col("_bad_vol_jump") == 1) | (F.col("_bad_vol_level") == 1)).cast("int"),
        )
    )

    # ---- audit bad rows (optional) ----
    bad_rows = base.filter(F.col("_outlier_bad") == 1)
    if bad_rows.limit(1).count() > 0:
        _audit_write(
            bad_rows.select(
                "ticker", "date",
                "adj_close", "_prev_adj", "_daily_ret",
                "volume", "_prev_vol", "_log_vol_change",
                "_median_volume", "_bad_ret", "_bad_vol_jump", "_bad_vol_level",
            ),
            f"{audit_dir}/bad_rows_outliers",
        )

    # ---- ticker-level thresholds (computed from base, single-pass) ----
    wN = Window.partitionBy("ticker").orderBy("date").rowsBetween(-(max_consec - 1), 0)
    base = base.withColumn("_runN", F.sum(F.col("_outlier_bad")).over(wN))

    per_ticker = (
        base.groupBy("ticker")
        .agg(
            F.count(F.lit(1)).alias("n_total"),
            F.sum(F.col("_outlier_bad")).alias("n_outlier"),
            F.max(F.col("_runN")).alias("max_runN"),
        )
        .withColumn("outlier_ratio", F.col("n_outlier") / F.col("n_total"))
    )

    drop_tickers = per_ticker.filter(
        (F.col("max_runN") >= F.lit(max_consec))
        | (F.col("n_outlier") > F.lit(max_abs))
        | (
            (F.col("n_total") >= F.lit(min_obs_for_ratio))
            & (F.col("outlier_ratio") > F.lit(max_ratio))
        )
    ).select("ticker").distinct()

    if drop_tickers.limit(1).count() > 0:
        _audit_write(
            per_ticker.join(drop_tickers, on="ticker", how="inner"),
            f"{audit_dir}/bad_tickers_outliers_stats",
        )
        drop_tickers.write.mode("overwrite").parquet(f"{audit_dir}/bad_tickers_outliers")

    # ---- row-level quarantine: remove outlier rows ONCE (do NOT re-compute lag after removal) ----
    outlier_keys = bad_rows.select("ticker", "date").distinct()

    cleaned = (
        prices
        .join(F.broadcast(outlier_keys), on=["ticker", "date"], how="left_anti")
    )

    return cleaned, drop_tickers



# ---------------------------
# NEW (3): Trading-day completeness tolerant
# Your defaults:
#   TRADING_MAX_MISSING_DAYS_ABS=30
#   TRADING_MAX_MISSING_RATIO=0.02
# Drop ticker only if:
#   missing > max(abs_cap, ceil(expected * ratio_cap))
# ---------------------------

def _bad_trading_day_tickers_tolerant(prices: DataFrame, companies: DataFrame, audit_dir: str, spark) -> DataFrame:
    abs_cap = int(os.getenv("TRADING_MAX_MISSING_DAYS_ABS", "30"))
    ratio_cap = float(os.getenv("TRADING_MAX_MISSING_RATIO", "0.02"))

    # Exchange -> calendar mapping (same spirit as your original)
    try:
        xcals.get_calendar("XNAS")
        nasdaq_cal = "XNAS"
    except Exception:
        nasdaq_cal = "NASDAQ"

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

    prices_ex = (
        prices.select("ticker", "date").distinct()
        .join(F.broadcast(comp_ex), on="ticker", how="left")
    )

    cal_name_col = (
        F.when(F.col("exchange_norm").isNotNull() & F.col("exchange_norm").contains("otc"), F.lit(None))
        .when(F.col("exchange_norm").isNotNull() & F.col("exchange_norm").contains("nasdaq"), F.lit(nasdaq_cal))
        .otherwise(F.lit("XNYS"))
    )
    prices_ex = prices_ex.withColumn("cal_name", cal_name_col)

    # Optional lookback window (keeps runtime + avoids calendar bound surprises)
    lookback_days = int(os.getenv("TRADING_DAY_LOOKBACK_DAYS", "3650"))  # 10y default
    ticker_max = prices_ex.groupBy("ticker").agg(F.max("date").alias("_mx"))
    prices_ex = (
        prices_ex.join(ticker_max, on="ticker", how="left")
        .withColumn("_cutoff", F.date_sub(F.col("_mx"), lookback_days))
        .filter(F.col("date") >= F.col("_cutoff"))
        .drop("_mx", "_cutoff")
    )

    # OTC: weak completeness only
    otc_min_dates = int(os.getenv("OTC_MIN_DISTINCT_DATES", "2"))
    otc_bad = (
        prices_ex.filter(F.col("cal_name").isNull())
        .groupBy("ticker")
        .agg(F.count(F.lit(1)).alias("n_dates"))
        .filter(F.col("n_dates") < F.lit(otc_min_dates))
    )
    if otc_bad.limit(1).count() > 0:
        _audit_write(otc_bad, f"{audit_dir}/bad_tickers_otc_weak_completeness")

    # Non-OTC tolerant check (python loop because exchange_calendars is python-side)
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
    bad_examples: List[Tuple[str, str, int, int, int, int, str, str]] = []
    bad_tickers: List[str] = []

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
            bad_tickers.append(ticker)
            continue

        first_sess = cal.first_session.date()
        last_sess = cal.last_session.date()

        # outside calendar coverage => mark bad (you can relax this if you want)
        if max_d < first_sess or min_d > last_sess:
            bad_tickers.append(ticker)
            continue

        start_d = max(min_d, first_sess)
        end_d = min(max_d, last_sess)
        if start_d > end_d:
            bad_tickers.append(ticker)
            continue

        expected = len(cal.sessions_in_range(start_d.isoformat(), end_d.isoformat()))
        missing = max(0, expected - actual)
        allowed = max(abs_cap, int(math.ceil(expected * ratio_cap)))

        if missing > allowed:
            bad_tickers.append(ticker)
            if len(bad_examples) < 50:
                bad_examples.append((ticker, cal_name, actual, expected, missing, allowed, str(start_d), str(end_d)))

    if bad_examples:
        df_ex = spark.createDataFrame(
            bad_examples, ["ticker", "cal_name", "actual", "expected", "missing", "allowed", "start", "end"]
        )
        _audit_write(df_ex, f"{audit_dir}/bad_tickers_missing_trading_days_examples")

    bad_strict_df = _ticker_df(spark, sorted(list(set(bad_tickers))))
    return otc_bad.select("ticker").distinct().unionByName(bad_strict_df.select("ticker").distinct()).distinct()


def _rm_path(spark, path: str) -> None:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    p = jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(p):
        fs.delete(p, True)




# ---------------------------
# Transformations
# ---------------------------

def transform_companies_bronze_to_silver() -> None:
    """
    Silver companies:
    - trim + empty->null
    - market_cap numeric
    - ticker uppercase
    - dedupe by ticker (deterministic)
    - light fill: sector <- coalesce(sector, tag1), industry <- coalesce(industry, tag2, tag3)
    """
    p = Paths()
    spark = get_spark()

    try:
        df = spark.read.parquet(p.BRONZE_COMPANIES_DIR)
        dataset = "bronze_companies"

        ticker_c = _resolve_col(df, ["ticker"], dataset=dataset)
        company_name_c = _resolve_col(df, ["company name", "company_name"], dataset=dataset)
        industry_c = _resolve_col(df, ["industry"], dataset=dataset)
        sector_c = _resolve_col(df, ["sector"], dataset=dataset)

        short_name = _col_or_null(df, ["short name", "short_name"], dataset=dataset, cast_to="string")
        website = _col_or_null(df, ["website"], dataset=dataset, cast_to="string")
        exchange = _col_or_null(df, ["exchange"], dataset=dataset, cast_to="string")
        market_cap = _col_or_null(df, ["market cap", "market_cap"], dataset=dataset, cast_to="string")
        description = _col_or_null(df, ["description"], dataset=dataset, cast_to="string")

        tag1 = _col_or_null(df, ["tag1"], dataset=dataset, cast_to="string")
        tag2 = _col_or_null(df, ["tag2"], dataset=dataset, cast_to="string")
        tag3 = _col_or_null(df, ["tag3"], dataset=dataset, cast_to="string")

        out = df.select(
            _standardize_ticker(F.col(ticker_c)).alias("ticker"),
            _null_if_blank(F.col(company_name_c)).alias("company_name"),
            _null_if_blank(short_name).alias("short_name"),
            _null_if_blank(F.col(industry_c)).alias("industry_raw"),
            _null_if_blank(F.col(sector_c)).alias("sector_raw"),
            _null_if_blank(exchange).alias("exchange"),
            _null_if_blank(website).alias("website"),
            _null_if_blank(description).alias("description"),
            # Convert market cap to numeric if it’s in scientific notation or string
            _to_double(market_cap).alias("market_cap"),
            _null_if_blank(tag1).alias("tag1"),
            _null_if_blank(tag2).alias("tag2"),
            _null_if_blank(tag3).alias("tag3"),
        ).filter(F.col("ticker").isNotNull())

        out = (
            out.withColumn("sector", F.coalesce(F.col("sector_raw"), F.col("tag1")))
            .withColumn("industry", F.coalesce(F.col("industry_raw"), F.col("tag2"), F.col("tag3")))
            .drop("sector_raw", "industry_raw")
            .withColumn("exchange", F.upper(F.col("exchange")))
        )

        quality = (
            F.when(F.col("company_name").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("sector").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("industry").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("market_cap").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("exchange").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("description").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        )

        w = Window.partitionBy("ticker").orderBy(
            F.desc(quality),
            F.col("market_cap").desc_nulls_last(),
            F.col("company_name").asc_nulls_last(),
        )

        out = (
            out.withColumn("_q", quality)
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_q", "_rn")
        )

        out.write.mode("overwrite").parquet(p.SILVER_COMPANIES_DIR)

    finally:
        spark.stop()


def transform_prices_bronze_to_silver() -> None:
    """
    Silver prices (bronze -> silver):

    What we do:
    1) Read bronze prices and standardize types + date normalization.
    2) Deterministic dedupe by (ticker, date) with "valid row" priority.
    3) Referential integrity: keep only tickers that exist in silver companies.
    4) Basic financial correctness:
       - Row-level quarantine: remove bad rows (e.g., volume/adj_close missing or <=0, high<low, negatives).
       - Ticker-level drop ONLY if bad rows are too frequent:
         * bad_ratio > BASIC_MAX_BAD_RATIO (default 0.01) AND n_total >= BASIC_BAD_RATIO_MIN_OBS (default 100)
         * OR n_bad > BASIC_MAX_BAD_ROWS_ABS (default 20)
    5) Outliers:
       - Row-level quarantine: remove outlier rows (return/volume jump/volume level).
       - Ticker-level drop ONLY if:
         * max_consecutive_outliers >= OUTLIER_MAX_CONSECUTIVE (default 3)
         * OR outlier_ratio > OUTLIER_MAX_RATIO (default 0.02) AND n_total >= OUTLIER_RATIO_MIN_OBS (default 100)
         * OR n_outlier > OUTLIER_MAX_OUTLIER_ROWS_ABS (default 30)
    6) Trading-day completeness (tolerant) computed on the FINAL cleaned row-set (post quarantine):
       - Compute expected trading sessions vs actual distinct dates (calendar-based)
       - Drop ticker ONLY if:
         missing > max(TRADING_MAX_MISSING_DAYS_ABS (default 30), ceil(expected * TRADING_MAX_MISSING_RATIO (default 0.02)))

    Still-hard ticker drop (kept strict):
    - Conflicting duplicates: same (ticker,date) has multiple different OHLCV versions => drop ticker.
    """
    p = Paths()
    spark = get_spark()

    try:
        df = spark.read.parquet(p.BRONZE_PRICES_DIR)
        dataset = "bronze_prices"

        date_c = _resolve_col(df, ["date", "Date"], dataset=dataset)
        open_c = _resolve_col(df, ["open", "Open"], dataset=dataset)
        high_c = _resolve_col(df, ["high", "High"], dataset=dataset)
        low_c = _resolve_col(df, ["low", "Low"], dataset=dataset)
        close_c = _resolve_col(df, ["close", "Close"], dataset=dataset)
        adj_c = _resolve_col(df, ["adj close", "adj_close", "Adj Close", "Adj_Close"], dataset=dataset)
        vol_c = _resolve_col(df, ["volume", "Volume"], dataset=dataset)
        ticker_c = _resolve_col(df, ["ticker"], dataset=dataset)

        prices = (
            df.select(
                _parse_date(F.col(date_c)).alias("date"),
                _to_double(F.col(open_c)).alias("open"),
                _to_double(F.col(high_c)).alias("high"),
                _to_double(F.col(low_c)).alias("low"),
                _to_double(F.col(close_c)).alias("close"),
                _to_double(F.col(adj_c)).alias("adj_close"),
                _to_long(F.col(vol_c)).alias("volume"),
                _standardize_ticker(F.col(ticker_c)).alias("ticker"),
            )
            .filter(F.col("date").isNotNull() & F.col("ticker").isNotNull())
        )

        audit_dir = f"{p.SILVER_PRICES_DIR}_audit"

        # 1) Conflicting duplicates => drop ticker (strict)
        conflict_tickers = _conflicting_version_tickers(prices, audit_dir)

        # 2) Deterministic dedupe with "valid row" priority
        quality = (
            F.when(F.col("open").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("high").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("low").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("close").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("adj_close").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("volume").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        )

        valid_row = (
            (F.col("close") > 0)
            & (F.col("adj_close") > 0)
            & (F.col("volume") > 0)
            & (F.col("open").isNull() | (F.col("open") >= 0))
            & (F.col("high").isNull() | (F.col("high") >= 0))
            & (F.col("low").isNull() | (F.col("low") >= 0))
            & (F.col("high").isNull() | F.col("low").isNull() | (F.col("high") >= F.col("low")))
        )

        row_sig = F.sha2(
            F.concat_ws(
                "||",
                F.col("ticker"),
                F.col("date").cast("string"),
                F.col("open").cast("string"),
                F.col("high").cast("string"),
                F.col("low").cast("string"),
                F.col("close").cast("string"),
                F.col("adj_close").cast("string"),
                F.col("volume").cast("string"),
            ),
            256,
        )

        w_dd = Window.partitionBy("ticker", "date").orderBy(
            F.desc(valid_row.cast("int")),
            F.desc(quality),
            F.col("volume").desc_nulls_last(),
            F.col("adj_close").desc_nulls_last(),
            F.asc(row_sig),
        )

        prices = (
            prices.withColumn("_q", quality)
            .withColumn("_sig", row_sig)
            .withColumn("_rn", F.row_number().over(w_dd))
            .filter(F.col("_rn") == 1)
            .drop("_q", "_sig", "_rn")
        )

        # 3) Referential integrity: keep only tickers that exist in silver companies
        companies = spark.read.parquet(p.SILVER_COMPANIES_DIR)
        company_tickers = companies.select("ticker").distinct()
        prices = prices.join(F.broadcast(company_tickers), on="ticker", how="left_semi")

        # 4) Basic rules: row-level quarantine + ticker-level drop only if ratio/abs too high
        prices_after_basic, drop_basic = _apply_basic_rules_rowlevel(prices, audit_dir)

        # 5) Outliers: row-level quarantine + ticker-level drop only if consecutive/ratio/abs too high
        prices_after_outlier, drop_outlier = _apply_outliers_rowlevel(prices_after_basic, audit_dir)

        # 6) Trading-day completeness (tolerant) computed on cleaned rows based on basic rules (align with silver_checks)
        bad_trading = _bad_trading_day_tickers_tolerant(prices_after_basic, companies, audit_dir, spark)

        # Union all bad tickers, then remove them from the cleaned rows
        bad_tickers = (
            conflict_tickers.select("ticker")
            .unionByName(drop_basic.select("ticker"))
            .unionByName(drop_outlier.select("ticker"))
            .unionByName(bad_trading.select("ticker"))
            .distinct()
        )

        if bad_tickers.limit(1).count() > 0:
            bad_tickers.write.mode("overwrite").parquet(f"{audit_dir}/bad_tickers_all")

        prices_clean = prices_after_outlier.join(F.broadcast(bad_tickers), on="ticker", how="left_anti")

        _rm_path(spark, p.SILVER_PRICES_DIR)

        (
            prices_clean.write.mode("overwrite")
            .partitionBy("ticker")
            .parquet(p.SILVER_PRICES_DIR)
        )

    finally:
        spark.stop()



def run_bronze_to_silver() -> None:
    transform_companies_bronze_to_silver()
    transform_prices_bronze_to_silver()


if __name__ == "__main__":
    run_bronze_to_silver()