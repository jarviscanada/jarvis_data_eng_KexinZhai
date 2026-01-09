from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType

from src.utils.spark_session import get_spark
from src.utils.config import Paths


# ---------------------------
# Helpers
# ---------------------------
def _safe_daily_return(adj: F.Column, prev_adj: F.Column) -> F.Column:
    return F.when(
        prev_adj.isNotNull() & (prev_adj > 0) & adj.isNotNull() & (adj > 0),
        (adj / prev_adj) - F.lit(1.0),
    ).otherwise(F.lit(None).cast(DoubleType()))


def _col_or_lit(df: DataFrame, col_name: str, default: str) -> F.Column:
    return F.col(col_name) if col_name in df.columns else F.lit(default)


def _non_empty(col: F.Column) -> F.Column:
    return col.isNotNull() & (F.trim(col) != F.lit(""))


def _blank_to_null(col: F.Column) -> F.Column:
    return F.when(col.isNull(), F.lit(None)).when(F.trim(col.cast("string")) == F.lit(""), F.lit(None)).otherwise(col)


def _recent_window(df: DataFrame, recent_n: int) -> DataFrame:
    """
    True recent window by DAYS (not by rows):
    keep rows where date >= (max_date_per_ticker - recent_n days)
    """
    mx = df.groupBy("ticker").agg(F.max("date").alias("_mx"))
    return (
        df.join(mx, on="ticker", how="left")
        .filter(F.col("date") >= F.date_sub(F.col("_mx"), int(recent_n)))
        .drop("_mx")
    )



# ---------------------------
# Labels
# ---------------------------
def _add_instrument_labels(companies: DataFrame) -> DataFrame:
    """
    Adds:
      - is_etf: boolean
      - etf_scope: 'broad' | 'sector' | 'unknown' | null (if not etf)
      - etf_hint: boolean (more permissive signal; used to mark etf_suspect later)

    Notes:
      - etf_scope classification is driven ONLY by name/short/description (not by sector/industry labels).
      - etf_hint is intentionally wider than is_etf to catch "maybe ETF" when metadata is incomplete.
    """
   
    # Be tolerant to either "snake_case" (silver-clean) or original CSV names.
    company_name_col = "company_name" if "company_name" in companies.columns else "company name"
    short_name_col = "short_name" if "short_name" in companies.columns else "short name"

    exchange_col = "exchange"
    desc_col = "description"

    com_name = F.lower(F.coalesce(_col_or_lit(companies, company_name_col, ""), F.lit("")))
    short_name = F.lower(F.coalesce(_col_or_lit(companies, short_name_col, ""), F.lit("")))
    exch = F.lower(F.coalesce(_col_or_lit(companies, exchange_col, ""), F.lit("")))
    desc = F.lower(F.coalesce(_col_or_lit(companies, desc_col, ""), F.lit("")))

    # Text for matching:
    # - ETF detection can use exchange too, so keep exchange in `text`.
    text = F.concat_ws(" ", com_name, short_name, desc, exch)
    # - ETF scope / sector guess should be driven by name/short/description (NOT by companies.sector).
    text_cls = F.concat_ws(" ", com_name, short_name, desc)

    # -----------------------
    # 1) is_etf (dataset-tuned)
    # -----------------------
    exch_arca = exch.contains("arca")

    # In your dataset, this appears to be the only "cboe/edgx" exchange value and is ETF-heavy.
    exch_cboe_edgx = exch.contains("cboe global markets edgx") | (exch.contains("cboe") & exch.contains("edgx"))

    # Strong ETF signal from description (more precise than just "seeks")
    desc_seeks = desc.contains("the investment seeks") | desc.contains("the fund seeks")

    # Explicit ETF/ETN tokens
    has_etf_token = text.rlike(r"\b(etf|etn)\b")

    # Issuer/brand keywords (expanded but still relatively high precision)
    issuer_kw = text.rlike(
        r"(ishares|blackrock|spdr|state\s+street|vanguard|invesco|powershares|"
        r"proshares|direxion|schwab|wisdomtree|van\s*eck|vaneck|first\s+trust|"
        r"global\s*x|pimco|fidelity|xtrackers|"
        r"franklin\s+templeton|templeton|nuveen|janus\s+henderson|"
        r"goldman\s+sachs|hsbc|ubs|deutsche\s+bank|"
        r"amundi|lyxor|bitwise|ark\s+invest|\bark\b|dws|dimensional)"
    )

    # Softer fund/trust words: only count them when we already have other ETF-like evidence
    fundish_kw = text.rlike(r"\b(exchange[-\s]?traded|index\s+fund|fund|trust|notes?)\b")

    # Required rule:
    # is_etf =
    #    exchange has "arca"
    # OR exchange is "cboe global markets edgx" (your dataset signal)
    # OR description contains "the investment seeks"
    # OR name/short/desc contains ETF/ETN
    # OR issuer/brand keywords
    # (+ fundish only as supporting evidence)
    is_etf = (
        exch_arca
        | exch_cboe_edgx
        | desc_seeks
        | has_etf_token
        | issuer_kw
        | (fundish_kw & (desc_seeks | has_etf_token | issuer_kw | exch_arca | exch_cboe_edgx))
    )

    # -----------------------
    # 2) ETF broad vs sector (ONLY from name/short/description)
    # -----------------------
    # Strong/explicit broad index keywords (high confidence)
    broad_strong_kw = text_cls.rlike(
        r"(s&p\s*500|sp\s*500|sp500|nasdaq[-\s]*100|\bndx\b|"
        r"russell\s*2000|russell\s*1000|dow\s+jones|"
        r"\bmsci\b|\bacwi\b|\beafe\b|"
        r"\bftse\b|stoxx|wilshire|"
        r"total\s+market|broad\s+market|"
        r"developed\s+markets|emerging\s+markets|ex[-\s]*us|"
        r"all\s*world|all[-\s]*world)"
    )

    # Generic geo words are NOT enough by themselves (avoid "international company" false broad):
    # Only treat them as broad if there's index/market/benchmark/fund context.
    broad_geo_kw = text_cls.rlike(r"\b(international|world|global(?!\s*x))\b")
    broad_context_kw = text_cls.rlike(
        r"\b(index|market|benchmark|equity|stock(s)?|shares|fund|etf|"
        r"msci|ftse|stoxx|russell|s&p|nasdaq|dow|acwi|eafe)\b"
    )
    broad_kw = broad_strong_kw | (broad_geo_kw & broad_context_kw)


    # -----------------------
    # 3) Sector ETF keywords (11 sectors)
    # -----------------------
    # Health Care FIRST to avoid biotech being caught by "technology"
    health_care = text_cls.rlike(
        r"\b(health\s*care|healthcare|biotech(nology)?|pharma(ceutical)?s?|"
        r"medical|hospital(s)?|clinical|life\s+sciences|health\s+services|managed\s+care|"
        r"medical\s+devices?|diagnostic(s)?)\b"
    )
    info_tech = text_cls.rlike(
        r"\b(information\s+technology|(?<!bio)technology|\btech\b|software|hardware|"
        r"semiconductor(s)?|chip(s)?|it\s+services|cybersecurity|cloud(\s+computing)?|"
        r"data\s+center(s)?|ai\b|artificial\s+intelligence)\b"
    )
    financials = text_cls.rlike(
        r"\b(financials?|financial\s+services|bank(s|ing)?|insurance|insurer(s)?|"
        r"broker(age)?|capital\s+markets|asset\s+management|investment\s+bank(ing)?|"
        r"fintech|payments?|mortgage)\b"
    )
    energy = text_cls.rlike(
        r"\b(energy|oil|gas|petroleum|crude|exploration|drilling|pipeline|midstream|"
        r"refin(ing|ery)|renewable(s)?|clean\s+energy|solar|wind)\b"
    )
    utilities = text_cls.rlike(r"\b(utilit(y|ies)|electric\s+utility|gas\s+utility|water\s+utility|power\s+utility)\b")
    real_estate = text_cls.rlike(
        r"\b(real\s+estate|reit(s)?|property|properties|realty|commercial\s+real\s+estate|"
        r"residential\s+real\s+estate)\b"
    )
    materials = text_cls.rlike(
        r"\b(materials?|chemical(s)?|mining|metal(s)?|metals?\s*&\s*mining|"
        r"steel|aluminum|copper|lithium|gold|silver|fertilizer(s)?|"
        r"building\s+materials?|construction\s+materials?)\b"
    )
    industrials = text_cls.rlike(
        r"\b(industrials?|industrial|aerospace|defen[cs]e|machinery|engineering|"
        r"transport(ation)?|logistics|rail(road)?s?|shipping|trucking|airline(s)?|"
        r"infrastructure)\b"
    )
    consumer_discretionary = text_cls.rlike(
        r"\b(consumer\s+discretionary|automobile(s)?|\bauto\b|\bcar\b|luxury|"
        r"hotel(s)?|resort(s)?|travel|leisure|casino(s)?|restaurant(s)?|dining|"
        r"apparel|footwear|specialty\s+retail|internet\s+retail|e-?commerce|"
        r"home\s+improvement)\b"
    )
    consumer_staples = text_cls.rlike(
        r"\b(consumer\s+staples|consumer\s+defensive|packaged\s+foods?|food(s)?|"
        r"beverage(s)?|tobacco|household|personal\s+products|grocery|supermarket(s)?)\b"
    )
    communication_services = text_cls.rlike(
        r"\b(communication\s+services|telecom(munications)?|wireless|media|entertainment|"
        r"streaming|social\s+media|interactive\s+media|internet\s+content)\b"
    )

    sector_kw = (
        energy
        | materials
        | industrials
        | utilities
        | health_care
        | financials
        | consumer_discretionary
        | consumer_staples
        | info_tech
        | communication_services
        | real_estate
    )

    etf_scope = (
        F.when(~is_etf, F.lit(None).cast("string"))
        .when(sector_kw, F.lit("sector"))
        .when(broad_kw, F.lit("broad"))
        .otherwise(F.lit("unknown"))
    )

    etf_hint = (exch_arca | exch_cboe_edgx | desc_seeks | has_etf_token | issuer_kw | fundish_kw)

    return (
        companies
        .withColumn("is_etf", is_etf.cast("boolean"))
        .withColumn("etf_scope", etf_scope)
        .withColumn("etf_hint", etf_hint.cast("boolean"))
    )


# ---------------------------
# Gold: Enriched
# ---------------------------
def create_gold_enriched() -> None:
    p = Paths()
    spark = get_spark()
    try:
        gold_enriched_dir = p.GOLD_ENRICHED_DIR
        audit_base = f"{p.GOLD_ENRICHED_DIR}_audit"

        prices = spark.read.parquet(p.SILVER_PRICES_DIR)
        companies = spark.read.parquet(p.SILVER_COMPANIES_DIR)
        companies_labeled = _add_instrument_labels(companies)

        # LEFT join to avoid systematic loss of prices after enrichment
        enriched = prices.join(companies_labeled, on="ticker", how="left")

        # --- resolve company_name column defensively ---
        company_name_col = "company_name" if "company_name" in enriched.columns else "company name"

        missing = enriched.filter(F.col(company_name_col).isNull()).select("ticker").distinct()
        if missing.limit(1).count() > 0:
            missing.write.mode("overwrite").parquet(f"{audit_base}/missing_company_join")
            raise ValueError(
                "[gold_enriched] join_missing_companies. "
                f"See: {audit_base}/missing_company_join"
            )

        # Normalize blanks to null
        enriched = (
            enriched
            .withColumn("sector", _blank_to_null(F.col("sector")))
            .withColumn("industry", _blank_to_null(F.col("industry")))
        )

        sector_missing = F.col("sector").isNull()
        industry_missing = F.col("industry").isNull()

        etf_suspect = (
            (F.col("is_etf") == F.lit(False))
            & (F.col("etf_hint") == F.lit(True))
            & sector_missing
            & industry_missing
        )

        instrument_type = (
            F.when(F.col("is_etf") == F.lit(True), F.lit("etf"))
            .when(etf_suspect, F.lit("etf_suspect"))
            .otherwise(F.lit("stock"))
        )

        label_quality = F.when(
            (instrument_type == F.lit("stock")) & sector_missing & industry_missing,
            F.lit("missing_sector_industry"),
        ).otherwise(F.lit("ok"))

        enriched = (
            enriched
            .withColumn("instrument_type", instrument_type)
            .withColumn("label_quality", label_quality)
        )

        # --- audit examples (NO FAIL) ---
        bad_stock_labels = (
            enriched
            .filter((F.col("instrument_type") == F.lit("stock")) & sector_missing & industry_missing)
            .select("ticker", F.col(company_name_col).alias("company_name"), "exchange", "sector", "industry", "label_quality")
            .distinct()
        )
        if bad_stock_labels.limit(1).count() > 0:
            bad_stock_labels.limit(2000).write.mode("overwrite").parquet(
                f"{audit_base}/stocks_missing_sector_industry_examples"
            )

        suspects = (
            enriched
            .filter(F.col("instrument_type") == F.lit("etf_suspect"))
            .select("ticker", F.col(company_name_col).alias("company_name"), "exchange", "is_etf", "etf_scope", "etf_hint", "sector", "industry")
            .distinct()
        )
        if suspects.limit(1).count() > 0:
            suspects.limit(5000).write.mode("overwrite").parquet(f"{audit_base}/etf_suspects_examples")

        # -----------------------
        # Returns + rolling (with cleaning)
        # -----------------------
        w = Window.partitionBy("ticker").orderBy("date")

        roll_n = int(os.getenv("ROLLING_WINDOW_DAYS", "20"))
        w_roll = Window.partitionBy("ticker").orderBy("date").rowsBetween(-(roll_n - 1), 0)

        max_gap_days = int(os.getenv("MAX_GAP_DAYS_FOR_DAILY_RETURN", "7"))
        min_roll_obs = int(os.getenv("MIN_ROLLING_RETURN_OBS", str(roll_n)))

        MAX_ABS_DAILY_RETURN_GOLD = float(os.getenv("MAX_ABS_DAILY_RETURN_GOLD", "3.0"))

        # ticker-drop policy (optional)
        DROP_TICKER_ON_BAD_RETURNS = os.getenv("DROP_TICKER_ON_BAD_RETURNS", "false").lower() == "true"
        MAX_BAD_RETURN_RATE = float(os.getenv("MAX_BAD_RETURN_RATE", "0.05"))  # 5%
        MAX_OUTLIER_STREAK = int(os.getenv("MAX_OUTLIER_STREAK", "5"))
        MIN_ADJACENT_RETURNS_FOR_EVAL = int(os.getenv("MIN_ADJACENT_RETURNS_FOR_EVAL", "60"))

        prev_date = F.lag(F.col("date"), 1).over(w)
        prev_adj = F.lag(F.col("adj_close"), 1).over(w)

        enriched = (
            enriched
            .withColumn("_prev_adj", prev_adj)
            .withColumn("_prev_date", prev_date)
            .withColumn("_gap_days", F.datediff(F.col("date"), F.col("_prev_date")))
            .withColumn(
                "_is_adjacent",
                F.col("_prev_date").isNotNull()
                & (F.col("_gap_days") >= F.lit(1))
                & (F.col("_gap_days") <= F.lit(max_gap_days))
            )
            .withColumn("_rn", F.row_number().over(w))
        )

        # raw daily_return (only when adjacent + valid prices)
        daily_return_raw = F.when(
            F.col("_is_adjacent")
            & F.col("_prev_adj").isNotNull() & (F.col("_prev_adj") > 0) & (~F.isnan(F.col("_prev_adj")))
            & F.col("adj_close").isNotNull() & (F.col("adj_close") > 0) & (~F.isnan(F.col("adj_close"))),
            (F.col("adj_close") / F.col("_prev_adj")) - F.lit(1.0),
        ).otherwise(F.lit(None).cast(DoubleType()))



        # -----------------------
        # Gold-enriched hard guards (align to gold_checks)
        # -----------------------
        
        # 1) required (should not happen often; defensive)
        bad_daily_required = (
            F.col("_is_adjacent")
            & F.col("_prev_adj").isNotNull()
            & (F.col("_prev_adj") > 0)
            & (F.col("adj_close") > 0)
            & (daily_return_raw.isNull() | F.isnan(daily_return_raw))
        )

        # 2) outlier bounds
        bad_daily_bounds = (
            F.col("_is_adjacent")
            & (F.col("_prev_adj") > 0)
            & (F.col("adj_close") > 0)
            & (F.abs(daily_return_raw) > F.lit(MAX_ABS_DAILY_RETURN_GOLD))
        )

        # quality label + CLEANED daily_return (outliers/missing -> NULL)
        daily_return_quality = (
            F.when(bad_daily_bounds, F.lit("outlier"))
            .when(bad_daily_required, F.lit("missing"))
            .otherwise(F.lit("ok"))
        )

        daily_return_clean = F.when(
            bad_daily_bounds | bad_daily_required,
            F.lit(None).cast(DoubleType())
        ).otherwise(daily_return_raw.cast(DoubleType()))

        enriched = (
            enriched
            .withColumn("daily_return", daily_return_clean)
            .withColumn("daily_return_quality", daily_return_quality)
        )

        # --- audit bad daily returns (NO FAIL) ---
        bad_daily = (
            enriched
            .filter(bad_daily_required | bad_daily_bounds)
            .select(
                "ticker",
                "date",
                "adj_close",
                "_prev_adj",
                "_prev_date",
                "_gap_days",
                "daily_return_quality",
                F.col(company_name_col).alias("company_name"),
                "exchange",
                "instrument_type",
                "etf_scope",
            )
        )
        if bad_daily.limit(1).count() > 0:
            bad_daily.write.mode("overwrite").parquet(f"{audit_base}/bad_daily_return_rows")

        # -----------------------
        # Optional: drop ticker if too many bad returns / too long outlier streak
        # -----------------------
        if DROP_TICKER_ON_BAD_RETURNS:
            opp = (
                F.col("_is_adjacent")
                & F.col("_prev_adj").isNotNull() & (F.col("_prev_adj") > 0) & (~F.isnan(F.col("_prev_adj")))
                & F.col("adj_close").isNotNull() & (F.col("adj_close") > 0) & (~F.isnan(F.col("adj_close")))
            )

            outlier_i = F.when(bad_daily_bounds, F.lit(1)).otherwise(F.lit(0))
            bad_i = F.when(bad_daily_bounds | bad_daily_required, F.lit(1)).otherwise(F.lit(0))
            opp_i = F.when(opp, F.lit(1)).otherwise(F.lit(0))

            # outlier streak length (consecutive outliers)
            grp_id = F.sum(F.when(outlier_i == 0, F.lit(1)).otherwise(F.lit(0))).over(w)
            streak_len = F.sum(outlier_i).over(Window.partitionBy("ticker", grp_id))

            stats = (
                enriched
                .withColumn("_opp_i", opp_i)
                .withColumn("_bad_i", bad_i)
                .withColumn("_outlier_i", outlier_i)
                .withColumn("_outlier_streak_len", streak_len)
                .groupBy("ticker")
                .agg(
                    F.sum("_opp_i").alias("n_opportunities"),
                    F.sum("_bad_i").alias("n_bad"),
                    F.sum("_outlier_i").alias("n_outliers"),
                    F.max("_outlier_streak_len").alias("max_outlier_streak"),
                )
                .withColumn(
                    "bad_rate",
                    F.when(F.col("n_opportunities") > 0, F.col("n_bad") / F.col("n_opportunities"))
                    .otherwise(F.lit(0.0))
                )
            )

            stats.write.mode("overwrite").parquet(f"{audit_base}/bad_return_ticker_stats")

            drop_tickers = (
                stats
                .filter(F.col("n_opportunities") >= F.lit(MIN_ADJACENT_RETURNS_FOR_EVAL))
                .filter(
                    (F.col("bad_rate") > F.lit(MAX_BAD_RETURN_RATE))
                    | (F.col("max_outlier_streak") >= F.lit(MAX_OUTLIER_STREAK))
                )
                .select("ticker", "n_opportunities", "n_bad", "n_outliers", "bad_rate", "max_outlier_streak")
            )

            if drop_tickers.limit(1).count() > 0:
                drop_tickers.write.mode("overwrite").parquet(f"{audit_base}/dropped_tickers_bad_returns")
                enriched = enriched.join(drop_tickers.select("ticker"), on="ticker", how="left_anti")

        # -----------------------
        # Rolling metrics computed on CLEANED daily_return
        # -----------------------
        # count of non-null returns in the rolling window
        enriched = enriched.withColumn("_roll_n_ret", F.count("daily_return").over(w_roll))

        min_vol_obs = max(min_roll_obs, 2)

        enriched = (
            enriched
            .withColumn(
                "rolling_avg_return",
                F.when(F.col("_roll_n_ret") >= F.lit(min_roll_obs), F.avg("daily_return").over(w_roll))
                .otherwise(F.lit(None).cast(DoubleType()))
            )
            .withColumn(
                "rolling_vol_return",
                F.when(F.col("_roll_n_ret") >= F.lit(min_vol_obs), F.stddev("daily_return").over(w_roll))
                .otherwise(F.lit(None).cast(DoubleType()))
            )
        )

        # --- audit rolling anomalies (NO FAIL, but should be rare) ---
        bad_roll = enriched.filter(
            (F.col("_roll_n_ret") >= F.lit(min_roll_obs))
            & (
                F.col("rolling_avg_return").isNull()
                | F.isnan(F.col("rolling_avg_return"))
                | F.col("rolling_vol_return").isNull()
                | F.isnan(F.col("rolling_vol_return"))
            )
        )
        if bad_roll.limit(1).count() > 0:
            (
                bad_roll.select("ticker", "date", "daily_return", "_roll_n_ret", "rolling_avg_return", "rolling_vol_return")
                .write.mode("overwrite")
                .parquet(f"{audit_base}/bad_rolling_metrics_rows")
            )
            # NOTE: do NOT raise in Scheme A

        # drop helper columns (optional; keep audit fields if you want)
        enriched_out = enriched.drop("_prev_adj", "_prev_date", "_gap_days", "_is_adjacent", "_rn", "_roll_n_ret")

        (
            enriched_out
            .write
            .mode("overwrite")
            .partitionBy("ticker")
            .parquet(gold_enriched_dir)
        )
    finally:
        spark.stop()


# ---------------------------
# Gold: Analytics (business question + required outputs)
# ---------------------------
def create_gold_analytics() -> None:
    """
    Outputs (under GOLD_ANALYTICS_DIR):
      - sector_daily_returns
      - industry_volatility
      - ticker_recent_metrics
      - top20_stocks_overall
      - top_stocks_by_sector
      - top_stocks_by_industry
      - top_broad_etfs
      - top_sector_etfs
      - etf_suspects_recent_metrics (optional, for inspection)
    """
    p = Paths()
    spark = get_spark()
    try:
        base = spark.read.parquet(p.GOLD_ENRICHED_DIR)
        analytics_dir = p.GOLD_ANALYTICS_DIR

        # Use different sources:
        # - df_ret: for metrics that truly require daily_return
        # - df_roll: for metrics that can use rolling_vol_return even if today's daily_return is null
        df_ret = base.filter(F.col("date").isNotNull() & F.col("daily_return").isNotNull())
        df_roll = base.filter(F.col("date").isNotNull() & F.col("rolling_vol_return").isNotNull())


        min_group_n = int(os.getenv("MIN_GROUP_SAMPLE_SIZE", "5"))

        # -----------------------
        # 1) Required analytics: sector_daily_returns
        # Columns expected by your gold checks (common pattern):
        #   date, sector, avg_daily_return, n_tickers
        # -----------------------
        sector_daily_returns = (
            df_ret.filter((F.col("instrument_type") == F.lit("stock")) & _non_empty(F.col("sector")))
            .groupBy("date", "sector")
            .agg(
                F.avg("daily_return").alias("avg_daily_return"),
                F.countDistinct("ticker").alias("n_tickers"),
            )
            .filter(F.col("n_tickers") >= F.lit(min_group_n))
            .select(
                F.col("date"),
                F.col("sector"),
                F.col("avg_daily_return").cast(DoubleType()).alias("avg_daily_return"),
                F.col("n_tickers").cast(LongType()).alias("n_tickers"),
            )
        )

        sector_daily_returns.write.mode("overwrite").parquet(f"{analytics_dir}/sector_daily_returns")

        # -----------------------
        # 2) Required analytics: industry_volatility
        # Compute 20d rolling vol per ticker, then average by (date, industry).
        # Columns:
        #   date, industry, avg_volatility, n_tickers
        # -----------------------
        industry_volatility = (
            df_roll.filter(
                (F.col("instrument_type") == F.lit("stock"))
                & _non_empty(F.col("industry"))
            )
            .groupBy("date", "industry")
            .agg(
                F.avg("rolling_vol_return").alias("avg_volatility"),
                F.countDistinct("ticker").alias("n_tickers"),
            )
            .filter(F.col("n_tickers") >= F.lit(min_group_n))
            .select(
                F.col("date"),
                F.col("industry"),
                F.col("avg_volatility").cast(DoubleType()).alias("avg_volatility"),
                F.col("n_tickers").cast(LongType()).alias("n_tickers"),
            )
        )

        industry_volatility.write.mode("overwrite").parquet(f"{analytics_dir}/industry_volatility")


        # -----------------------
        # 3) Core business question analytics
        # Recent performance + risk metrics (best-effort, no risk-free rate).
        # -----------------------
        recent_n = int(os.getenv("RECENT_WINDOW_DAYS", "60"))
        min_obs = int(os.getenv("MIN_RECENT_OBS", "30"))
        top_n = int(os.getenv("TOP_N", "20"))

        recent = _recent_window(df_ret, recent_n)


        # Guard against log(1+r) invalid values (r <= -1)
        log_ret = F.when(
            F.col("daily_return") > F.lit(-0.999999),
            F.log1p(F.col("daily_return"))
        ).otherwise(F.lit(None).cast(DoubleType()))

        recent_metrics = (
            recent
            .withColumn("log_return", log_ret)
            .groupBy("ticker")
            .agg(
                F.max("date").alias("end_date"),
                F.min("date").alias("start_date"),
                F.count(F.lit(1)).alias("n_obs"),
                F.avg("daily_return").alias("avg_daily_return"),
                F.stddev("daily_return").alias("vol_daily"),
                F.sum("log_return").alias("sum_log_return"),
                F.first("company_name", ignorenulls=True).alias("company_name"),
                F.first("sector", ignorenulls=True).alias("sector"),
                F.first("industry", ignorenulls=True).alias("industry"),
                F.first("exchange", ignorenulls=True).alias("exchange"),
                F.first("is_etf", ignorenulls=True).alias("is_etf"),
                F.first("etf_scope", ignorenulls=True).alias("etf_scope"),
                F.first("market_cap", ignorenulls=True).alias("market_cap"),
                F.first("instrument_type", ignorenulls=True).alias("instrument_type"),
                F.first("label_quality", ignorenulls=True).alias("label_quality"),
            )
            .withColumn("recent_cum_return", F.expm1(F.col("sum_log_return")))
            .withColumn(
                "sharpe_like",
                F.when(F.col("vol_daily").isNotNull() & (F.col("vol_daily") > 0), F.col("avg_daily_return") / F.col("vol_daily"))
                .otherwise(F.lit(None).cast(DoubleType()))
            )
            .filter(F.col("n_obs") >= F.lit(min_obs))
        )

        recent_metrics.write.mode("overwrite").parquet(f"{analytics_dir}/ticker_recent_metrics")


        # -----------------------
        # 3a) Market-wide Top 20 stocks overall
        # -----------------------
        stocks = recent_metrics.filter(F.col("instrument_type") == F.lit("stock"))
        etfs = recent_metrics.filter(F.col("instrument_type") == F.lit("etf"))
        suspects = recent_metrics.filter(F.col("instrument_type") == F.lit("etf_suspect"))

        if suspects.limit(1).count() > 0:
            suspects.write.mode("overwrite").parquet(f"{analytics_dir}/etf_suspects_recent_metrics")

        top20_overall = (
            stocks
            .orderBy(F.col("sharpe_like").desc_nulls_last(), F.col("recent_cum_return").desc_nulls_last())
            .limit(top_n)
        )
        top20_overall.write.mode("overwrite").parquet(f"{analytics_dir}/top20_stocks_overall")

        # -----------------------
        # 3b) Sector-specific Top N stocks
        # -----------------------
        w_sector_rank = Window.partitionBy("sector").orderBy(
            F.col("sharpe_like").desc_nulls_last(),
            F.col("recent_cum_return").desc_nulls_last()
        )
        top_by_sector = (
            stocks.filter(_non_empty(F.col("sector")))
            .withColumn("rank_in_sector", F.row_number().over(w_sector_rank))
            .filter(F.col("rank_in_sector") <= F.lit(top_n))
        )
        top_by_sector.write.mode("overwrite").parquet(f"{analytics_dir}/top_stocks_by_sector")

        # -----------------------
        # 3c) Industry-specific Top N stocks
        # -----------------------
        w_ind_rank = Window.partitionBy("industry").orderBy(
            F.col("sharpe_like").desc_nulls_last(),
            F.col("recent_cum_return").desc_nulls_last()
        )
        top_by_industry = (
            stocks.filter(_non_empty(F.col("industry")))
            .withColumn("rank_in_industry", F.row_number().over(w_ind_rank))
            .filter(F.col("rank_in_industry") <= F.lit(top_n))
        )
        top_by_industry.write.mode("overwrite").parquet(f"{analytics_dir}/top_stocks_by_industry")

        # -----------------------
        # 3d) ETF-based: broad vs sector ETFs
        # -----------------------
        top_broad_etfs = (
            etfs.filter(F.col("etf_scope") == F.lit("broad"))
            .orderBy(F.col("sharpe_like").desc_nulls_last(), F.col("recent_cum_return").desc_nulls_last())
            .limit(top_n)
        )
        top_broad_etfs.write.mode("overwrite").parquet(f"{analytics_dir}/top_broad_etfs")

        top_sector_etfs = (
            etfs.filter(F.col("etf_scope") == F.lit("sector"))
            .orderBy(F.col("sharpe_like").desc_nulls_last(), F.col("recent_cum_return").desc_nulls_last())
            .limit(top_n)
        )
        top_sector_etfs.write.mode("overwrite").parquet(f"{analytics_dir}/top_sector_etfs")

    finally:
        spark.stop()


def run() -> None:
    create_gold_enriched()
    create_gold_analytics()


if __name__ == "__main__":
    run()
