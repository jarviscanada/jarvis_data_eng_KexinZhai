import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    RAW_PRICES_DIR: str = os.getenv("RAW_PRICES_DIR", "/opt/data/raw/us_stocks_etfs")
    RAW_COMPANIES_CSV: str = os.getenv(
        "RAW_COMPANIES_CSV", "/opt/data/raw/company_info_and_logos/companies.csv"
    )

    BRONZE_COMPANIES_DIR: str = os.getenv("BRONZE_COMPANIES_DIR", "/opt/data/bronze/companies")
    BRONZE_PRICES_DIR: str = os.getenv("BRONZE_PRICES_DIR", "/opt/data/bronze/prices")

    SILVER_COMPANIES_DIR: str = os.getenv("SILVER_COMPANIES_DIR", "/opt/data/silver/companies")
    SILVER_PRICES_DIR: str = os.getenv("SILVER_PRICES_DIR", "/opt/data/silver/prices")

    GOLD_ENRICHED_DIR: str = os.getenv("GOLD_ENRICHED_DIR", "/opt/data/gold/prices_enriched")
    GOLD_ANALYTICS_DIR: str = os.getenv("GOLD_ANALYTICS_DIR", "/opt/data/gold/analytics")


@dataclass(frozen=True)
class PipelineConfig:
    APP_NAME: str = os.getenv("SPARK_APP_NAME", "equity-market-etl")
    SPARK_MASTER: str = os.getenv("SPARK_MASTER_URL") or os.getenv("SPARK_MASTER", "local[*]")

    SHUFFLE_PARTITIONS: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))


PATHS = Paths()
PIPELINE = PipelineConfig()
