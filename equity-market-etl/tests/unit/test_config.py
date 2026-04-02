# tests/unit/test_config.py
import os
from src.utils.config import Paths


def test_default_paths_exist_format():
    """
    Ensure Paths are container-safe by default, while still allowing env overrides.
    - Default expectation: /opt/data/...
    - If DATA_ROOT is set, validate against that root.
    """
    p = Paths()

    data_root = os.getenv("DATA_ROOT", "/opt/data").rstrip("/")

    # Key paths should live under data_root
    assert p.RAW_PRICES_DIR.startswith(f"{data_root}/"), (
        f"RAW_PRICES_DIR must be under {data_root}. Got: {p.RAW_PRICES_DIR}"
    )
    assert p.BRONZE_DIR.startswith(f"{data_root}/"), (
        f"BRONZE_DIR must be under {data_root}. Got: {p.BRONZE_DIR}"
    )
    assert p.SILVER_DIR.startswith(f"{data_root}/"), (
        f"SILVER_DIR must be under {data_root}. Got: {p.SILVER_DIR}"
    )
    assert p.GOLD_DIR.startswith(f"{data_root}/"), (
        f"GOLD_DIR must be under {data_root}. Got: {p.GOLD_DIR}"
    )

    # Companies CSV should look like a CSV path
    assert p.RAW_COMPANIES_CSV.endswith(".csv"), (
        f"RAW_COMPANIES_CSV must end with .csv. Got: {p.RAW_COMPANIES_CSV}"
    )
