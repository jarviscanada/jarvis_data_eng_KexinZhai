# tests/data_quality/test_dq_checks.py
import os
import pytest

from src.utils.config import Paths
from src.validation.data_quality_checks import bronze_checks, silver_checks, gold_checks


p = Paths()


@pytest.mark.skipif(not os.path.exists(p.BRONZE_DIR), reason="Bronze layer not built yet")
def test_bronze_checks():
    bronze_checks()


@pytest.mark.skipif(not os.path.exists(p.SILVER_DIR), reason="Silver layer not built yet")
def test_silver_checks():
    silver_checks()


@pytest.mark.skipif(not os.path.exists(p.GOLD_DIR), reason="Gold layer not built yet")
def test_gold_checks():
    gold_checks()
