from pyspark.sql import SparkSession
from src.utils.config import PIPELINE
import os


def get_spark() -> SparkSession:
    cfg = PIPELINE

    spark = (
        SparkSession.builder
        .appName(cfg.APP_NAME)
        .master(cfg.SPARK_MASTER)

        # memory
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "10g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "8g"))
        .config("spark.executor.memoryOverhead", os.getenv("SPARK_EXECUTOR_MEMORY_OVERHEAD", "2g"))
        .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "0"))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
