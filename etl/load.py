import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def load_full(df: DataFrame, config: dict):
    """Full load"""

    df.write.mode("overwrite").partitionBy(*config["target"]["partition_by"]).parquet(
        config["target"]["path"]
    )

    logger.info(f"Full load: {df.count()} rows → {config['target']['path']}")


def load_incremental(df: DataFrame, config: dict):
    """Incremental load"""

    df.write.mode("append").partitionBy(*config["target"]["partition_by"]).parquet(
        config["target"]["path"]
    )

    logger.info(f"Incremental load: {df.count()} rows -> {config['target']['path']}")
