import time
import logging
from etl.extract import extract_full, extract_incremental
from etl.transform import apply_transformations
from etl.load import load_full, load_incremental
from config import load_config, get_spark_session, setup_hwm_store

logger = logging.getLogger(__name__)


def run_full_snapshot(config=None):
    """etl with snapshot"""
    if config is None:
        config = load_config()

    spark = get_spark_session(config)
    start = time.time()

    df = extract_full(spark, config)
    df = apply_transformations(df)
    load_full(df, config)

    elapsed = round(time.time() - start, 2)

    result = {
        "method": "snapshot",
        "duration_seconds": elapsed,
        "records_processed": df.count(),
        "status": "success",
    }

    logger.info("snapshot completed {result}")

    return result


def run_incremental(config=None):
    """etl with incremental load"""
    if config is None:
        config = load_config()

    start = time.time()
    spark = get_spark_session(config)
    hwm_store = setup_hwm_store(config)

    with hwm_store:
        df = extract_incremental(spark, config)
        df = apply_transformations(df)
        load_incremental(df, config)

    elapsed = round(time.time() - start, 2)
    result = {
        "method": "incremental",
        "duration_seconds": elapsed,
        "records_processed": df.count(),
        "status": "success",
    }

    logger.info(f"incremental completed {result}")

    return result
