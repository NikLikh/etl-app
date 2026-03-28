import argparse
import os
import sys
import shutil
import logging

import kagglehub

from pyspark.sql import functions as F
from onetl.connection import Postgres
from onetl.db import DBWriter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import load_config, get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAGGLE_DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"


def download_dataset():
    """downloading dataset from kaggle"""

    path = kagglehub.dataset_download(KAGGLE_DATASET)
    csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]

    if not csv_files:
        logger.error("No CSV files found")
        sys.exit(1)

    src = os.path.join(path, csv_files[0])
    shared_dir = "/shared_data"
    dst = os.path.join(shared_dir, csv_files[0])

    if os.path.isdir(shared_dir):
        shutil.copy2(src, dst)
        logger.info(f"Copied {csv_files[0]} to {shared_dir}")
        return dst

    return src


def load_to_postgres(spark, csv_path, config, append=False):

    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    df = df.withColumn("event_time", F.to_timestamp("event_time"))

    connection = Postgres(
        host=config["source"]["host"],
        port=config["source"]["port"],
        user=config["source"]["user"],
        password=config["source"]["password"],
        database=config["source"]["database"],
        spark=spark,
    )

    mode = "append" if append else "overwrite"

    writer = DBWriter(
        connection=connection,
        target=config["source"]["table"],
        options=Postgres.WriteOptions(mode=mode),
    )
    writer.run(df)
    logger.info(f"Loaded {df.count()} rows")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Load Kaggle dataset to PostgreSQL")
    parser.add_argument(
        "--append", action="store_true", help="Append data instead of replacing"
    )

    args = parser.parse_args()

    config = load_config()
    spark = get_spark_session(config)
    csv_path = download_dataset()
    load_to_postgres(spark, csv_path, config, args.append)
