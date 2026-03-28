import logging
from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

logger = logging.getLogger(__name__)


def create_connection(spark, config):
    connection = Postgres(
        host=config["source"]["host"],
        port=config["source"]["port"],
        user=config["source"]["user"],
        password=config["source"]["password"],
        database=config["source"]["database"],
        spark=spark,
    )
    connection.check()
    return connection


def extract_full(spark, config):
    connection = create_connection(spark, config)
    reader = DBReader(connection=connection, source=config["source"]["table"])
    df = reader.run()
    logger.info(f"Extracted (full): {df.count()} rows")
    return df


def extract_incremental(spark, config):

    connection = create_connection(spark, config)

    reader = DBReader(
        connection=connection,
        source=config["source"]["table"],
        hwm=DBReader.AutoDetectHWM(
            name="events_hwm", expression=config["incremental"]["hwm_column"]
        ),
    )

    with IncrementalStrategy():
        df = reader.run()

    logger.info(f'extracted: {df.count()} rows')

    return df
