import os
import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from onetl.hwm.store import YAMLHWMStore


def load_config(config_path="config/settings.yaml"):
    load_dotenv()

    with open(config_path) as f:
        config = yaml.safe_load(f)

    config["source"]["user"] = os.environ["POSTGRES_USER"]
    config["source"]["password"] = os.environ["POSTGRES_PASSWORD"]

    return config


def get_spark_session(config):

    builder = SparkSession.builder.appName(config["spark"]["app_name"]).master(
        config["spark"]["master"]
    )

    for key, value in config["spark"]["config"].items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    return spark


def setup_hwm_store(config):
    path = config["hwm_store"]["path"]
    os.makedirs(path, exist_ok=True)
    store = YAMLHWMStore(path=path)
    return store
