import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def filter_invalid_events(df: DataFrame) -> DataFrame:
    """filter for invalid events"""

    df = df.filter((F.col("product_id").isNotNull()) & (F.col("price") >= 0))

    return df


def filter_bots(df: DataFrame, BOT_THRESHOLD=1000) -> DataFrame:
    """filter for bot sessions"""

    session_count = df.groupBy("user_session").agg(
        F.count("*").alias("session_event_count")
    )
    normal_sessions = session_count.filter(
        F.col("session_event_count") <= BOT_THRESHOLD
    )

    df = df.join(normal_sessions, on="user_session", how="inner")
    df = df.drop("session_event_count")

    return df


def parse_categories(df: DataFrame) -> DataFrame:
    """parse category code. Null if no category"""

    parts = F.split(F.col("category_code"), "\\.")

    df = df.withColumn("top_category", F.try_element_at(parts, F.lit(1)))
    df = df.withColumn("sub_category", F.try_element_at(parts, F.lit(2)))
    df = df.withColumn("detail_category", F.try_element_at(parts, F.lit(3)))

    return df


def add_time_features(df: DataFrame) -> DataFrame:

    df = df.withColumn("event_date", F.to_date("event_time"))
    df = df.withColumn("event_hour", F.hour("event_time"))
    df = df.withColumn("day_of_week", F.dayofweek("event_time"))

    return df


def add_funnel_flags(df: DataFrame) -> DataFrame:
    """add flags"""
    df = df.withColumn(
        "is_view", F.when(F.col("event_type") == "view", True).otherwise(False)
    )
    df = df.withColumn(
        "is_cart", F.when(F.col("event_type") == "cart", True).otherwise(False)
    )
    df = df.withColumn(
        "is_purchase", F.when(F.col("event_type") == "purchase", True).otherwise(False)
    )

    return df


def apply_transformations(df: DataFrame) -> DataFrame:

    df = filter_invalid_events(df)
    df = filter_bots(df)
    df = parse_categories(df)
    df = add_time_features(df)
    df = add_funnel_flags(df)

    return df
