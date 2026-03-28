from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from datetime import date, timedelta


def build_dim_date(spark: SparkSession, orders_df: DataFrame) -> DataFrame:
    min_date = orders_df.agg(F.min("order_date")).collect()[0][0]
    max_date = orders_df.agg(F.max("order_date")).collect()[0][0]

    if isinstance(min_date, str):
        min_date = date.fromisoformat(min_date)
    if isinstance(max_date, str):
        max_date = date.fromisoformat(max_date)

    dates = []
    current = min_date
    key = 1
    while current <= max_date:
        dates.append((key, current))
        current += timedelta(days=1)
        key += 1

    date_df = spark.createDataFrame(dates, ["date_key", "full_date"])

    return (
        date_df
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False),
        )
    )
