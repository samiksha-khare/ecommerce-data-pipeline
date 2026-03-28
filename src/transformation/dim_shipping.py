from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_shipping(shipping_df: DataFrame) -> DataFrame:
    w = Window.orderBy("shipping_id")
    return (
        shipping_df
        .withColumn("shipping_key", F.row_number().over(w))
        .withColumn(
            "ship_date", F.col("ship_date").cast("date")
        )
        .withColumn(
            "delivery_date", F.col("delivery_date").cast("date")
        )
        .withColumn(
            "delivery_days",
            F.datediff(F.col("delivery_date"), F.col("ship_date")),
        )
        .select(
            "shipping_key",
            "shipping_id",
            "order_id",
            "carrier",
            "shipping_mode",
            "tracking_number",
            "ship_date",
            "delivery_date",
            "delivery_days",
            "shipping_cost",
            "status",
        )
    )
