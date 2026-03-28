from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_payment(payments_df: DataFrame) -> DataFrame:
    w = Window.orderBy("payment_id")
    return (
        payments_df
        .withColumn("payment_key", F.row_number().over(w))
        .withColumn("payment_date", F.col("payment_date").cast("date"))
        .select(
            "payment_key",
            "payment_id",
            "order_id",
            "payment_method",
            "card_type",
            "card_number",
            "payment_status",
            "payment_date",
            "transaction_id",
        )
    )
