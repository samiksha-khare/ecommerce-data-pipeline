from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_customer(customers_df: DataFrame) -> DataFrame:
    w = Window.orderBy("customer_id")
    return (
        customers_df
        .withColumn("customer_key", F.row_number().over(w))
        .withColumn(
            "age_group",
            F.when(F.datediff(F.current_date(), F.col("signup_date")) < 365, "New")
            .when(F.datediff(F.current_date(), F.col("signup_date")) < 730, "Regular")
            .otherwise("Loyal")
        )
        .select(
            "customer_key",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "address",
            "city",
            "state",
            "country",
            "segment",
            F.col("signup_date").cast("date").alias("signup_date"),
            "age_group",
        )
    )
