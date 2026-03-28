from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_product(products_df: DataFrame) -> DataFrame:
    w = Window.orderBy("product_id")
    return (
        products_df
        .withColumn("product_key", F.row_number().over(w))
        .select(
            "product_key",
            "product_id",
            "product_name",
            "category",
            "sub_category",
            "brand",
            "unit_price",
            "cost_price",
            "weight",
            "supplier",
        )
    )
