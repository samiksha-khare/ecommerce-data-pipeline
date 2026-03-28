from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_orders(
    orders_df: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
    dim_shipping: DataFrame,
    dim_payment: DataFrame,
) -> DataFrame:
    # Compute monetary columns
    orders = (
        orders_df
        .withColumn("order_date", F.col("order_date").cast("date"))
        .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("discount_amount", F.col("total_amount") * F.col("discount_pct"))
        .withColumn("tax_amount", F.col("total_amount") * F.col("tax_pct"))
        .withColumn(
            "net_amount",
            F.col("total_amount") - F.col("discount_amount") + F.col("tax_amount"),
        )
    )

    # Join to get surrogate keys
    # Customer key
    fact = orders.join(
        dim_customer.select("customer_key", "customer_id"),
        on="customer_id",
        how="left",
    )

    # Product key
    fact = fact.join(
        dim_product.select("product_key", "product_id"),
        on="product_id",
        how="left",
    )

    # Date key
    fact = fact.join(
        dim_date.select("date_key", "full_date"),
        on=fact["order_date"] == dim_date["full_date"],
        how="left",
    )

    # Shipping key
    fact = fact.join(
        dim_shipping.select("shipping_key", F.col("order_id").alias("ship_order_id")),
        on=fact["order_id"] == F.col("ship_order_id"),
        how="left",
    ).drop("ship_order_id")

    # Payment key
    fact = fact.join(
        dim_payment.select("payment_key", F.col("order_id").alias("pay_order_id")),
        on=fact["order_id"] == F.col("pay_order_id"),
        how="left",
    ).drop("pay_order_id")

    return fact.select(
        "order_id",
        "customer_key",
        "product_key",
        "date_key",
        "shipping_key",
        "payment_key",
        "quantity",
        "unit_price",
        "total_amount",
        "discount_amount",
        "tax_amount",
        "net_amount",
    )
