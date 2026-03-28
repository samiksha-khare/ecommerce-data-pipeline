from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.kpi.base_kpi import BaseKPI


class CustomerKPIs(BaseKPI):
    @property
    def name(self) -> str:
        return "customer"

    def compute(self, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        fact = tables["fact_orders"]
        dim_customer = tables["dim_customer"]
        dim_date = tables["dim_date"]

        fact_with_customer = fact.join(dim_customer, on="customer_key", how="left")
        fact_with_date = fact.join(dim_date, on="date_key", how="left")

        # 1. Customer Lifetime Value
        fact_customer_date = fact_with_customer.join(dim_date, on="date_key", how="left")
        clv = (
            fact_customer_date
            .groupBy("customer_key", "customer_id", "segment")
            .agg(
                F.sum("net_amount").alias("total_spent"),
                F.countDistinct("order_id").alias("num_orders"),
                F.avg("net_amount").alias("avg_order_value"),
            )
        )

        # 2. Customer acquisition by month
        acquisition_by_month = (
            dim_customer
            .withColumn("signup_year", F.year("signup_date"))
            .withColumn("signup_month", F.month("signup_date"))
            .groupBy("signup_year", "signup_month")
            .agg(F.count("customer_id").alias("new_customers"))
            .orderBy("signup_year", "signup_month")
        )

        # 3. Churn rate — customers with no order in last 90 days
        max_date = fact_with_date.agg(F.max("full_date")).collect()[0][0]
        if max_date is not None:
            customer_last_order = (
                fact_customer_date
                .groupBy("customer_key")
                .agg(F.max("full_date").alias("last_order_date"))
            )
            total_customers = customer_last_order.count()
            churned = customer_last_order.filter(
                F.datediff(F.lit(max_date), F.col("last_order_date")) > 90
            ).count()

            churn_rate_value = (churned / total_customers * 100) if total_customers > 0 else 0
            spark = fact.sparkSession
            churn_rate = spark.createDataFrame(
                [(total_customers, churned, round(churn_rate_value, 2))],
                ["total_customers", "churned_customers", "churn_rate_pct"],
            )
        else:
            spark = fact.sparkSession
            churn_rate = spark.createDataFrame(
                [(0, 0, 0.0)],
                ["total_customers", "churned_customers", "churn_rate_pct"],
            )

        # 4. Repeat purchase rate
        order_counts = (
            fact.groupBy("customer_key")
            .agg(F.countDistinct("order_id").alias("num_orders"))
        )
        total = order_counts.count()
        repeat = order_counts.filter(F.col("num_orders") >= 2).count()
        repeat_rate = (repeat / total * 100) if total > 0 else 0

        spark = fact.sparkSession
        repeat_purchase = spark.createDataFrame(
            [(total, repeat, round(repeat_rate, 2))],
            ["total_customers", "repeat_customers", "repeat_purchase_rate_pct"],
        )

        # 5. Average orders per customer
        avg_orders = order_counts.agg(
            F.avg("num_orders").alias("avg_orders_per_customer"),
            F.min("num_orders").alias("min_orders"),
            F.max("num_orders").alias("max_orders"),
        )

        return {
            "kpi_customer_lifetime_value": clv,
            "kpi_customer_acquisition": acquisition_by_month,
            "kpi_churn_rate": churn_rate,
            "kpi_repeat_purchase_rate": repeat_purchase,
            "kpi_avg_orders_per_customer": avg_orders,
        }
