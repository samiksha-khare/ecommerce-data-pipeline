from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.kpi.base_kpi import BaseKPI


class RevenueKPIs(BaseKPI):
    @property
    def name(self) -> str:
        return "revenue"

    def compute(self, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        fact = tables["fact_orders"]
        dim_date = tables["dim_date"]
        dim_product = tables["dim_product"]
        dim_customer = tables["dim_customer"]

        # Join fact with date dimension
        fact_with_date = fact.join(dim_date, on="date_key", how="left")

        # 1. Total revenue
        total_revenue = fact.agg(
            F.sum("net_amount").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.countDistinct("order_id").alias("unique_orders"),
        )

        # 2. Revenue by month
        revenue_by_month = (
            fact_with_date
            .groupBy("year", "month", "month_name")
            .agg(
                F.sum("net_amount").alias("revenue"),
                F.countDistinct("order_id").alias("num_orders"),
                F.avg("net_amount").alias("avg_order_value"),
            )
            .orderBy("year", "month")
        )

        # 3. MoM growth rate
        w = Window.orderBy("year", "month")
        revenue_growth = (
            revenue_by_month
            .withColumn("prev_revenue", F.lag("revenue").over(w))
            .withColumn(
                "mom_growth_pct",
                F.when(
                    F.col("prev_revenue").isNotNull(),
                    ((F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100)
                ),
            )
            .select("year", "month", "month_name", "revenue", "mom_growth_pct")
        )

        # 4. Revenue by category
        fact_with_product = fact.join(dim_product, on="product_key", how="left")
        revenue_by_category = (
            fact_with_product
            .groupBy("category")
            .agg(
                F.sum("net_amount").alias("revenue"),
                F.countDistinct("order_id").alias("num_orders"),
            )
            .orderBy(F.desc("revenue"))
        )

        # 5. Revenue by customer segment
        fact_with_customer = fact.join(dim_customer, on="customer_key", how="left")
        revenue_by_segment = (
            fact_with_customer
            .groupBy("segment")
            .agg(
                F.sum("net_amount").alias("revenue"),
                F.countDistinct("order_id").alias("num_orders"),
                F.avg("net_amount").alias("avg_order_value"),
            )
            .orderBy(F.desc("revenue"))
        )

        # 6. AOV (Average Order Value)
        aov = fact.groupBy("order_id").agg(
            F.sum("net_amount").alias("order_total")
        ).agg(
            F.avg("order_total").alias("average_order_value"),
            F.min("order_total").alias("min_order_value"),
            F.max("order_total").alias("max_order_value"),
        )

        return {
            "kpi_total_revenue": total_revenue,
            "kpi_revenue_by_month": revenue_by_month,
            "kpi_revenue_growth": revenue_growth,
            "kpi_revenue_by_category": revenue_by_category,
            "kpi_revenue_by_segment": revenue_by_segment,
            "kpi_aov": aov,
        }
