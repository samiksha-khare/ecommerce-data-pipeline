from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.kpi.base_kpi import BaseKPI


class ProductKPIs(BaseKPI):
    @property
    def name(self) -> str:
        return "product"

    def compute(self, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        fact = tables["fact_orders"]
        dim_product = tables["dim_product"]

        fact_with_product = fact.join(dim_product, on="product_key", how="left")

        # 1. Top 10 products by revenue
        top_by_revenue = (
            fact_with_product
            .groupBy("product_id", "product_name", "category")
            .agg(F.sum("net_amount").alias("total_revenue"))
            .orderBy(F.desc("total_revenue"))
            .limit(10)
        )

        # 2. Top 10 products by quantity sold
        top_by_quantity = (
            fact_with_product
            .groupBy("product_id", "product_name", "category")
            .agg(F.sum("quantity").alias("total_quantity"))
            .orderBy(F.desc("total_quantity"))
            .limit(10)
        )

        # 3. Product profit margin
        profit_margin = (
            dim_product
            .withColumn(
                "profit_margin_pct",
                F.round(
                    (F.col("unit_price") - F.col("cost_price")) / F.col("unit_price") * 100,
                    2,
                ),
            )
            .select("product_id", "product_name", "category", "unit_price",
                    "cost_price", "profit_margin_pct")
            .orderBy(F.desc("profit_margin_pct"))
        )

        # 4. Category revenue contribution
        total_revenue = fact.agg(F.sum("net_amount")).collect()[0][0] or 1
        category_contribution = (
            fact_with_product
            .groupBy("category")
            .agg(F.sum("net_amount").alias("category_revenue"))
            .withColumn(
                "contribution_pct",
                F.round(F.col("category_revenue") / F.lit(total_revenue) * 100, 2),
            )
            .orderBy(F.desc("contribution_pct"))
        )

        # 5. Average discount by category
        avg_discount = (
            fact_with_product
            .withColumn(
                "discount_pct",
                F.when(F.col("total_amount") > 0,
                       F.col("discount_amount") / F.col("total_amount") * 100)
                .otherwise(0),
            )
            .groupBy("category")
            .agg(F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"))
            .orderBy("category")
        )

        return {
            "kpi_top_products_by_revenue": top_by_revenue,
            "kpi_top_products_by_quantity": top_by_quantity,
            "kpi_product_profit_margin": profit_margin,
            "kpi_category_contribution": category_contribution,
            "kpi_avg_discount_by_category": avg_discount,
        }
