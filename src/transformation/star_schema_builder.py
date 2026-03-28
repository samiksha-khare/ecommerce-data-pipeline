from pyspark.sql import DataFrame, SparkSession

from src.transformation.dim_customer import build_dim_customer
from src.transformation.dim_product import build_dim_product
from src.transformation.dim_date import build_dim_date
from src.transformation.dim_shipping import build_dim_shipping
from src.transformation.dim_payment import build_dim_payment
from src.transformation.fact_orders import build_fact_orders
from src.utils.logger import get_logger

logger = get_logger(__name__)


class StarSchemaBuilder:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def build(self, sources: dict[str, DataFrame]) -> dict[str, DataFrame]:
        logger.info("Building star schema...")

        dim_customer = build_dim_customer(sources["customers"])
        logger.info(f"  dim_customer: {dim_customer.count()} rows")

        dim_product = build_dim_product(sources["products"])
        logger.info(f"  dim_product: {dim_product.count()} rows")

        dim_date = build_dim_date(self._spark, sources["orders"])
        logger.info(f"  dim_date: {dim_date.count()} rows")

        dim_shipping = build_dim_shipping(sources["shipping"])
        logger.info(f"  dim_shipping: {dim_shipping.count()} rows")

        dim_payment = build_dim_payment(sources["payments"])
        logger.info(f"  dim_payment: {dim_payment.count()} rows")

        fact_orders = build_fact_orders(
            sources["orders"],
            dim_customer,
            dim_product,
            dim_date,
            dim_shipping,
            dim_payment,
        )
        logger.info(f"  fact_orders: {fact_orders.count()} rows")

        tables = {
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_date": dim_date,
            "dim_shipping": dim_shipping,
            "dim_payment": dim_payment,
            "fact_orders": fact_orders,
        }

        logger.info("Star schema build complete.")
        return tables
