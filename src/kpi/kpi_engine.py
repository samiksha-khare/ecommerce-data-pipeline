from pyspark.sql import DataFrame

from src.kpi.base_kpi import BaseKPI
from src.kpi.revenue_kpis import RevenueKPIs
from src.kpi.customer_kpis import CustomerKPIs
from src.kpi.product_kpis import ProductKPIs
from src.utils.logger import get_logger

logger = get_logger(__name__)


class KPIEngine:
    def __init__(self, kpi_calculators: list[BaseKPI] | None = None):
        self._calculators = kpi_calculators or [
            RevenueKPIs(),
            CustomerKPIs(),
            ProductKPIs(),
        ]

    def compute_all(self, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        logger.info("Computing KPIs...")
        all_kpis: dict[str, DataFrame] = {}

        for calculator in self._calculators:
            logger.info(f"  Running {calculator.name} KPIs...")
            kpis = calculator.compute(tables)
            all_kpis.update(kpis)
            logger.info(f"  {calculator.name}: {len(kpis)} KPI tables computed")

        logger.info(f"KPI computation complete. Total: {len(all_kpis)} KPI tables")
        return all_kpis
