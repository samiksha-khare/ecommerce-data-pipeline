from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseKPI(ABC):
    @abstractmethod
    def compute(self, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Compute KPIs and return a dict of {kpi_name: DataFrame}."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass
