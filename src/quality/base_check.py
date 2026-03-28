from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseQualityCheck(ABC):
    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass
