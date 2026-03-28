from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.quality.base_check import BaseQualityCheck


class NonNegativeCheck(BaseQualityCheck):
    def __init__(self, columns: list[str]):
        self._columns = columns

    @property
    def name(self) -> str:
        return f"non_negative({','.join(self._columns)})"

    def apply(self, df: DataFrame) -> DataFrame:
        conditions = [F.col(c) >= 0 for c in self._columns]
        combined = reduce(lambda a, b: a & b, conditions)
        return df.filter(combined)
