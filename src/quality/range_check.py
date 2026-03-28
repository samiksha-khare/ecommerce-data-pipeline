from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.quality.base_check import BaseQualityCheck


class RangeCheck(BaseQualityCheck):
    def __init__(self, column: str, min_val: float, max_val: float):
        self._column = column
        self._min_val = min_val
        self._max_val = max_val

    @property
    def name(self) -> str:
        return f"range_validation({self._column})"

    def apply(self, df: DataFrame) -> DataFrame:
        return df.filter(
            (F.col(self._column) >= self._min_val)
            & (F.col(self._column) <= self._max_val)
        )
