from pyspark.sql import DataFrame

from src.quality.base_check import BaseQualityCheck


class NullCheck(BaseQualityCheck):
    def __init__(self, columns: list[str] | None = None):
        self._columns = columns

    @property
    def name(self) -> str:
        return "null_removal"

    def apply(self, df: DataFrame) -> DataFrame:
        if self._columns:
            return df.dropna(subset=self._columns)
        return df.dropna()
