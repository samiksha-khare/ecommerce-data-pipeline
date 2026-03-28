from pyspark.sql import DataFrame

from src.quality.base_check import BaseQualityCheck


class DeduplicationCheck(BaseQualityCheck):
    def __init__(self, subset: list[str] | None = None):
        self._subset = subset

    @property
    def name(self) -> str:
        return "deduplication"

    def apply(self, df: DataFrame) -> DataFrame:
        return df.dropDuplicates(subset=self._subset)
