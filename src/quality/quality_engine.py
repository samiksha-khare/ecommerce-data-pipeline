from pyspark.sql import DataFrame

from src.quality.base_check import BaseQualityCheck
from src.utils.logger import get_logger

logger = get_logger(__name__)


class QualityEngine:
    def __init__(self, checks: list[BaseQualityCheck]):
        self._checks = checks

    def run(self, df: DataFrame, source_name: str = "") -> DataFrame:
        prefix = f"[{source_name}] " if source_name else ""
        logger.info(f"{prefix}Starting quality checks. Initial row count: {df.count()}")

        for check in self._checks:
            before = df.count()
            df = check.apply(df)
            after = df.count()
            removed = before - after
            logger.info(
                f"{prefix}{check.name}: {before} -> {after} rows "
                f"({removed} removed)"
            )

        logger.info(f"{prefix}Quality checks complete. Final row count: {df.count()}")
        return df
