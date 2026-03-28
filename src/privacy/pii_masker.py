from pyspark.sql import DataFrame

from src.privacy.masking_strategy import MaskingStrategyFactory
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PIIMasker:
    def __init__(self, masking_config: dict):
        self._rules = masking_config.get("masking_rules", [])

    def apply(self, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        for rule in self._rules:
            table_name = rule["table"]
            if table_name not in tables:
                logger.warning(f"Table '{table_name}' not found, skipping PII masking")
                continue

            df = tables[table_name]
            for field in rule["fields"]:
                col_name = field["column"]
                strategy_name = field["strategy"]

                if col_name not in df.columns:
                    logger.warning(
                        f"Column '{col_name}' not found in '{table_name}', skipping"
                    )
                    continue

                strategy = MaskingStrategyFactory.create(strategy_name)
                df = df.withColumn(col_name, strategy.mask(col_name))
                logger.info(
                    f"Masked '{col_name}' in '{table_name}' using {strategy_name}"
                )

            tables[table_name] = df

        return tables
