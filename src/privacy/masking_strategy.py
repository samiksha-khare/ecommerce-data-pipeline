from abc import ABC, abstractmethod

from pyspark.sql import Column
from pyspark.sql import functions as F


class MaskingStrategy(ABC):
    @abstractmethod
    def mask(self, col_name: str) -> Column:
        pass


class PartialEmailMask(MaskingStrategy):
    def mask(self, col_name: str) -> Column:
        split = F.split(F.col(col_name), "@")
        local_part = split[0]
        domain = split[1]
        masked_local = F.concat(F.substring(local_part, 1, 2), F.lit("****"))
        return F.when(
            F.col(col_name).isNotNull(),
            F.concat(masked_local, F.lit("@"), domain),
        )


class PartialPhoneMask(MaskingStrategy):
    def mask(self, col_name: str) -> Column:
        return F.when(
            F.col(col_name).isNotNull(),
            F.concat(F.lit("****"), F.substring(F.col(col_name), -4, 4)),
        )


class RedactMask(MaskingStrategy):
    def mask(self, col_name: str) -> Column:
        return F.when(
            F.col(col_name).isNotNull(),
            F.lit("[REDACTED]"),
        )


class HashPartialMask(MaskingStrategy):
    def mask(self, col_name: str) -> Column:
        return F.when(
            F.col(col_name).isNotNull(),
            F.concat(F.lit("****"), F.substring(F.col(col_name), -4, 4)),
        )


class MaskingStrategyFactory:
    _strategies: dict[str, type[MaskingStrategy]] = {
        "partial_email": PartialEmailMask,
        "partial_phone": PartialPhoneMask,
        "redact": RedactMask,
        "hash_partial": HashPartialMask,
    }

    @classmethod
    def create(cls, strategy_name: str) -> MaskingStrategy:
        strategy_class = cls._strategies.get(strategy_name)
        if strategy_class is None:
            raise ValueError(f"Unknown masking strategy: {strategy_name}. "
                             f"Available: {list(cls._strategies.keys())}")
        return strategy_class()
