from src.quality.base_check import BaseQualityCheck
from src.quality.deduplication_check import DeduplicationCheck
from src.quality.null_check import NullCheck
from src.quality.range_check import RangeCheck
from src.quality.non_negative_check import NonNegativeCheck


class QualityCheckRegistry:
    _checks: dict[str, type[BaseQualityCheck]] = {
        "deduplication": DeduplicationCheck,
        "null_removal": NullCheck,
        "range_validation": RangeCheck,
        "non_negative": NonNegativeCheck,
    }

    @classmethod
    def register(cls, check_name: str, check_class: type[BaseQualityCheck]):
        cls._checks[check_name] = check_class

    @classmethod
    def create(cls, check_name: str, **params) -> BaseQualityCheck:
        check_class = cls._checks.get(check_name)
        if check_class is None:
            raise ValueError(f"Unknown quality check: {check_name}. "
                             f"Available: {list(cls._checks.keys())}")
        return check_class(**params)

    @classmethod
    def create_from_config(cls, checks_config: list[dict]) -> list[BaseQualityCheck]:
        return [
            cls.create(c["name"], **c.get("params", {}))
            for c in checks_config
        ]
