from src.quality.deduplication_check import DeduplicationCheck
from src.quality.null_check import NullCheck
from src.quality.range_check import RangeCheck
from src.quality.non_negative_check import NonNegativeCheck
from src.quality.check_registry import QualityCheckRegistry
from src.quality.quality_engine import QualityEngine


def test_deduplication(spark):
    data = [("A", 1), ("A", 1), ("B", 2)]
    df = spark.createDataFrame(data, ["id", "val"])
    check = DeduplicationCheck(subset=["id"])
    result = check.apply(df)
    assert result.count() == 2


def test_null_removal(spark):
    data = [("A", 1), (None, 2), ("C", None)]
    df = spark.createDataFrame(data, ["id", "val"])
    check = NullCheck(columns=["id"])
    result = check.apply(df)
    assert result.count() == 2


def test_range_check(spark):
    data = [(0.1,), (0.5,), (1.5,), (-0.1,)]
    df = spark.createDataFrame(data, ["discount"])
    check = RangeCheck(column="discount", min_val=0.0, max_val=1.0)
    result = check.apply(df)
    assert result.count() == 2


def test_non_negative(spark):
    data = [(1, 10.0), (-1, 5.0), (3, -2.0), (2, 8.0)]
    df = spark.createDataFrame(data, ["qty", "price"])
    check = NonNegativeCheck(columns=["qty", "price"])
    result = check.apply(df)
    assert result.count() == 2


def test_check_registry():
    check = QualityCheckRegistry.create("deduplication", subset=["id"])
    assert isinstance(check, DeduplicationCheck)


def test_quality_engine(spark):
    data = [("A", 1, 0.5), ("A", 1, 0.5), ("B", -1, 0.3), ("C", 2, 1.5)]
    df = spark.createDataFrame(data, ["id", "qty", "discount"])
    checks = [
        DeduplicationCheck(subset=["id"]),
        NonNegativeCheck(columns=["qty"]),
        RangeCheck(column="discount", min_val=0.0, max_val=1.0),
    ]
    engine = QualityEngine(checks)
    result = engine.run(df, "test")
    assert result.count() == 1  # Only "A" survives all checks
