from pyspark.sql import DataFrame, SparkSession

from src.ingestion.base_reader import BaseReader


class JSONReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return spark.read.json(path)
