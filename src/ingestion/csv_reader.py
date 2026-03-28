from pyspark.sql import DataFrame, SparkSession

from src.ingestion.base_reader import BaseReader


class CSVReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return spark.read.csv(path, header=True, inferSchema=True)
