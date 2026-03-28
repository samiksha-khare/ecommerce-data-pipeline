from pyspark.sql import SparkSession

from src.utils.logger import get_logger

logger = get_logger(__name__)


class SparkSessionManager:
    _instance = None
    _spark = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_session(self, app_name: str = "ecommerce-pipeline",
                    master: str = "local[*]",
                    shuffle_partitions: int = 8) -> SparkSession:
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName(app_name)
                .master(master)
                .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.driver.memory", "2g")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
            logger.info(f"SparkSession created: app={app_name}, master={master}")
        return self._spark

    def stop(self):
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            logger.info("SparkSession stopped")
