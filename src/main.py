"""Ecommerce Data Pipeline — Main Orchestrator."""

import argparse
import os
import sys

from src.spark_session import SparkSessionManager
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.ingestion.reader_factory import ReaderFactory
from src.quality.check_registry import QualityCheckRegistry
from src.quality.quality_engine import QualityEngine
from src.transformation.star_schema_builder import StarSchemaBuilder
from src.privacy.pii_masker import PIIMasker
from src.kpi.kpi_engine import KPIEngine

logger = get_logger(__name__)


class PipelineOrchestrator:
    def __init__(self, config_path: str, pii_config_path: str):
        self._config = load_config(config_path)
        self._pii_config = load_config(pii_config_path)
        self._spark_manager = SparkSessionManager()

    def run(self):
        spark_config = self._config["spark"]
        spark = self._spark_manager.get_session(
            app_name=spark_config["app_name"],
            master=spark_config["master"],
            shuffle_partitions=spark_config["shuffle_partitions"],
        )

        try:
            # Stage 1: Ingest data from all sources
            logger.info("=" * 60)
            logger.info("STAGE 1: DATA INGESTION")
            logger.info("=" * 60)
            sources = self._ingest(spark)

            # Stage 2: Run quality checks
            logger.info("=" * 60)
            logger.info("STAGE 2: DATA QUALITY CHECKS")
            logger.info("=" * 60)
            cleaned = self._quality_check(sources)

            # Stage 3: Build star schema
            logger.info("=" * 60)
            logger.info("STAGE 3: STAR SCHEMA TRANSFORMATION")
            logger.info("=" * 60)
            builder = StarSchemaBuilder(spark)
            warehouse_tables = builder.build(cleaned)

            # Stage 4: Apply PII masking
            logger.info("=" * 60)
            logger.info("STAGE 4: PII MASKING")
            logger.info("=" * 60)
            masker = PIIMasker(self._pii_config)
            warehouse_tables = masker.apply(warehouse_tables)

            # Stage 5: Write warehouse tables
            logger.info("=" * 60)
            logger.info("STAGE 5: WRITING WAREHOUSE TABLES")
            logger.info("=" * 60)
            warehouse_path = self._config["output"]["warehouse_path"]
            output_format = self._config["output"]["format"]
            for table_name, df in warehouse_tables.items():
                path = os.path.join(warehouse_path, table_name)
                df.write.mode("overwrite").format(output_format).save(path)
                logger.info(f"  Wrote {table_name} -> {path}")

            # Stage 6: Compute KPIs
            logger.info("=" * 60)
            logger.info("STAGE 6: KPI COMPUTATION")
            logger.info("=" * 60)
            kpi_engine = KPIEngine()
            kpis = kpi_engine.compute_all(warehouse_tables)

            # Stage 7: Write KPI tables
            logger.info("=" * 60)
            logger.info("STAGE 7: WRITING KPI TABLES")
            logger.info("=" * 60)
            kpi_path = self._config["output"]["kpi_path"]
            for kpi_name, df in kpis.items():
                path = os.path.join(kpi_path, kpi_name)
                df.write.mode("overwrite").format(output_format).save(path)
                logger.info(f"  Wrote {kpi_name} -> {path}")

            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETE")
            logger.info("=" * 60)

        finally:
            self._spark_manager.stop()

    def _ingest(self, spark) -> dict:
        sources = {}
        for source_name, source_config in self._config["data_sources"].items():
            reader = ReaderFactory.create(source_config["format"])
            df = reader.read(spark, source_config["path"])
            sources[source_name] = df
            logger.info(
                f"  Ingested {source_name}: {df.count()} rows, "
                f"{len(df.columns)} columns ({source_config['format']})"
            )
        return sources

    def _quality_check(self, sources: dict) -> dict:
        quality_config = self._config.get("quality_checks", {})
        cleaned = {}
        for source_name, df in sources.items():
            checks_config = quality_config.get(source_name, [])
            if checks_config:
                checks = QualityCheckRegistry.create_from_config(checks_config)
                engine = QualityEngine(checks)
                cleaned[source_name] = engine.run(df, source_name)
            else:
                cleaned[source_name] = df
                logger.info(f"  [{source_name}] No quality checks configured, skipping")
        return cleaned


def main():
    parser = argparse.ArgumentParser(description="Ecommerce Data Pipeline")
    parser.add_argument(
        "--config",
        default="config/pipeline_config.yaml",
        help="Path to pipeline config file",
    )
    parser.add_argument(
        "--pii-config",
        default="config/pii_masking_config.yaml",
        help="Path to PII masking config file",
    )
    args = parser.parse_args()

    orchestrator = PipelineOrchestrator(args.config, args.pii_config)
    orchestrator.run()


if __name__ == "__main__":
    main()
