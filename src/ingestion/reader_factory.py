from src.ingestion.base_reader import BaseReader
from src.ingestion.csv_reader import CSVReader
from src.ingestion.json_reader import JSONReader
from src.ingestion.parquet_reader import ParquetReader


class ReaderFactory:
    _readers: dict[str, type[BaseReader]] = {
        "csv": CSVReader,
        "json": JSONReader,
        "parquet": ParquetReader,
    }

    @classmethod
    def register(cls, format_key: str, reader_class: type[BaseReader]):
        cls._readers[format_key] = reader_class

    @classmethod
    def create(cls, format_key: str) -> BaseReader:
        reader_class = cls._readers.get(format_key)
        if reader_class is None:
            raise ValueError(f"Unsupported format: {format_key}. "
                             f"Available: {list(cls._readers.keys())}")
        return reader_class()
