import pytest

from src.ingestion.reader_factory import ReaderFactory
from src.ingestion.csv_reader import CSVReader
from src.ingestion.json_reader import JSONReader
from src.ingestion.parquet_reader import ParquetReader


def test_create_csv_reader():
    reader = ReaderFactory.create("csv")
    assert isinstance(reader, CSVReader)


def test_create_json_reader():
    reader = ReaderFactory.create("json")
    assert isinstance(reader, JSONReader)


def test_create_parquet_reader():
    reader = ReaderFactory.create("parquet")
    assert isinstance(reader, ParquetReader)


def test_unsupported_format():
    with pytest.raises(ValueError, match="Unsupported format"):
        ReaderFactory.create("avro")
