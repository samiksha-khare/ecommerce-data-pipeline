from src.privacy.pii_masker import PIIMasker


def test_pii_masker_email(spark):
    data = [("john@example.com",), ("jane@test.org",)]
    df = spark.createDataFrame(data, ["email"])
    config = {
        "masking_rules": [
            {
                "table": "test_table",
                "fields": [{"column": "email", "strategy": "partial_email"}],
            }
        ]
    }
    masker = PIIMasker(config)
    result = masker.apply({"test_table": df})
    emails = [row.email for row in result["test_table"].collect()]
    assert "jo****@example.com" in emails
    assert "ja****@test.org" in emails


def test_pii_masker_phone(spark):
    data = [("1234567890",)]
    df = spark.createDataFrame(data, ["phone"])
    config = {
        "masking_rules": [
            {
                "table": "test_table",
                "fields": [{"column": "phone", "strategy": "partial_phone"}],
            }
        ]
    }
    masker = PIIMasker(config)
    result = masker.apply({"test_table": df})
    phone = result["test_table"].collect()[0].phone
    assert phone == "****7890"


def test_pii_masker_redact(spark):
    data = [("123 Main St",)]
    df = spark.createDataFrame(data, ["address"])
    config = {
        "masking_rules": [
            {
                "table": "test_table",
                "fields": [{"column": "address", "strategy": "redact"}],
            }
        ]
    }
    masker = PIIMasker(config)
    result = masker.apply({"test_table": df})
    address = result["test_table"].collect()[0].address
    assert address == "[REDACTED]"


def test_pii_masker_skips_missing_table(spark):
    config = {
        "masking_rules": [
            {
                "table": "nonexistent",
                "fields": [{"column": "email", "strategy": "partial_email"}],
            }
        ]
    }
    masker = PIIMasker(config)
    result = masker.apply({})  # No tables
    assert result == {}
