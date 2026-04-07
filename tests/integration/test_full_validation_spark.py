"""Integration tests for full validation workflow with PySpark DataFrames."""

import os
import sys

import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")

# Ensure PySpark workers use the same Python as the test runner
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from databridge_validator import (
    build_mismatch_report,
    cast_all_to_string,
    clean_control_characters,
    compare_dataframes,
    get_duplicate_report,
    get_null_analysis,
    get_row_counts,
    get_schema_diff,
    mask_pii_columns,
    normalize_columns,
    trim_whitespace,
)
from databridge_validator.core.models import ValidationResult


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for integration tests."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("databridge-validator-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def spark_source_df(spark):
    """Standard source Spark DataFrame for testing."""
    return spark.createDataFrame(
        [
            (1, "Alice", "alice@test.com", 100.0),
            (2, "Bob", "bob@test.com", 200.0),
            (3, "Charlie", "charlie@test.com", 300.0),
            (4, "Diana", "diana@test.com", 400.0),
            (5, "Eve", "eve@test.com", 500.0),
        ],
        ["id", "name", "email", "amount"],
    )


@pytest.fixture
def spark_target_df(spark):
    """Standard target Spark DataFrame with intentional differences.

    Expected:
    - id=1: match
    - id=2: mismatch (name, email, amount differ)
    - id=3: match
    - id=4, 5: source extras
    - id=6, 7: target extras
    """
    return spark.createDataFrame(
        [
            (1, "Alice", "alice@test.com", 100.0),
            (2, "Bobby", "bob_new@test.com", 250.0),
            (3, "Charlie", "charlie@test.com", 300.0),
            (6, "Frank", "frank@test.com", 600.0),
            (7, "Grace", "grace@test.com", 700.0),
        ],
        ["id", "name", "email", "amount"],
    )


@pytest.mark.spark
class TestCompareDataframesSpark:
    """Tests for compare_dataframes with PySpark DataFrames."""

    def test_compare_dataframes_basic_mismatches(self, spark_source_df, spark_target_df):
        """Verify basic comparison detects mismatches, source extras, and target extras."""
        result = compare_dataframes(spark_source_df, spark_target_df, key_columns=["id"])

        assert isinstance(result, ValidationResult)
        assert result.total_source_rows == 5
        assert result.total_target_rows == 5
        assert result.is_match is False
        assert result.mismatch_count == 1  # id=2
        assert result.source_extra_count == 2  # id=4, 5
        assert result.target_extra_count == 2  # id=6, 7
        assert result.matched_count == 2  # id=1, 3

    def test_compare_dataframes_mismatch_records_content(self, spark_source_df, spark_target_df):
        """Verify mismatch_records DataFrame contains correct data."""
        result = compare_dataframes(spark_source_df, spark_target_df, key_columns=["id"])

        assert result.mismatch_records is not None
        mismatch_pdf = result.mismatch_records.toPandas()
        assert len(mismatch_pdf) == 1
        assert mismatch_pdf["id"].iloc[0] == 2
        # Default report_columns="target" — should have target values
        assert mismatch_pdf["name"].iloc[0] == "Bobby"
        # mismatch_columns string should list the differing columns
        mc = mismatch_pdf["mismatch_columns"].iloc[0]
        assert "name" in mc
        assert "email" in mc
        assert "amount" in mc

    def test_compare_dataframes_source_extras(self, spark_source_df, spark_target_df):
        """Verify source_extra_records contains rows only in source."""
        result = compare_dataframes(spark_source_df, spark_target_df, key_columns=["id"])

        assert result.source_extra_records is not None
        extra_pdf = result.source_extra_records.toPandas()
        assert set(extra_pdf["id"].tolist()) == {4, 5}

    def test_compare_dataframes_target_extras(self, spark_source_df, spark_target_df):
        """Verify target_extra_records contains rows only in target."""
        result = compare_dataframes(spark_source_df, spark_target_df, key_columns=["id"])

        assert result.target_extra_records is not None
        extra_pdf = result.target_extra_records.toPandas()
        assert set(extra_pdf["id"].tolist()) == {6, 7}

    def test_compare_dataframes_identical_data_returns_match(self, spark, spark_source_df):
        """When DataFrames are identical, result should be a match."""
        result = compare_dataframes(spark_source_df, spark_source_df, key_columns=["id"])

        assert result.is_match is True
        assert result.mismatch_count == 0
        assert result.source_extra_count == 0
        assert result.target_extra_count == 0
        assert result.matched_count == 5
        assert result.mismatch_records is None
        assert result.source_extra_records is None
        assert result.target_extra_records is None

    def test_compare_dataframes_empty_dataframes(self, spark):
        """Comparing empty DataFrames should return a match with zero counts."""
        schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ])
        empty = spark.createDataFrame([], schema)

        result = compare_dataframes(empty, empty, key_columns=["id"])

        assert result.is_match is True
        assert result.total_source_rows == 0
        assert result.total_target_rows == 0

    def test_compare_dataframes_with_nulls(self, spark):
        """Null values should be handled correctly in comparisons."""
        source = spark.createDataFrame(
            [(1, "Alice", None), (2, None, "bob@test.com")],
            ["id", "name", "email"],
        )
        target = spark.createDataFrame(
            [(1, "Alice", "alice@test.com"), (2, "Bob", "bob@test.com")],
            ["id", "name", "email"],
        )
        result = compare_dataframes(source, target, key_columns=["id"])

        assert result.mismatch_count == 2

    def test_compare_dataframes_with_exclude_columns(self, spark_source_df, spark_target_df):
        """Excluded columns should not affect comparison."""
        result = compare_dataframes(
            spark_source_df, spark_target_df,
            key_columns=["id"],
            exclude_columns=["name", "email", "amount"],
        )
        # All common rows should match when comparison columns are excluded
        assert result.mismatch_count == 0
        # Extras are still detected (based on keys, not values)
        assert result.source_extra_count == 2
        assert result.target_extra_count == 2

    def test_compare_dataframes_case_insensitive(self, spark):
        """Case-insensitive column names should work."""
        source = spark.createDataFrame([(1, "Alice")], ["ID", "Name"])
        target = spark.createDataFrame([(1, "Bob")], ["id", "name"])

        result = compare_dataframes(source, target, key_columns=["id"], case_sensitive=False)
        assert result.mismatch_count == 1

    def test_compare_dataframes_report_columns_source(self, spark_source_df, spark_target_df):
        """report_columns='source' should include source data columns."""
        result = compare_dataframes(
            spark_source_df, spark_target_df,
            key_columns=["id"],
            report_columns="source",
        )
        assert result.mismatch_records is not None
        mismatch_pdf = result.mismatch_records.toPandas()
        # Source value for id=2 should be "Bob" (not "Bobby")
        assert mismatch_pdf["name"].iloc[0] == "Bob"

    def test_compare_dataframes_report_columns_both(self, spark_source_df, spark_target_df):
        """report_columns='both' should include source and target data columns."""
        result = compare_dataframes(
            spark_source_df, spark_target_df,
            key_columns=["id"],
            report_columns="both",
        )
        assert result.mismatch_records is not None
        cols = result.mismatch_records.columns
        assert "name_source" in cols
        assert "name_target" in cols

    def test_compare_dataframes_with_num_partitions(self, spark_source_df, spark_target_df):
        """num_partitions should be accepted without error."""
        result = compare_dataframes(
            spark_source_df, spark_target_df,
            key_columns=["id"],
            num_partitions=4,
        )
        assert isinstance(result, ValidationResult)
        assert result.mismatch_count == 1

    def test_compare_dataframes_with_persist(self, spark_source_df, spark_target_df):
        """persist=True should be accepted without error."""
        result = compare_dataframes(
            spark_source_df, spark_target_df,
            key_columns=["id"],
            persist=True,
        )
        assert isinstance(result, ValidationResult)
        assert result.mismatch_count == 1

    def test_compare_dataframes_summary_dict(self, spark_source_df, spark_target_df):
        """Summary dict should contain all expected keys."""
        result = compare_dataframes(spark_source_df, spark_target_df, key_columns=["id"])
        summary = result.summary
        assert summary["total_source"] == 5
        assert summary["total_target"] == 5
        assert summary["matched"] == 2
        assert summary["mismatched"] == 1
        assert summary["source_extras"] == 2
        assert summary["target_extras"] == 2

    def test_compare_dataframes_to_dict(self, spark_source_df, spark_target_df):
        """ValidationResult.to_dict() should work for Spark results."""
        result = compare_dataframes(spark_source_df, spark_target_df, key_columns=["id"])
        d = result.to_dict()
        assert isinstance(d, dict)
        assert d["mismatch_count"] == 1


@pytest.mark.spark
class TestPiiMaskingSpark:
    """Tests for PII masking with PySpark DataFrames."""

    def test_mask_pii_columns_alternate(self, spark):
        """Alternate masking on Spark DataFrame."""
        df = spark.createDataFrame(
            [(1, "Hello", "12345"), (2, "World", "67890")],
            ["id", "name", "ssn"],
        )
        result = mask_pii_columns(df, pii_columns=["ssn"], mask_strategy="alternate")
        pdf = result.toPandas()
        assert pdf["ssn"].iloc[0] == "1*3*5"
        assert pdf["ssn"].iloc[1] == "6*8*0"
        # Non-PII column should be unchanged
        assert pdf["name"].iloc[0] == "Hello"

    def test_mask_pii_columns_hash(self, spark):
        """Hash masking on Spark DataFrame."""
        df = spark.createDataFrame([(1, "secret")], ["id", "value"])
        result = mask_pii_columns(df, pii_columns=["value"], mask_strategy="hash")
        pdf = result.toPandas()
        assert len(pdf["value"].iloc[0]) == 64

    def test_mask_pii_columns_redact(self, spark):
        """Redact masking on Spark DataFrame."""
        df = spark.createDataFrame([(1, "secret")], ["id", "value"])
        result = mask_pii_columns(df, pii_columns=["value"], mask_strategy="redact")
        pdf = result.toPandas()
        assert pdf["value"].iloc[0] == "***"

    def test_mask_pii_columns_partial(self, spark):
        """Partial masking on Spark DataFrame."""
        df = spark.createDataFrame([(1, "Hello")], ["id", "value"])
        result = mask_pii_columns(df, pii_columns=["value"], mask_strategy="partial")
        pdf = result.toPandas()
        assert pdf["value"].iloc[0] == "H***o"

    def test_mask_pii_columns_with_nulls(self, spark):
        """PII masking should handle nulls gracefully."""
        df = spark.createDataFrame([(1, "secret"), (2, None)], ["id", "value"])
        result = mask_pii_columns(df, pii_columns=["value"], mask_strategy="alternate")
        pdf = result.toPandas()
        assert pdf["value"].iloc[0] == "s*c*e*"
        assert pd.isna(pdf["value"].iloc[1])

    def test_mask_pii_columns_missing_column_skipped(self, spark):
        """Missing PII column should be skipped with warning, not error."""
        df = spark.createDataFrame([(1, "secret")], ["id", "value"])
        result = mask_pii_columns(df, pii_columns=["nonexistent"])
        pdf = result.toPandas()
        assert pdf["value"].iloc[0] == "secret"

    def test_compare_dataframes_pii_in_mismatch_columns_string(self, spark):
        """PII values should be masked inside the mismatch_columns string."""
        source = spark.createDataFrame(
            [(1, "123-45-6789", "Alice")],
            ["id", "ssn", "name"],
        )
        target = spark.createDataFrame(
            [(1, "987-65-4321", "Alice")],
            ["id", "ssn", "name"],
        )
        result = compare_dataframes(
            source, target, key_columns=["id"],
            pii_columns=["ssn"], mask_strategy="alternate",
        )
        assert result.mismatch_records is not None
        mismatch_pdf = result.mismatch_records.toPandas()
        mc = mismatch_pdf["mismatch_columns"].iloc[0]
        # Raw SSN values should NOT appear in the string
        assert "123-45-6789" not in mc
        assert "987-65-4321" not in mc

    def test_compare_dataframes_pii_masks_extras(self, spark):
        """PII columns in source/target extras should also be masked."""
        source = spark.createDataFrame(
            [(1, "111-11-1111"), (2, "222-22-2222")],
            ["id", "ssn"],
        )
        target = spark.createDataFrame(
            [(1, "999-99-9999"), (3, "333-33-3333")],
            ["id", "ssn"],
        )
        result = compare_dataframes(
            source, target, key_columns=["id"],
            pii_columns=["ssn"], mask_strategy="redact",
        )
        # Source extra (id=2) should have masked SSN
        assert result.source_extra_records is not None
        src_pdf = result.source_extra_records.toPandas()
        assert src_pdf["ssn"].iloc[0] == "***"

        # Target extra (id=3) should have masked SSN
        assert result.target_extra_records is not None
        tgt_pdf = result.target_extra_records.toPandas()
        assert tgt_pdf["ssn"].iloc[0] == "***"


@pytest.mark.spark
class TestCleaningSpark:
    """Tests for cleaning utilities with PySpark DataFrames."""

    def test_trim_whitespace(self, spark):
        """Trim whitespace from string columns in Spark DataFrame."""
        df = spark.createDataFrame(
            [(1, "  Alice  ", " alice@test.com "), (2, "Bob  ", "bob@test.com")],
            ["id", "name", "email"],
        )
        result = trim_whitespace(df)
        pdf = result.toPandas()
        assert pdf["name"].iloc[0] == "Alice"
        assert pdf["name"].iloc[1] == "Bob"
        assert pdf["email"].iloc[0] == "alice@test.com"

    def test_trim_whitespace_preserves_non_string(self, spark):
        """Non-string columns should be unaffected by trim."""
        df = spark.createDataFrame([(1, "  Alice  ", 100.0)], ["id", "name", "amount"])
        result = trim_whitespace(df)
        pdf = result.toPandas()
        assert pdf["amount"].iloc[0] == 100.0

    def test_clean_control_characters(self, spark):
        """Remove control characters from Spark DataFrame."""
        df = spark.createDataFrame(
            [(1, "Alice\r\n", "alice@test.com\n"), (2, "Bob\t", "bob@test.com")],
            ["id", "name", "email"],
        )
        result = clean_control_characters(df)
        pdf = result.toPandas()
        assert pdf["name"].iloc[0] == "Alice"
        assert pdf["name"].iloc[1] == "Bob"
        assert pdf["email"].iloc[0] == "alice@test.com"

    def test_clean_control_characters_custom_pattern(self, spark):
        """Custom regex pattern should be used for cleaning."""
        df = spark.createDataFrame([(1, "Hello-World")], ["id", "name"])
        result = clean_control_characters(df, pattern=r"-")
        pdf = result.toPandas()
        assert pdf["name"].iloc[0] == "HelloWorld"

    def test_clean_then_trim(self, spark):
        """Pipeline: clean control chars then trim whitespace."""
        df = spark.createDataFrame(
            [(1, "  Alice\n  "), (2, "  Bob\t")],
            ["id", "name"],
        )
        result = clean_control_characters(df)
        result = trim_whitespace(result)
        pdf = result.toPandas()
        assert pdf["name"].iloc[0] == "Alice"
        assert pdf["name"].iloc[1] == "Bob"


@pytest.mark.spark
class TestDataframeHelpersSpark:
    """Tests for DataFrame helper utilities with PySpark DataFrames."""

    def test_normalize_columns(self, spark):
        """Normalize column names: lowercase, strip, replace spaces."""
        df = spark.createDataFrame([(1, "Alice")], ["  ID  ", "Full Name"])
        result = normalize_columns(df)
        assert result.columns == ["id", "full_name"]

    def test_cast_all_to_string(self, spark):
        """Cast all columns to string, preserving nulls."""
        df = spark.createDataFrame([(1, 100.5, "Alice"), (2, None, None)], ["id", "amount", "name"])
        result = cast_all_to_string(df)
        pdf = result.toPandas()
        # All values should be strings
        assert pdf["id"].iloc[0] == "1"
        assert pdf["amount"].iloc[0] == "100.5"
        # Nulls should stay null (toPandas converts Spark null to NaN)
        assert pd.isna(pdf["amount"].iloc[1])
        assert pd.isna(pdf["name"].iloc[1])

    def test_cast_all_to_string_and_compare(self, spark):
        """Casting to string and comparing should work."""
        source = spark.createDataFrame([(1, 100), (2, 200)], ["id", "amount"])
        target = spark.createDataFrame([(1, 100), (2, 250)], ["id", "amount"])
        source_str = cast_all_to_string(source)
        target_str = cast_all_to_string(target)
        result = compare_dataframes(source_str, target_str, key_columns=["id"])
        assert result.mismatch_count == 1


@pytest.mark.spark
class TestReporterSpark:
    """Tests for reporter functions with PySpark DataFrames."""

    def test_build_mismatch_report(self, spark):
        """Build mismatch report for Spark DataFrames."""
        source = spark.createDataFrame(
            [(1, "Alice", 10), (2, "Bob", 20), (3, "Charlie", 30)],
            ["id", "name", "val"],
        )
        target = spark.createDataFrame(
            [(1, "Alice", 10), (2, "Bobby", 20), (3, "Chuck", 99)],
            ["id", "name", "val"],
        )
        report = build_mismatch_report(source, target, key_columns=["id"])
        pdf = report.toPandas()
        assert len(pdf) == 2  # ids 2 and 3

    def test_build_mismatch_report_report_columns_source(self, spark):
        """Build mismatch report with report_columns='source'."""
        source = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        target = spark.createDataFrame([(1, "Bob")], ["id", "name"])
        report = build_mismatch_report(source, target, key_columns=["id"], report_columns="source")
        pdf = report.toPandas()
        assert pdf["name"].iloc[0] == "Alice"

    def test_build_mismatch_report_report_columns_both(self, spark):
        """Build mismatch report with report_columns='both'."""
        source = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        target = spark.createDataFrame([(1, "Bob")], ["id", "name"])
        report = build_mismatch_report(source, target, key_columns=["id"], report_columns="both")
        cols = report.columns
        assert "name_source" in cols
        assert "name_target" in cols

    def test_get_schema_diff(self, spark):
        """Schema diff on Spark DataFrames."""
        source = spark.createDataFrame([(1, "A")], ["id", "name"])
        target = spark.createDataFrame([(1, 1)], ["id", "extra"])
        diff = get_schema_diff(source, target)
        assert "name" in diff.source_only_columns
        assert "extra" in diff.target_only_columns

    def test_get_row_counts(self, spark):
        """Row count comparison on Spark DataFrames."""
        source = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        target = spark.createDataFrame([(1,), (2,)], ["id"])
        counts = get_row_counts(source, target)
        assert counts["source_count"] == 3
        assert counts["target_count"] == 2
        assert counts["difference"] == 1
        assert counts["is_count_match"] is False

    def test_get_duplicate_report(self, spark):
        """Duplicate detection on Spark DataFrames."""
        df = spark.createDataFrame(
            [(1, "Alice"), (1, "Alice2"), (2, "Bob")],
            ["id", "name"],
        )
        dups = get_duplicate_report(df, key_columns=["id"])
        pdf = dups.toPandas()
        assert len(pdf) == 2
        assert set(pdf["id"].tolist()) == {1}

    def test_get_null_analysis(self, spark):
        """Null analysis on Spark DataFrames."""
        df = spark.createDataFrame(
            [(1, "Alice", None), (2, None, "bob@test.com"), (3, None, None)],
            ["id", "name", "email"],
        )
        analysis = get_null_analysis(df)
        assert analysis["name"]["null_count"] == 2
        assert analysis["name"]["total_count"] == 3
        assert analysis["email"]["null_count"] == 2


@pytest.mark.spark
class TestEndToEndSpark:
    """End-to-end integration tests mirroring pandas workflow with PySpark."""

    def test_full_workflow_clean_normalize_compare(self, spark):
        """Full workflow: clean, normalize, compare, check results."""
        source_raw = spark.createDataFrame(
            [
                (1, "  Alice\n", "alice@test.com", 100.0),
                (2, "Bob\t", "bob@test.com", 200.0),
                (3, "  Charlie  ", "charlie@test.com", 300.0),
                (4, "Diana", "diana@test.com", 400.0),
            ],
            ["ID", "Name", "Email", "Amount"],
        )
        target = spark.createDataFrame(
            [
                (1, "Alice", "alice@test.com", 100.0),
                (2, "Bobby", "bob_new@test.com", 250.0),
                (3, "Charlie", "charlie@test.com", 300.0),
                (5, "Eve", "eve@test.com", 500.0),
            ],
            ["id", "name", "email", "amount"],
        )

        # Clean source
        source = clean_control_characters(source_raw)
        source = trim_whitespace(source)
        source = normalize_columns(source)

        result = compare_dataframes(source, target, key_columns=["id"])

        assert result.total_source_rows == 4
        assert result.total_target_rows == 4
        assert result.is_match is False
        assert result.mismatch_count == 1  # id=2
        assert result.source_extra_count == 1  # id=4
        assert result.target_extra_count == 1  # id=5
        assert result.matched_count == 2  # ids 1, 3

    def test_pii_masking_full_workflow(self, spark):
        """Full workflow with PII masking using hash strategy."""
        source = spark.createDataFrame(
            [(1, "123-45-6789", "Alice"), (2, "987-65-4321", "Bob")],
            ["id", "ssn", "name"],
        )
        target = spark.createDataFrame(
            [(1, "111-22-3333", "Alice"), (2, "444-55-6666", "Bob")],
            ["id", "ssn", "name"],
        )
        result = compare_dataframes(
            source, target, key_columns=["id"],
            pii_columns=["ssn"], mask_strategy="hash",
        )
        assert result.mismatch_count == 2
        mismatch_pdf = result.mismatch_records.toPandas()
        # SSN data column should be hashed
        assert len(mismatch_pdf["ssn"].iloc[0]) == 64
        # mismatch_columns string should also have hashed SSN values
        mc = mismatch_pdf["mismatch_columns"].iloc[0]
        assert "123-45-6789" not in mc
        assert "111-22-3333" not in mc

    def test_pandas_spark_results_agree(self, spark):
        """Pandas and Spark paths should produce equivalent validation results."""
        import pandas as pd

        data_source = [(1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0)]
        data_target = [(1, "Alice", 100.0), (2, "Bobby", 250.0), (4, "Diana", 400.0)]

        # Pandas DataFrames
        pd_source = pd.DataFrame(data_source, columns=["id", "name", "amount"])
        pd_target = pd.DataFrame(data_target, columns=["id", "name", "amount"])

        # Spark DataFrames
        sp_source = spark.createDataFrame(data_source, ["id", "name", "amount"])
        sp_target = spark.createDataFrame(data_target, ["id", "name", "amount"])

        pd_result = compare_dataframes(pd_source, pd_target, key_columns=["id"])
        sp_result = compare_dataframes(sp_source, sp_target, key_columns=["id"])

        # Both should agree on counts
        assert pd_result.is_match == sp_result.is_match
        assert pd_result.total_source_rows == sp_result.total_source_rows
        assert pd_result.total_target_rows == sp_result.total_target_rows
        assert pd_result.matched_count == sp_result.matched_count
        assert pd_result.mismatch_count == sp_result.mismatch_count
        assert pd_result.source_extra_count == sp_result.source_extra_count
        assert pd_result.target_extra_count == sp_result.target_extra_count
