"""Tests for the legacy-style mismatch_columns format in comparator and reporter."""

import pandas as pd
import pytest

from databridge_validator.core.comparator import compare_dataframes
from databridge_validator.core.reporter import build_mismatch_report


class TestMismatchColumnsFormat:
    """Mismatch report should have a `mismatch_columns` string column
    in the format: [{col : (src_val:tgt_val)}, ...]"""

    def test_default_mismatch_report_has_mismatch_columns_col(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"], "amount": [100.0]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"], "amount": [200.0]})
        result = compare_dataframes(source, target, key_columns=["id"])
        mismatch = result.mismatch_records
        assert "mismatch_columns" in mismatch.columns

    def test_mismatch_columns_string_format(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"], "amount": [100.0]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"], "amount": [100.0]})
        result = compare_dataframes(source, target, key_columns=["id"])
        mismatch = result.mismatch_records
        val = mismatch["mismatch_columns"].iloc[0]
        # Should contain the legacy format
        assert "{name : (Alice:Bob)}" in val

    def test_mismatch_columns_only_lists_differing_cols(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"], "val": ["A"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"], "val": ["A"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        val = result.mismatch_records["mismatch_columns"].iloc[0]
        assert "name" in val
        assert "val" not in val

    def test_mismatch_columns_with_null_values(self):
        source = pd.DataFrame({"id": [1], "name": [None]})
        target = pd.DataFrame({"id": [1], "name": ["Alice"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        val = result.mismatch_records["mismatch_columns"].iloc[0]
        assert "{name : (null:Alice)}" in val

    def test_mismatch_columns_multiple_diffs(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"], "email": ["a@b.com"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"], "email": ["x@y.com"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        val = result.mismatch_records["mismatch_columns"].iloc[0]
        assert "{name : (Alice:Bob)}" in val
        assert "{email : (a@b.com:x@y.com)}" in val


class TestMismatchReportColumnsParam:
    """The `report_columns` param controls which data columns appear
    alongside key_columns and mismatch_columns in the mismatch DataFrame.

    - "target" (default): key_columns + target df columns + mismatch_columns
    - "source": key_columns + source df columns + mismatch_columns
    - "both": key_columns + source df columns + target df columns + mismatch_columns
    """

    def test_default_report_columns_is_target(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"], "val": ["A"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"], "val": ["X"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        mismatch = result.mismatch_records
        # Default: key + target cols + mismatch_columns
        assert "id" in mismatch.columns
        assert "name" in mismatch.columns
        assert "val" in mismatch.columns
        assert "mismatch_columns" in mismatch.columns
        # No _source/_target suffixes
        assert "name_source" not in mismatch.columns
        assert "name_target" not in mismatch.columns

    def test_default_values_come_from_target(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        mismatch = result.mismatch_records
        # Values should be from target
        assert mismatch["name"].iloc[0] == "Bob"

    def test_report_columns_source(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"]})
        result = compare_dataframes(source, target, key_columns=["id"], report_columns="source")
        mismatch = result.mismatch_records
        assert "name" in mismatch.columns
        assert mismatch["name"].iloc[0] == "Alice"
        assert "mismatch_columns" in mismatch.columns

    def test_report_columns_both(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"]})
        result = compare_dataframes(source, target, key_columns=["id"], report_columns="both")
        mismatch = result.mismatch_records
        assert "name_source" in mismatch.columns
        assert "name_target" in mismatch.columns
        assert "mismatch_columns" in mismatch.columns
        assert mismatch["name_source"].iloc[0] == "Alice"
        assert mismatch["name_target"].iloc[0] == "Bob"

    def test_invalid_report_columns_raises(self):
        source = pd.DataFrame({"id": [1], "name": ["A"]})
        target = pd.DataFrame({"id": [1], "name": ["B"]})
        with pytest.raises(ValueError, match="report_columns must be one of"):
            compare_dataframes(source, target, key_columns=["id"], report_columns="invalid")


class TestMismatchReportExcludeColumns:
    def test_excluded_columns_not_in_mismatch_columns_string(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"], "ts": ["2024-01"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"], "ts": ["2024-12"]})
        result = compare_dataframes(source, target, key_columns=["id"], exclude_columns=["ts"])
        mismatch = result.mismatch_records
        val = mismatch["mismatch_columns"].iloc[0]
        assert "ts" not in val
        assert "name" in val


class TestMismatchReportPiiMasking:
    def test_pii_masked_in_mismatch_columns_string(self):
        source = pd.DataFrame({"id": [1], "ssn": ["123-45-6789"], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "ssn": ["987-65-4321"], "name": ["Bob"]})
        result = compare_dataframes(source, target, key_columns=["id"], pii_columns=["ssn"])
        mismatch = result.mismatch_records
        val = mismatch["mismatch_columns"].iloc[0]
        # SSN values should be masked in the mismatch_columns string
        assert "123-45-6789" not in val
        assert "987-65-4321" not in val
        # But the ssn column entry should still be present
        assert "ssn" in val

    def test_pii_masked_in_data_columns_too(self):
        source = pd.DataFrame({"id": [1], "ssn": ["123-45-6789"]})
        target = pd.DataFrame({"id": [1], "ssn": ["987-65-4321"]})
        result = compare_dataframes(source, target, key_columns=["id"], pii_columns=["ssn"])
        mismatch = result.mismatch_records
        # The ssn data column should also be masked
        assert mismatch["ssn"].iloc[0] != "987-65-4321"
        assert mismatch["ssn"].iloc[0] != "123-45-6789"


class TestBuildMismatchReportLegacyFormat:
    """build_mismatch_report() should also produce the legacy format."""

    def test_default_has_mismatch_columns(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"]})
        report = build_mismatch_report(source, target, key_columns=["id"])
        assert "mismatch_columns" in report.columns

    def test_report_columns_target(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"]})
        report = build_mismatch_report(source, target, key_columns=["id"], report_columns="target")
        assert report["name"].iloc[0] == "Bob"

    def test_report_columns_source(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"]})
        report = build_mismatch_report(source, target, key_columns=["id"], report_columns="source")
        assert report["name"].iloc[0] == "Alice"

    def test_report_columns_both(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"]})
        report = build_mismatch_report(source, target, key_columns=["id"], report_columns="both")
        assert "name_source" in report.columns
        assert "name_target" in report.columns
