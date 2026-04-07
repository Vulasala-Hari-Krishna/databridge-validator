"""Tests for core/comparator.py."""

import pandas as pd
import pytest

from databridge_validator.core.comparator import compare_dataframes
from databridge_validator.core.models import ValidationResult


class TestCompareDataframesIdentical:
    def test_identical_data_returns_zero_mismatches(self):
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        result = compare_dataframes(df, df.copy(), key_columns=["id"])
        assert isinstance(result, ValidationResult)
        assert result.is_match is True
        assert result.mismatch_count == 0
        assert result.source_extra_count == 0
        assert result.target_extra_count == 0
        assert result.matched_count == 3

    def test_identical_single_row(self, single_row_df):
        result = compare_dataframes(single_row_df, single_row_df.copy(), key_columns=["id"])
        assert result.is_match is True
        assert result.matched_count == 1


class TestCompareDataframesMismatches:
    def test_detects_value_mismatches(self, sample_source_df, sample_target_df):
        result = compare_dataframes(sample_source_df, sample_target_df, key_columns=["id"])
        assert result.is_match is False
        assert result.mismatch_count > 0

    def test_detects_source_extras(self, sample_source_df, sample_target_df):
        result = compare_dataframes(sample_source_df, sample_target_df, key_columns=["id"])
        assert result.source_extra_count == 2  # ids 4, 5

    def test_detects_target_extras(self, sample_source_df, sample_target_df):
        result = compare_dataframes(sample_source_df, sample_target_df, key_columns=["id"])
        assert result.target_extra_count == 2  # ids 6, 7

    def test_mismatch_records_is_dataframe(self, sample_source_df, sample_target_df):
        result = compare_dataframes(sample_source_df, sample_target_df, key_columns=["id"])
        assert isinstance(result.mismatch_records, pd.DataFrame)

    def test_source_extra_records_contain_right_ids(self, sample_source_df, sample_target_df):
        result = compare_dataframes(sample_source_df, sample_target_df, key_columns=["id"])
        assert isinstance(result.source_extra_records, pd.DataFrame)
        source_extra_ids = sorted(result.source_extra_records["id"].tolist())
        assert source_extra_ids == [4, 5]

    def test_target_extra_records_contain_right_ids(self, sample_source_df, sample_target_df):
        result = compare_dataframes(sample_source_df, sample_target_df, key_columns=["id"])
        assert isinstance(result.target_extra_records, pd.DataFrame)
        target_extra_ids = sorted(result.target_extra_records["id"].tolist())
        assert target_extra_ids == [6, 7]

    def test_all_rows_mismatch(self):
        source = pd.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        target = pd.DataFrame({"id": [1, 2], "val": ["X", "Y"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        assert result.mismatch_count == 2
        assert result.matched_count == 0


class TestCompareDataframesEdgeCases:
    def test_empty_source_returns_all_target_as_extras(self):
        source = pd.DataFrame({"id": pd.Series([], dtype="int64"), "name": pd.Series([], dtype="str")})
        target = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        assert result.total_source_rows == 0
        assert result.target_extra_count == 2
        assert result.source_extra_count == 0

    def test_empty_target_returns_all_source_as_extras(self):
        source = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        target = pd.DataFrame({"id": pd.Series([], dtype="int64"), "name": pd.Series([], dtype="str")})
        result = compare_dataframes(source, target, key_columns=["id"])
        assert result.total_target_rows == 0
        assert result.source_extra_count == 2
        assert result.target_extra_count == 0

    def test_both_empty_returns_match(self):
        source = pd.DataFrame({"id": pd.Series([], dtype="int64"), "name": pd.Series([], dtype="str")})
        target = pd.DataFrame({"id": pd.Series([], dtype="int64"), "name": pd.Series([], dtype="str")})
        result = compare_dataframes(source, target, key_columns=["id"])
        assert result.is_match is True

    def test_handles_null_values_in_data_columns(self):
        source = pd.DataFrame({"id": [1, 2], "name": ["Alice", None]})
        target = pd.DataFrame({"id": [1, 2], "name": ["Alice", None]})
        result = compare_dataframes(source, target, key_columns=["id"])
        assert result.is_match is True

    def test_null_vs_value_detected_as_mismatch(self):
        source = pd.DataFrame({"id": [1], "name": [None]})
        target = pd.DataFrame({"id": [1], "name": ["Alice"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        assert result.mismatch_count == 1

    def test_multiple_key_columns(self):
        source = pd.DataFrame({"id": [1, 1], "date": ["2024-01", "2024-02"], "val": ["A", "B"]})
        target = pd.DataFrame({"id": [1, 1], "date": ["2024-01", "2024-02"], "val": ["A", "X"]})
        result = compare_dataframes(source, target, key_columns=["id", "date"])
        assert result.mismatch_count == 1
        assert result.matched_count == 1


class TestCompareDataframesExcludeColumns:
    def test_exclude_columns_ignores_specified(self):
        source = pd.DataFrame({"id": [1], "name": ["A"], "updated_at": ["2024-01-01"]})
        target = pd.DataFrame({"id": [1], "name": ["A"], "updated_at": ["2024-12-31"]})
        result = compare_dataframes(source, target, key_columns=["id"], exclude_columns=["updated_at"])
        assert result.is_match is True

    def test_exclude_columns_still_detects_other_mismatches(self):
        source = pd.DataFrame({"id": [1], "name": ["A"], "updated_at": ["2024-01-01"]})
        target = pd.DataFrame({"id": [1], "name": ["B"], "updated_at": ["2024-12-31"]})
        result = compare_dataframes(source, target, key_columns=["id"], exclude_columns=["updated_at"])
        assert result.mismatch_count == 1


class TestCompareDataframesCaseSensitivity:
    def test_case_insensitive_column_matching(self):
        source = pd.DataFrame({"ID": [1], "NAME": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Alice"]})
        result = compare_dataframes(source, target, key_columns=["id"], case_sensitive=False)
        assert result.is_match is True

    def test_case_sensitive_column_matching_fails_on_case_diff(self):
        source = pd.DataFrame({"ID": [1], "NAME": ["Alice"]})
        target = pd.DataFrame({"id": [1], "name": ["Alice"]})
        with pytest.raises(ValueError, match="Key columns not found"):
            compare_dataframes(source, target, key_columns=["ID"], case_sensitive=True)


class TestCompareDataframesPiiMasking:
    def test_pii_columns_are_masked_in_mismatch_report(self):
        source = pd.DataFrame({"id": [1], "ssn": ["123-45-6789"], "name": ["Alice"]})
        target = pd.DataFrame({"id": [1], "ssn": ["987-65-4321"], "name": ["Bob"]})
        result = compare_dataframes(source, target, key_columns=["id"], pii_columns=["ssn"])
        mismatch = result.mismatch_records
        assert mismatch is not None
        # Default report_columns="target": ssn data column should be masked
        assert mismatch["ssn"].iloc[0] != "987-65-4321"
        assert mismatch["ssn"].iloc[0] != "123-45-6789"
        # The mismatch_columns string should also have masked SSN values
        val = mismatch["mismatch_columns"].iloc[0]
        assert "123-45-6789" not in val
        assert "987-65-4321" not in val


class TestCompareDataframesInputValidation:
    def test_mismatched_types_raises_type_error(self):
        with pytest.raises(TypeError, match="must be a pandas or PySpark DataFrame"):
            compare_dataframes({"a": 1}, pd.DataFrame(), key_columns=["a"])

    def test_missing_key_columns_raises_value_error(self):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="Key columns not found"):
            compare_dataframes(df, df, key_columns=["nonexistent"])

    def test_type_mismatch_source_target_raises(self):
        with pytest.raises(TypeError):
            compare_dataframes(pd.DataFrame(), [1, 2, 3], key_columns=["a"])


class TestCompareDataframesSummary:
    def test_summary_contains_expected_keys(self, sample_source_df, sample_target_df):
        result = compare_dataframes(sample_source_df, sample_target_df, key_columns=["id"])
        assert "total_source" in result.summary
        assert "total_target" in result.summary
        assert "matched" in result.summary
        assert "mismatched" in result.summary
        assert "source_extras" in result.summary
        assert "target_extras" in result.summary

    def test_row_counts_are_correct(self, sample_source_df, sample_target_df):
        result = compare_dataframes(sample_source_df, sample_target_df, key_columns=["id"])
        assert result.total_source_rows == 5
        assert result.total_target_rows == 5
