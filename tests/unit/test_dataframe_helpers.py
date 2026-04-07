"""Tests for utils/dataframe_helpers.py."""

import pandas as pd
import pytest

from databridge_validator.utils.dataframe_helpers import (
    _is_spark_dataframe,
    cast_all_to_string,
    normalize_columns,
    validate_dataframe_type,
    validate_key_columns,
)


class TestIsSparkDataframe:
    def test_pandas_dataframe_returns_false(self):
        df = pd.DataFrame({"a": [1]})
        assert _is_spark_dataframe(df) is False

    def test_dict_returns_false(self):
        assert _is_spark_dataframe({"a": 1}) is False

    def test_none_returns_false(self):
        assert _is_spark_dataframe(None) is False

    def test_string_returns_false(self):
        assert _is_spark_dataframe("not a dataframe") is False


class TestValidateDataframeType:
    def test_accepts_pandas_dataframe(self):
        df = pd.DataFrame({"a": [1]})
        validate_dataframe_type(df, "test_df")

    def test_rejects_dict(self):
        with pytest.raises(TypeError, match="test_df must be a pandas or PySpark DataFrame"):
            validate_dataframe_type({"a": 1}, "test_df")

    def test_rejects_none(self):
        with pytest.raises(TypeError, match="source must be a pandas or PySpark DataFrame"):
            validate_dataframe_type(None, "source")

    def test_rejects_list(self):
        with pytest.raises(TypeError, match="must be a pandas or PySpark DataFrame"):
            validate_dataframe_type([1, 2, 3], "df")


class TestValidateKeyColumns:
    def test_all_keys_present(self):
        df = pd.DataFrame({"id": [1], "name": ["A"]})
        validate_key_columns(df, ["id"])

    def test_missing_key_raises_value_error(self):
        df = pd.DataFrame({"id": [1], "name": ["A"]})
        with pytest.raises(ValueError, match="Key columns not found"):
            validate_key_columns(df, ["id", "missing_col"])

    def test_empty_keys_passes(self):
        df = pd.DataFrame({"id": [1]})
        validate_key_columns(df, [])

    def test_multiple_keys_present(self):
        df = pd.DataFrame({"id": [1], "name": ["A"], "email": ["a@b.com"]})
        validate_key_columns(df, ["id", "name"])


class TestNormalizeColumns:
    def test_lowercases_column_names(self):
        df = pd.DataFrame({"ID": [1], "Name": ["A"], "EMAIL": ["a@b.com"]})
        result = normalize_columns(df)
        assert list(result.columns) == ["id", "name", "email"]

    def test_strips_whitespace_from_column_names(self):
        df = pd.DataFrame({" id ": [1], "name ": ["A"]})
        result = normalize_columns(df)
        assert list(result.columns) == ["id", "name"]

    def test_does_not_mutate_original(self):
        df = pd.DataFrame({"ID": [1], "Name": ["A"]})
        normalize_columns(df)
        assert list(df.columns) == ["ID", "Name"]

    def test_replaces_spaces_with_underscores(self):
        df = pd.DataFrame({"first name": [1], "last name": [2]})
        result = normalize_columns(df)
        assert list(result.columns) == ["first_name", "last_name"]

    def test_empty_dataframe_preserves_columns(self):
        df = pd.DataFrame({"ID": pd.Series([], dtype="int64")})
        result = normalize_columns(df)
        assert list(result.columns) == ["id"]


class TestCastAllToString:
    def test_casts_int_columns(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = cast_all_to_string(df)
        assert pd.api.types.is_string_dtype(result["id"])
        assert result["id"].tolist() == ["1", "2", "3"]

    def test_casts_float_columns(self):
        df = pd.DataFrame({"amount": [1.5, 2.5]})
        result = cast_all_to_string(df)
        assert pd.api.types.is_string_dtype(result["amount"])

    def test_preserves_string_columns(self):
        df = pd.DataFrame({"name": ["Alice", "Bob"]})
        result = cast_all_to_string(df)
        assert result["name"].tolist() == ["Alice", "Bob"]

    def test_handles_null_values(self):
        df = pd.DataFrame({"name": ["Alice", None, "Charlie"]})
        result = cast_all_to_string(df)
        assert pd.isna(result["name"].iloc[1])

    def test_does_not_mutate_original(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        cast_all_to_string(df)
        assert not pd.api.types.is_string_dtype(df["id"])

    def test_empty_dataframe(self):
        df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
        result = cast_all_to_string(df)
        assert pd.api.types.is_string_dtype(result["id"])

    def test_mixed_types(self):
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "amount": [1.0, 2.0]})
        result = cast_all_to_string(df)
        for col in result.columns:
            assert pd.api.types.is_string_dtype(result[col])
