"""Tests for cleaning/sanitizer.py."""

import pandas as pd
import pytest

from databridge_validator.cleaning.sanitizer import clean_control_characters, trim_whitespace


class TestTrimWhitespace:
    def test_trims_leading_and_trailing_spaces(self, df_with_whitespace):
        result = trim_whitespace(df_with_whitespace)
        assert result["name"].tolist() == ["Alice", "Bob", "Charlie"]
        assert result["email"].tolist() == ["alice@test.com", "bob@test.com", "charlie@test.com"]

    def test_preserves_non_string_columns(self):
        df = pd.DataFrame({"id": [1, 2], "name": ["  A  ", "B  "], "amount": [1.5, 2.5]})
        result = trim_whitespace(df)
        assert result["id"].tolist() == [1, 2]
        assert result["amount"].tolist() == [1.5, 2.5]
        assert result["name"].tolist() == ["A", "B"]

    def test_does_not_mutate_original(self, df_with_whitespace):
        original_values = df_with_whitespace["name"].tolist()
        trim_whitespace(df_with_whitespace)
        assert df_with_whitespace["name"].tolist() == original_values

    def test_handles_empty_dataframe(self):
        df = pd.DataFrame({"name": pd.Series([], dtype="str")})
        result = trim_whitespace(df)
        assert len(result) == 0

    def test_handles_null_values(self):
        df = pd.DataFrame({"name": ["  Alice  ", None, "  Charlie  "]})
        result = trim_whitespace(df)
        assert result["name"].iloc[0] == "Alice"
        assert pd.isna(result["name"].iloc[1])
        assert result["name"].iloc[2] == "Charlie"

    def test_whitespace_only_becomes_empty(self):
        df = pd.DataFrame({"name": ["   ", "  \t  ", "valid"]})
        result = trim_whitespace(df)
        assert result["name"].iloc[0] == ""
        assert result["name"].iloc[2] == "valid"

    def test_rejects_non_dataframe(self):
        with pytest.raises(TypeError, match="must be a pandas or PySpark DataFrame"):
            trim_whitespace({"a": 1})


class TestCleanControlCharacters:
    def test_removes_newlines_and_tabs(self, df_with_control_chars):
        result = clean_control_characters(df_with_control_chars)
        assert result["name"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_removes_carriage_returns(self):
        df = pd.DataFrame({"data": ["hello\r\nworld", "foo\rbar"]})
        result = clean_control_characters(df)
        assert result["data"].tolist() == ["helloworld", "foobar"]

    def test_preserves_non_string_columns(self):
        df = pd.DataFrame({"id": [1, 2], "name": ["A\n", "B\t"]})
        result = clean_control_characters(df)
        assert result["id"].tolist() == [1, 2]
        assert result["name"].tolist() == ["A", "B"]

    def test_does_not_mutate_original(self, df_with_control_chars):
        original_values = df_with_control_chars["name"].tolist()
        clean_control_characters(df_with_control_chars)
        assert df_with_control_chars["name"].tolist() == original_values

    def test_custom_pattern(self):
        df = pd.DataFrame({"name": ["Alice!", "Bob?"]})
        result = clean_control_characters(df, pattern=r"[!?]")
        assert result["name"].tolist() == ["Alice", "Bob"]

    def test_handles_null_values(self):
        df = pd.DataFrame({"name": ["Alice\n", None, "Charlie\t"]})
        result = clean_control_characters(df)
        assert result["name"].iloc[0] == "Alice"
        assert pd.isna(result["name"].iloc[1])
        assert result["name"].iloc[2] == "Charlie"

    def test_handles_empty_dataframe(self):
        df = pd.DataFrame({"name": pd.Series([], dtype="str")})
        result = clean_control_characters(df)
        assert len(result) == 0

    def test_default_pattern_removes_zero_width_space(self):
        df = pd.DataFrame({"name": ["hello\u200bworld"]})
        result = clean_control_characters(df)
        assert result["name"].iloc[0] == "helloworld"

    def test_rejects_non_dataframe(self):
        with pytest.raises(TypeError, match="must be a pandas or PySpark DataFrame"):
            clean_control_characters([1, 2, 3])
