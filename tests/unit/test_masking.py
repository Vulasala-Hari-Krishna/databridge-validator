"""Tests for pii/masking.py."""

import pandas as pd
import pytest

from databridge_validator.pii.masking import (
    mask_alternate_chars,
    mask_partial,
    mask_pii_columns,
    mask_redact,
    mask_with_hash,
)


class TestMaskAlternateChars:
    def test_basic_string(self):
        assert mask_alternate_chars("Hello") == "H*l*o"

    def test_empty_string(self):
        assert mask_alternate_chars("") == ""

    def test_none_input(self):
        assert mask_alternate_chars(None) is None

    def test_single_char(self):
        assert mask_alternate_chars("A") == "A"

    def test_numeric_string(self):
        assert mask_alternate_chars("123456") == "1*3*5*"

    def test_two_chars(self):
        assert mask_alternate_chars("AB") == "A*"

    def test_special_characters(self):
        assert mask_alternate_chars("a@b.c") == "a*b*c"

    def test_spaces_in_string(self):
        result = mask_alternate_chars("A B")
        assert result == "A*B"


class TestMaskWithHash:
    def test_basic_string(self):
        result = mask_with_hash("Hello")
        assert result != "Hello"
        assert len(result) == 64  # SHA-256 hex digest

    def test_deterministic(self):
        assert mask_with_hash("test") == mask_with_hash("test")

    def test_none_input(self):
        assert mask_with_hash(None) is None

    def test_empty_string(self):
        result = mask_with_hash("")
        assert len(result) == 64

    def test_different_inputs_different_hashes(self):
        assert mask_with_hash("Alice") != mask_with_hash("Bob")


class TestMaskPartial:
    def test_basic_string(self):
        result = mask_partial("Hello")
        assert result[0] == "H"
        assert result[-1] == "o"
        assert "*" in result

    def test_none_input(self):
        assert mask_partial(None) is None

    def test_empty_string(self):
        assert mask_partial("") == ""

    def test_single_char(self):
        assert mask_partial("A") == "A"

    def test_two_chars(self):
        assert mask_partial("AB") == "AB"

    def test_custom_visible_chars(self):
        result = mask_partial("Hello World", visible_chars=2)
        assert result[:2] == "He"
        assert result[-2:] == "ld"

    def test_short_string_with_large_visible(self):
        result = mask_partial("Hi", visible_chars=5)
        assert result == "Hi"


class TestMaskRedact:
    def test_basic_string(self):
        assert mask_redact("Hello") == "***"

    def test_none_input(self):
        assert mask_redact(None) is None

    def test_empty_string(self):
        assert mask_redact("") == "***"

    def test_custom_replacement(self):
        assert mask_redact("Hello", replacement="[REDACTED]") == "[REDACTED]"


class TestMaskPiiColumns:
    def test_masks_specified_columns(self):
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "ssn": ["123-45-6789", "987-65-4321", "111-22-3333"],
            }
        )
        result = mask_pii_columns(df, pii_columns=["ssn"])
        assert result["ssn"].iloc[0] != "123-45-6789"
        assert result["id"].tolist() == [1, 2, 3]
        assert result["name"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_does_not_mutate_original(self):
        df = pd.DataFrame({"ssn": ["123-45-6789"]})
        mask_pii_columns(df, pii_columns=["ssn"])
        assert df["ssn"].iloc[0] == "123-45-6789"

    def test_alternate_strategy(self):
        df = pd.DataFrame({"name": ["Alice"]})
        result = mask_pii_columns(df, pii_columns=["name"], mask_strategy="alternate")
        assert result["name"].iloc[0] == "A*i*e"

    def test_hash_strategy(self):
        df = pd.DataFrame({"name": ["Alice"]})
        result = mask_pii_columns(df, pii_columns=["name"], mask_strategy="hash")
        assert len(result["name"].iloc[0]) == 64

    def test_redact_strategy(self):
        df = pd.DataFrame({"name": ["Alice"]})
        result = mask_pii_columns(df, pii_columns=["name"], mask_strategy="redact")
        assert result["name"].iloc[0] == "***"

    def test_partial_strategy(self):
        df = pd.DataFrame({"name": ["Alice"]})
        result = mask_pii_columns(df, pii_columns=["name"], mask_strategy="partial")
        assert result["name"].iloc[0][0] == "A"
        assert result["name"].iloc[0][-1] == "e"

    def test_handles_null_values(self):
        df = pd.DataFrame({"name": ["Alice", None, "Charlie"]})
        result = mask_pii_columns(df, pii_columns=["name"])
        assert pd.isna(result["name"].iloc[1])

    def test_invalid_strategy_raises(self):
        df = pd.DataFrame({"name": ["Alice"]})
        with pytest.raises(ValueError, match="Unknown mask_strategy"):
            mask_pii_columns(df, pii_columns=["name"], mask_strategy="invalid")

    def test_missing_column_logs_warning(self):
        df = pd.DataFrame({"name": ["Alice"]})
        result = mask_pii_columns(df, pii_columns=["nonexistent"])
        assert result["name"].iloc[0] == "Alice"

    def test_multiple_pii_columns(self):
        df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "ssn": ["123-45-6789"],
                "email": ["alice@test.com"],
            }
        )
        result = mask_pii_columns(df, pii_columns=["ssn", "email"])
        assert result["ssn"].iloc[0] != "123-45-6789"
        assert result["email"].iloc[0] != "alice@test.com"
        assert result["name"].iloc[0] == "Alice"

    def test_rejects_non_dataframe(self):
        with pytest.raises(TypeError, match="must be a pandas or PySpark DataFrame"):
            mask_pii_columns({"a": 1}, pii_columns=["a"])

    def test_empty_dataframe(self):
        df = pd.DataFrame({"name": pd.Series([], dtype="str")})
        result = mask_pii_columns(df, pii_columns=["name"])
        assert len(result) == 0
