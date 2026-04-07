"""Tests for core/models.py."""

import pandas as pd

from databridge_validator.core.models import ColumnMismatch, SchemaDiff, ValidationResult


class TestValidationResult:
    def test_creation_with_defaults(self):
        result = ValidationResult(
            is_match=True,
            total_source_rows=10,
            total_target_rows=10,
            matched_count=10,
            mismatch_count=0,
            source_extra_count=0,
            target_extra_count=0,
        )
        assert result.is_match is True
        assert result.mismatch_records is None
        assert result.source_extra_records is None
        assert result.target_extra_records is None
        assert result.summary == {}
        assert result.metadata == {}

    def test_to_dict_excludes_dataframes(self):
        df = pd.DataFrame({"id": [1, 2]})
        result = ValidationResult(
            is_match=False,
            total_source_rows=5,
            total_target_rows=5,
            matched_count=3,
            mismatch_count=1,
            source_extra_count=1,
            target_extra_count=0,
            mismatch_records=df,
        )
        d = result.to_dict()
        assert "mismatch_records" not in d
        assert d["is_match"] is False
        assert d["mismatch_count"] == 1

    def test_str_representation(self):
        result = ValidationResult(
            is_match=False,
            total_source_rows=100,
            total_target_rows=90,
            matched_count=80,
            mismatch_count=5,
            source_extra_count=15,
            target_extra_count=5,
        )
        s = str(result)
        assert "is_match=False" in s
        assert "source_rows=100" in s
        assert "target_rows=90" in s
        assert "matched=80" in s
        assert "mismatched=5" in s
        assert "source_extras=15" in s
        assert "target_extras=5" in s

    def test_is_match_true_for_identical(self):
        result = ValidationResult(
            is_match=True,
            total_source_rows=5,
            total_target_rows=5,
            matched_count=5,
            mismatch_count=0,
            source_extra_count=0,
            target_extra_count=0,
        )
        assert result.is_match is True

    def test_summary_and_metadata_are_independent(self):
        r1 = ValidationResult(
            is_match=True, total_source_rows=0, total_target_rows=0,
            matched_count=0, mismatch_count=0, source_extra_count=0, target_extra_count=0,
        )
        r2 = ValidationResult(
            is_match=True, total_source_rows=0, total_target_rows=0,
            matched_count=0, mismatch_count=0, source_extra_count=0, target_extra_count=0,
        )
        r1.summary["key"] = "value"
        assert "key" not in r2.summary


class TestColumnMismatch:
    def test_creation(self):
        cm = ColumnMismatch(column_name="name", source_value="Alice", target_value="Bob")
        assert cm.column_name == "name"
        assert cm.source_value == "Alice"
        assert cm.target_value == "Bob"

    def test_with_none_values(self):
        cm = ColumnMismatch(column_name="email", source_value=None, target_value="a@b.com")
        assert cm.source_value is None


class TestSchemaDiff:
    def test_is_compatible_when_same_columns(self):
        sd = SchemaDiff(common_columns=["id", "name"])
        assert sd.is_compatible is True

    def test_not_compatible_with_source_only(self):
        sd = SchemaDiff(source_only_columns=["extra_col"], common_columns=["id"])
        assert sd.is_compatible is False

    def test_not_compatible_with_target_only(self):
        sd = SchemaDiff(target_only_columns=["extra_col"], common_columns=["id"])
        assert sd.is_compatible is False

    def test_type_mismatches(self):
        sd = SchemaDiff(
            common_columns=["id"],
            type_mismatches={"id": ("int64", "string")},
        )
        assert sd.type_mismatches["id"] == ("int64", "string")
