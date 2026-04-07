"""Tests for core/reporter.py."""

import pandas as pd
import pytest

from databridge_validator.core.models import ValidationResult
from databridge_validator.core.reporter import (
    build_mismatch_report,
    get_duplicate_report,
    get_null_analysis,
    get_row_counts,
    get_schema_diff,
)


class TestBuildMismatchReport:
    def test_returns_dataframe_with_key_and_diff_columns(self):
        source = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "val": ["A", "B"]})
        target = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bobby"], "val": ["A", "X"]})
        report = build_mismatch_report(source, target, key_columns=["id"])
        assert isinstance(report, pd.DataFrame)
        assert "id" in report.columns

    def test_identifies_mismatched_columns(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"], "val": ["A"]})
        target = pd.DataFrame({"id": [1], "name": ["Bob"], "val": ["A"]})
        report = build_mismatch_report(source, target, key_columns=["id"])
        assert len(report) == 1
        # Default is "target" mode: key + target data columns + mismatch_columns
        assert "name" in report.columns
        assert "mismatch_columns" in report.columns
        assert "name" in report["mismatch_columns"].iloc[0]

    def test_no_mismatches_returns_empty(self):
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        report = build_mismatch_report(df, df.copy(), key_columns=["id"])
        assert len(report) == 0

    def test_excludes_specified_columns(self):
        source = pd.DataFrame({"id": [1], "name": ["Alice"], "ts": ["2024-01"]})
        target = pd.DataFrame({"id": [1], "name": ["Alice"], "ts": ["2024-12"]})
        report = build_mismatch_report(source, target, key_columns=["id"], exclude_columns=["ts"])
        assert len(report) == 0

    def test_handles_null_values(self):
        source = pd.DataFrame({"id": [1], "name": [None]})
        target = pd.DataFrame({"id": [1], "name": ["Alice"]})
        report = build_mismatch_report(source, target, key_columns=["id"])
        assert len(report) == 1

    def test_does_not_mutate_inputs(self):
        source = pd.DataFrame({"id": [1], "name": ["A"]})
        target = pd.DataFrame({"id": [1], "name": ["B"]})
        src_copy = source.copy()
        tgt_copy = target.copy()
        build_mismatch_report(source, target, key_columns=["id"])
        pd.testing.assert_frame_equal(source, src_copy)
        pd.testing.assert_frame_equal(target, tgt_copy)

    def test_rejects_non_dataframe(self):
        with pytest.raises(TypeError):
            build_mismatch_report({"a": 1}, pd.DataFrame(), key_columns=["a"])


class TestGetSchemasDiff:
    def test_identical_schemas(self):
        source = pd.DataFrame({"id": [1], "name": ["A"]})
        target = pd.DataFrame({"id": [1], "name": ["A"]})
        diff = get_schema_diff(source, target)
        assert diff.is_compatible
        assert diff.source_only_columns == []
        assert diff.target_only_columns == []

    def test_source_only_columns(self):
        source = pd.DataFrame({"id": [1], "name": ["A"], "extra": [1]})
        target = pd.DataFrame({"id": [1], "name": ["A"]})
        diff = get_schema_diff(source, target)
        assert "extra" in diff.source_only_columns
        assert not diff.is_compatible

    def test_target_only_columns(self):
        source = pd.DataFrame({"id": [1]})
        target = pd.DataFrame({"id": [1], "extra": [1]})
        diff = get_schema_diff(source, target)
        assert "extra" in diff.target_only_columns

    def test_type_mismatches(self):
        source = pd.DataFrame({"id": [1], "val": [1]})
        target = pd.DataFrame({"id": [1], "val": ["1"]})
        diff = get_schema_diff(source, target)
        assert "val" in diff.type_mismatches


class TestGetRowCounts:
    def test_returns_correct_counts(self):
        source = pd.DataFrame({"id": [1, 2, 3]})
        target = pd.DataFrame({"id": [1, 2]})
        counts = get_row_counts(source, target)
        assert counts["source_count"] == 3
        assert counts["target_count"] == 2
        assert counts["difference"] == 1

    def test_equal_counts(self):
        df = pd.DataFrame({"id": [1, 2]})
        counts = get_row_counts(df, df.copy())
        assert counts["difference"] == 0
        assert counts["is_count_match"] is True


class TestGetDuplicateReport:
    def test_no_duplicates(self):
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        report = get_duplicate_report(df, key_columns=["id"])
        assert len(report) == 0

    def test_finds_duplicates(self):
        df = pd.DataFrame({"id": [1, 1, 2], "name": ["A", "B", "C"]})
        report = get_duplicate_report(df, key_columns=["id"])
        assert len(report) == 2  # Both rows with id=1

    def test_composite_key_duplicates(self):
        df = pd.DataFrame({"id": [1, 1, 1], "date": ["01", "01", "02"], "val": ["A", "B", "C"]})
        report = get_duplicate_report(df, key_columns=["id", "date"])
        assert len(report) == 2  # Two rows with id=1, date=01


class TestGetNullAnalysis:
    def test_counts_nulls(self):
        df = pd.DataFrame({"name": ["Alice", None, None], "email": [None, "b@b.com", None]})
        analysis = get_null_analysis(df)
        assert analysis["name"]["null_count"] == 2
        assert analysis["email"]["null_count"] == 2

    def test_no_nulls(self):
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        analysis = get_null_analysis(df)
        assert analysis["id"]["null_count"] == 0
        assert analysis["name"]["null_count"] == 0

    def test_empty_dataframe(self):
        df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
        analysis = get_null_analysis(df)
        assert analysis["id"]["null_count"] == 0
        assert analysis["id"]["total_count"] == 0
