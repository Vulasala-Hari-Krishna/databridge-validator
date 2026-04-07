"""Integration tests for full validation workflow with pandas DataFrames."""

import pandas as pd

from databridge_validator import (
    build_mismatch_report,
    cast_all_to_string,
    clean_control_characters,
    compare_dataframes,
    get_duplicate_report,
    get_null_analysis,
    get_row_counts,
    get_schema_diff,
    normalize_columns,
    trim_whitespace,
)


class TestFullValidationPandas:
    def test_end_to_end_validation_workflow(self):
        """Full workflow: clean, normalize, compare, report."""
        # Raw source data with whitespace and control chars
        source_raw = pd.DataFrame(
            {
                "ID": [1, 2, 3, 4],
                "Name": ["  Alice\n", "Bob\t", "  Charlie  ", "Diana"],
                "Email": ["alice@test.com", "bob@test.com", "charlie@test.com", "diana@test.com"],
                "Amount": [100.0, 200.0, 300.0, 400.0],
            }
        )

        target_raw = pd.DataFrame(
            {
                "id": [1, 2, 3, 5],
                "name": ["Alice", "Bobby", "Charlie", "Eve"],
                "email": ["alice@test.com", "bob_new@test.com", "charlie@test.com", "eve@test.com"],
                "amount": [100.0, 250.0, 300.0, 500.0],
            }
        )

        # Step 1: Clean source data
        source = clean_control_characters(source_raw)
        source = trim_whitespace(source)
        source = normalize_columns(source)

        # Step 2: Compare
        result = compare_dataframes(source, target_raw, key_columns=["id"])

        # Verify results
        assert result.total_source_rows == 4
        assert result.total_target_rows == 4
        assert result.is_match is False
        assert result.mismatch_count == 1  # id=2 mismatches
        assert result.source_extra_count == 1  # id=4
        assert result.target_extra_count == 1  # id=5
        assert result.matched_count == 2  # ids 1, 3

    def test_schema_and_row_count_checks(self):
        source = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        target = pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "extra": [1, 2]})

        # Schema check
        schema_diff = get_schema_diff(source, target)
        assert not schema_diff.is_compatible
        assert "extra" in schema_diff.target_only_columns

        # Row count check
        counts = get_row_counts(source, target)
        assert counts["source_count"] == 3
        assert counts["target_count"] == 2
        assert not counts["is_count_match"]

    def test_duplicate_and_null_checks(self):
        df = pd.DataFrame(
            {
                "id": [1, 1, 2, 3],
                "name": ["Alice", "Alice2", None, "Charlie"],
            }
        )

        # Duplicate check
        dups = get_duplicate_report(df, key_columns=["id"])
        assert len(dups) == 2

        # Null analysis
        nulls = get_null_analysis(df)
        assert nulls["name"]["null_count"] == 1

    def test_pii_masking_in_full_workflow(self):
        source = pd.DataFrame(
            {
                "id": [1, 2],
                "ssn": ["123-45-6789", "987-65-4321"],
                "name": ["Alice", "Bob"],
            }
        )
        target = pd.DataFrame(
            {
                "id": [1, 2],
                "ssn": ["111-22-3333", "444-55-6666"],
                "name": ["Alice", "Bob"],
            }
        )

        result = compare_dataframes(
            source,
            target,
            key_columns=["id"],
            pii_columns=["ssn"],
            mask_strategy="hash",
        )
        assert result.mismatch_count == 2
        # SSN data column should be hashed (default report_columns="target")
        mismatch = result.mismatch_records
        assert mismatch is not None
        assert len(mismatch["ssn"].iloc[0]) == 64
        # mismatch_columns string should also have hashed SSN values
        val = mismatch["mismatch_columns"].iloc[0]
        assert "123-45-6789" not in val
        assert "111-22-3333" not in val

    def test_build_mismatch_report_standalone(self):
        source = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "val": [10, 20, 30],
            }
        )
        target = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bobby", "Chuck"],
                "val": [10, 20, 99],
            }
        )
        report = build_mismatch_report(source, target, key_columns=["id"])
        assert len(report) == 2  # ids 2 and 3 have mismatches

    def test_cast_all_to_string_and_compare(self):
        source = pd.DataFrame({"id": [1, 2], "amount": [100, 200]})
        target = pd.DataFrame({"id": [1, 2], "amount": [100, 250]})

        source_str = cast_all_to_string(source)
        target_str = cast_all_to_string(target)

        result = compare_dataframes(source_str, target_str, key_columns=["id"])
        assert result.mismatch_count == 1

    def test_validation_result_to_dict(self):
        source = pd.DataFrame({"id": [1], "name": ["A"]})
        target = pd.DataFrame({"id": [1], "name": ["B"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        d = result.to_dict()
        assert isinstance(d, dict)
        assert d["mismatch_count"] == 1

    def test_validation_result_str(self):
        source = pd.DataFrame({"id": [1], "name": ["A"]})
        target = pd.DataFrame({"id": [1], "name": ["A"]})
        result = compare_dataframes(source, target, key_columns=["id"])
        s = str(result)
        assert "is_match=True" in s
