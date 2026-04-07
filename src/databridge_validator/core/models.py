"""Result dataclasses for validation operations."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

import pandas as pd


@dataclass
class ValidationResult:
    """Structured result from a DataFrame comparison.

    Attributes:
        is_match: True if source and target are identical (no mismatches, no extras).
        total_source_rows: Number of rows in the source DataFrame.
        total_target_rows: Number of rows in the target DataFrame.
        matched_count: Number of rows that matched between source and target.
        mismatch_count: Number of rows with value differences.
        source_extra_count: Number of rows in source but not in target.
        target_extra_count: Number of rows in target but not in source.
        mismatch_records: DataFrame of records with value differences.
        source_extra_records: DataFrame of records only in source.
        target_extra_records: DataFrame of records only in target.
        summary: Dictionary summary of the validation.
        metadata: Additional metadata about the comparison run.
    """

    is_match: bool
    total_source_rows: int
    total_target_rows: int
    matched_count: int
    mismatch_count: int
    source_extra_count: int
    target_extra_count: int
    mismatch_records: Optional[Union[pd.DataFrame, Any]] = None
    source_extra_records: Optional[Union[pd.DataFrame, Any]] = None
    target_extra_records: Optional[Union[pd.DataFrame, Any]] = None
    summary: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary fields to a dictionary (excludes DataFrames).

        Returns:
            Dictionary with scalar summary fields.
        """
        return {
            "is_match": self.is_match,
            "total_source_rows": self.total_source_rows,
            "total_target_rows": self.total_target_rows,
            "matched_count": self.matched_count,
            "mismatch_count": self.mismatch_count,
            "source_extra_count": self.source_extra_count,
            "target_extra_count": self.target_extra_count,
            "summary": self.summary,
            "metadata": self.metadata,
        }

    def __str__(self) -> str:
        """Human-readable summary string."""
        return (
            f"ValidationResult("
            f"is_match={self.is_match}, "
            f"source_rows={self.total_source_rows}, "
            f"target_rows={self.total_target_rows}, "
            f"matched={self.matched_count}, "
            f"mismatched={self.mismatch_count}, "
            f"source_extras={self.source_extra_count}, "
            f"target_extras={self.target_extra_count})"
        )


@dataclass
class ColumnMismatch:
    """Represents a column-level mismatch between source and target values.

    Attributes:
        column_name: Name of the mismatched column.
        source_value: Value from the source DataFrame.
        target_value: Value from the target DataFrame.
    """

    column_name: str
    source_value: Any
    target_value: Any


@dataclass
class SchemaDiff:
    """Result from a schema comparison between two DataFrames.

    Attributes:
        source_only_columns: Columns present only in source.
        target_only_columns: Columns present only in target.
        common_columns: Columns present in both.
        type_mismatches: Dict of column name to (source_type, target_type) for type differences.
    """

    source_only_columns: List[str] = field(default_factory=list)
    target_only_columns: List[str] = field(default_factory=list)
    common_columns: List[str] = field(default_factory=list)
    type_mismatches: Dict[str, tuple] = field(default_factory=dict)

    @property
    def is_compatible(self) -> bool:
        """True if schemas have the same columns (types may differ)."""
        return not self.source_only_columns and not self.target_only_columns
