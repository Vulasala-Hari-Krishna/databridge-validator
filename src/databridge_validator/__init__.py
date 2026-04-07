"""databridge-validator: Data migration validation utilities.

Provides generic, reusable utilities for validating data migration from legacy
systems (mainframe COBOL, DB2, flat files) to modern cloud platforms
(AWS Aurora, S3, Redshift, etc.).
"""

from databridge_validator.cleaning.sanitizer import clean_control_characters, trim_whitespace
from databridge_validator.core.comparator import compare_dataframes
from databridge_validator.core.models import ColumnMismatch, SchemaDiff, ValidationResult
from databridge_validator.core.reporter import (
    build_mismatch_report,
    get_duplicate_report,
    get_null_analysis,
    get_row_counts,
    get_schema_diff,
)
from databridge_validator.pii.masking import (
    mask_alternate_chars,
    mask_partial,
    mask_pii_columns,
    mask_redact,
    mask_with_hash,
)
from databridge_validator.utils.dataframe_helpers import cast_all_to_string, normalize_columns

__all__ = [
    "ColumnMismatch",
    "SchemaDiff",
    # Models
    "ValidationResult",
    # Reporting
    "build_mismatch_report",
    # Utils
    "cast_all_to_string",
    # Cleaning
    "clean_control_characters",
    # Core comparison
    "compare_dataframes",
    "get_duplicate_report",
    "get_null_analysis",
    "get_row_counts",
    "get_schema_diff",
    # PII masking
    "mask_alternate_chars",
    "mask_partial",
    "mask_pii_columns",
    "mask_redact",
    "mask_with_hash",
    "normalize_columns",
    "trim_whitespace",
]
