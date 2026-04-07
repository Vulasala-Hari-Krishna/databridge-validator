---
applyTo: "src/**/*.py"
---

# Source Code Instructions — databridge-validator

## DataFrame Dual-Support Pattern (MUST follow)

Every function that operates on DataFrames MUST support both pandas and PySpark.
Use this pattern consistently:

```python
import pandas as pd
from typing import Union, Optional, List

# Lazy import pattern for optional PySpark dependency
def _is_spark_dataframe(df) -> bool:
    """Check if df is a PySpark DataFrame without requiring PySpark to be installed."""
    return type(df).__module__.startswith("pyspark")

def _get_spark_imports():
    """Lazily import PySpark modules. Raises ImportError with helpful message."""
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        return SparkDataFrame, F, StringType
    except ImportError:
        raise ImportError(
            "PySpark is required for Spark DataFrame support. "
            "Install it with: pip install databridge-validator[spark]"
        )
```

## Function Signature Rules

- All data-accepting parameters should be typed as `Union[pd.DataFrame, "SparkDataFrame"]`
- Use string literal `"SparkDataFrame"` in type hints to avoid import errors when PySpark is not installed
- All optional parameters MUST have sensible defaults
- Key columns / join columns parameter should be `List[str]`
- Boolean flags default to `False` unless the "on" behavior is clearly the common case

## Parameter Design — Make Everything User-Configurable

These aspects from the legacy code were hardcoded and MUST become optional parameters:

| Aspect | Parameter | Default | Notes |
|---|---|---|---|
| Spark partitions | `num_partitions: Optional[int] = None` | `None` (no repartition) | Only applies to Spark |
| Persist strategy | `persist: bool = False` | `False` | User opts in; don't force memory usage |
| Storage level | `storage_level: Optional[str] = "MEMORY_AND_DISK"` | Standard level | Only when persist=True |
| PII masking | `pii_columns: Optional[List[str]] = None` | `None` (no masking) | Replaces `pii_exists` boolean |
| Mask strategy | `mask_strategy: str = "alternate"` | Alternate char mask | Options: "alternate", "hash", "redact", "partial" |
| Columns to exclude | `exclude_columns: Optional[List[str]] = None` | `None` | Replaces `acceptable_mismatch_columns` |
| Case sensitivity | `case_sensitive: bool = False` | `False` | Column name comparison |

## Input Validation Pattern

Every public function MUST validate inputs at the top:

```python
def compare_dataframes(
    source_df: Union[pd.DataFrame, "SparkDataFrame"],
    target_df: Union[pd.DataFrame, "SparkDataFrame"],
    key_columns: List[str],
    ...
) -> "ValidationResult":
    # 1. Type validation
    if not isinstance(source_df, pd.DataFrame) and not _is_spark_dataframe(source_df):
        raise TypeError(
            f"source_df must be a pandas or PySpark DataFrame, got {type(source_df).__name__}"
        )
    # 2. Consistency validation
    if type(source_df).__module__ != type(target_df).__module__:
        raise TypeError("source_df and target_df must be the same DataFrame type")
    # 3. Column validation
    missing = set(key_columns) - set(source_df.columns)
    if missing:
        raise ValueError(f"Key columns not found in source_df: {missing}")
    # 4. Empty check
    if isinstance(source_df, pd.DataFrame) and source_df.empty:
        logger.warning("source_df is empty — returning empty result")
```

## Logging (NOT print)

```python
import logging

logger = logging.getLogger(__name__)

# Use throughout:
logger.info("Comparing %d source rows with %d target rows", src_count, tgt_count)
logger.warning("Column '%s' exists in source but not in target — skipping", col)
logger.debug("Hash join completed in %.2fs", elapsed)
```

## Docstring Format (Google-style)

```python
def compare_dataframes(
    source_df: Union[pd.DataFrame, "SparkDataFrame"],
    target_df: Union[pd.DataFrame, "SparkDataFrame"],
    key_columns: List[str],
    exclude_columns: Optional[List[str]] = None,
    pii_columns: Optional[List[str]] = None,
    mask_strategy: str = "alternate",
    num_partitions: Optional[int] = None,
    persist: bool = False,
    case_sensitive: bool = False,
) -> "ValidationResult":
    """Compare source and target DataFrames and generate a validation report.

    Performs a full delta comparison between two DataFrames using hash-based
    matching, then identifies mismatched records, source-only extras, and
    target-only extras.

    Args:
        source_df: The source/legacy DataFrame to validate against.
        target_df: The target/cloud DataFrame to validate.
        key_columns: Column names to use as join keys for matching rows.
        exclude_columns: Columns to ignore during value comparison.
            Defaults to None (compare all non-key columns).
        pii_columns: Columns containing PII to mask in the mismatch report.
            Defaults to None (no masking).
        mask_strategy: PII masking strategy. One of "alternate", "hash",
            "redact", "partial". Defaults to "alternate".
        num_partitions: Number of Spark partitions for repartitioning.
            Only applies to PySpark DataFrames. Defaults to None (no repartition).
        persist: Whether to persist intermediate Spark DataFrames.
            Only applies to PySpark. Defaults to False.
        case_sensitive: Whether column name matching is case-sensitive.
            Defaults to False.

    Returns:
        ValidationResult containing mismatch_records, source_extra_records,
        target_extra_records, and a summary dict.

    Raises:
        TypeError: If DataFrames are not pandas or PySpark, or if types don't match.
        ValueError: If key_columns are not found in both DataFrames.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator import compare_dataframes
        >>> source = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        >>> target = pd.DataFrame({"id": [1, 2, 4], "name": ["A", "X", "D"]})
        >>> result = compare_dataframes(source, target, key_columns=["id"])
        >>> print(result.summary)
        {'total_source': 3, 'total_target': 3, 'matched': 1, 'mismatched': 1, ...}
    """
```

## Error Handling

- Never use bare `except Exception`
- Catch specific exceptions and re-raise with context
- Use `logger.exception()` for unexpected errors before re-raising
- Never swallow errors silently