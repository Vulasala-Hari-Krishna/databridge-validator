"""PII masking strategies for DataFrame columns.

Provides multiple masking strategies: alternate-char, hash, redact, and partial.
All functions support both pandas and PySpark DataFrames.
"""

import hashlib
import logging
from typing import List, Optional, Union

import pandas as pd

from databridge_validator.utils.dataframe_helpers import (
    _get_spark_imports,
    _is_spark_dataframe,
    validate_dataframe_type,
)

logger = logging.getLogger(__name__)

VALID_STRATEGIES = ("alternate", "hash", "redact", "partial")


def mask_alternate_chars(value: Optional[str], mask_char: str = "*") -> Optional[str]:
    """Mask every other character in a string.

    Args:
        value: Input string to mask.
        mask_char: Character to use for masking. Defaults to "*".

    Returns:
        Masked string with alternating characters replaced, or None if input is None.

    Example:
        >>> mask_alternate_chars("Hello")
        'H*l*o'
    """
    if value is None:
        return None
    return "".join(char if i % 2 == 0 else mask_char for i, char in enumerate(value))


def mask_with_hash(value: Optional[str]) -> Optional[str]:
    """Mask a string by replacing it with its SHA-256 hash.

    Args:
        value: Input string to hash.

    Returns:
        SHA-256 hex digest of the value, or None if input is None.

    Example:
        >>> len(mask_with_hash("secret"))
        64
    """
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def mask_partial(value: Optional[str], visible_chars: int = 1) -> Optional[str]:
    """Mask a string showing only the first and last N characters.

    Args:
        value: Input string to mask.
        visible_chars: Number of characters to show at start and end. Defaults to 1.

    Returns:
        Partially masked string, or None if input is None.

    Example:
        >>> mask_partial("Hello")
        'H***o'
    """
    if value is None:
        return None
    if len(value) <= visible_chars * 2:
        return value
    return value[:visible_chars] + "*" * (len(value) - visible_chars * 2) + value[-visible_chars:]


def mask_redact(value: Optional[str], replacement: str = "***") -> Optional[str]:
    """Replace an entire string with a redaction placeholder.

    Args:
        value: Input string to redact.
        replacement: Replacement text. Defaults to "***".

    Returns:
        Redaction placeholder, or None if input is None.

    Example:
        >>> mask_redact("secret")
        '***'
    """
    if value is None:
        return None
    return replacement


def _get_mask_function(strategy: str):
    """Return the masking function for the given strategy name.

    Args:
        strategy: One of "alternate", "hash", "redact", "partial".

    Returns:
        The corresponding masking function.

    Raises:
        ValueError: If strategy is not recognized.
    """
    strategies = {
        "alternate": mask_alternate_chars,
        "hash": mask_with_hash,
        "redact": mask_redact,
        "partial": mask_partial,
    }
    if strategy not in strategies:
        raise ValueError(f"Unknown mask_strategy '{strategy}'. Must be one of: {VALID_STRATEGIES}")
    return strategies[strategy]


def mask_pii_columns(
    df: Union[pd.DataFrame, "SparkDataFrame"],
    pii_columns: List[str],
    mask_strategy: str = "alternate",
) -> Union[pd.DataFrame, "SparkDataFrame"]:
    """Mask PII columns in a DataFrame using the specified strategy.

    Args:
        df: Input DataFrame (pandas or PySpark).
        pii_columns: List of column names containing PII to mask.
        mask_strategy: Masking strategy to use. One of "alternate", "hash",
            "redact", "partial". Defaults to "alternate".

    Returns:
        New DataFrame with PII columns masked. Original is not mutated.

    Raises:
        TypeError: If df is not a pandas or PySpark DataFrame.
        ValueError: If mask_strategy is not recognized.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator.pii.masking import mask_pii_columns
        >>> df = pd.DataFrame({"name": ["Alice"], "ssn": ["123-45-6789"]})
        >>> result = mask_pii_columns(df, pii_columns=["ssn"])
        >>> result["ssn"].iloc[0]
        '1*3*4*-*7*9'
    """
    validate_dataframe_type(df, "df")
    mask_fn = _get_mask_function(mask_strategy)

    if _is_spark_dataframe(df):
        return _mask_pii_columns_spark(df, pii_columns, mask_fn)

    return _mask_pii_columns_pandas(df, pii_columns, mask_fn)


def _mask_pii_columns_pandas(
    df: pd.DataFrame,
    pii_columns: List[str],
    mask_fn,
) -> pd.DataFrame:
    """Apply PII masking to pandas DataFrame columns."""
    result = df.copy()
    for col_name in pii_columns:
        if col_name not in result.columns:
            logger.warning("PII column '%s' not found in DataFrame — skipping", col_name)
            continue
        result[col_name] = result[col_name].map(lambda x: mask_fn(x) if pd.notna(x) else x)
    return result


def _mask_pii_columns_spark(df, pii_columns: List[str], mask_fn):
    """Apply PII masking to PySpark DataFrame columns."""
    _, F, StringType = _get_spark_imports()
    from pyspark.sql.functions import udf

    mask_udf = udf(lambda x: mask_fn(x) if x is not None else None, StringType())

    result = df
    for col_name in pii_columns:
        if col_name not in result.columns:
            logger.warning("PII column '%s' not found in DataFrame — skipping", col_name)
            continue
        result = result.withColumn(col_name, mask_udf(F.col(col_name)))
    return result
