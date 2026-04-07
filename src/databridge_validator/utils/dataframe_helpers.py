"""Shared DataFrame helper utilities.

Provides runtime type detection, column normalization, and casting
utilities that work with both pandas and PySpark DataFrames.
"""

import logging
import re
from typing import List, Union

import pandas as pd

logger = logging.getLogger(__name__)


def _is_spark_dataframe(df) -> bool:
    """Check if df is a PySpark DataFrame without requiring PySpark to be installed.

    Args:
        df: Object to check.

    Returns:
        True if df is a PySpark DataFrame.
    """
    if df is None:
        return False
    return type(df).__module__.startswith("pyspark")


def _get_spark_imports():
    """Lazily import PySpark modules.

    Returns:
        Tuple of (SparkDataFrame, pyspark.sql.functions, StringType).

    Raises:
        ImportError: If PySpark is not installed.
    """
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        return SparkDataFrame, F, StringType
    except ImportError as err:
        raise ImportError(
            "PySpark is required for Spark DataFrame support. Install it with: pip install databridge-validator[spark]"
        ) from err


def validate_dataframe_type(df, name: str = "df") -> None:
    """Validate that df is a pandas or PySpark DataFrame.

    Args:
        df: Object to validate.
        name: Parameter name for the error message.

    Raises:
        TypeError: If df is not a pandas or PySpark DataFrame.
    """
    if not isinstance(df, pd.DataFrame) and not _is_spark_dataframe(df):
        raise TypeError(f"{name} must be a pandas or PySpark DataFrame, got {type(df).__name__}")


def validate_key_columns(df: Union[pd.DataFrame, "SparkDataFrame"], key_columns: List[str]) -> None:
    """Validate that all key columns exist in the DataFrame.

    Args:
        df: DataFrame to check.
        key_columns: Column names that must be present.

    Raises:
        ValueError: If any key columns are missing.
    """
    missing = set(key_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Key columns not found in DataFrame: {missing}")


def normalize_columns(df: Union[pd.DataFrame, "SparkDataFrame"]) -> Union[pd.DataFrame, "SparkDataFrame"]:
    """Normalize column names: lowercase, strip whitespace, replace spaces with underscores.

    Args:
        df: Input DataFrame (pandas or PySpark).

    Returns:
        New DataFrame with normalized column names. Original is not mutated.
    """
    if _is_spark_dataframe(df):
        new_names = [re.sub(r"\s+", "_", col.strip().lower()) for col in df.columns]
        return df.toDF(*new_names)

    new_columns = [re.sub(r"\s+", "_", col.strip().lower()) for col in df.columns]
    result = df.copy()
    result.columns = new_columns
    return result


def cast_all_to_string(df: Union[pd.DataFrame, "SparkDataFrame"]) -> Union[pd.DataFrame, "SparkDataFrame"]:
    """Cast all columns to string type.

    Null/NaN values are preserved as null/NaN, not converted to the string "None" or "nan".

    Args:
        df: Input DataFrame (pandas or PySpark).

    Returns:
        New DataFrame with all columns cast to string. Original is not mutated.
    """
    if _is_spark_dataframe(df):
        _, _F, _StringType = _get_spark_imports()
        from pyspark.sql.functions import col, when

        return df.select([when(col(c).isNotNull(), col(c).cast("string")).otherwise(None).alias(c) for c in df.columns])

    result = df.copy()
    for col_name in result.columns:
        if pd.api.types.is_string_dtype(result[col_name]):
            # Already string-like; just ensure nulls stay as None
            result[col_name] = result[col_name].where(result[col_name].notna(), other=None)
        else:
            result[col_name] = result[col_name].astype(object).where(result[col_name].notna(), other=None)
            mask = result[col_name].notna()
            result.loc[mask, col_name] = result.loc[mask, col_name].astype(str)
    return result
