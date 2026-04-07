"""DataFrame cleaning and sanitization utilities.

Provides functions to trim whitespace and remove control characters
from string columns in pandas and PySpark DataFrames.
"""

import logging
import re
from typing import Optional, Union

import pandas as pd

from databridge_validator.utils.dataframe_helpers import (
    _get_spark_imports,
    _is_spark_dataframe,
    validate_dataframe_type,
)

logger = logging.getLogger(__name__)

# Default regex pattern for control characters to remove
DEFAULT_CONTROL_CHAR_PATTERN = r"[\r\n\u200b\t\u00a0\x00\x07\x08\x0c\x1b\x0b]"


def trim_whitespace(df: Union[pd.DataFrame, "SparkDataFrame"]) -> Union[pd.DataFrame, "SparkDataFrame"]:
    """Trim leading and trailing whitespace from all string columns.

    Args:
        df: Input DataFrame (pandas or PySpark).

    Returns:
        New DataFrame with whitespace trimmed from string columns.
        Original is not mutated.

    Raises:
        TypeError: If df is not a pandas or PySpark DataFrame.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator.cleaning.sanitizer import trim_whitespace
        >>> df = pd.DataFrame({"name": ["  Alice  ", "Bob  "]})
        >>> trim_whitespace(df)["name"].tolist()
        ['Alice', 'Bob']
    """
    validate_dataframe_type(df, "df")

    if _is_spark_dataframe(df):
        _, _F, _ = _get_spark_imports()
        from pyspark.sql.functions import col, trim

        return df.select(
            [trim(col(c)).alias(c) if df.schema[c].dataType.simpleString() == "string" else col(c) for c in df.columns]
        )

    result = df.copy()
    for col_name in result.columns:
        if pd.api.types.is_string_dtype(result[col_name]):
            result[col_name] = result[col_name].str.strip()
    return result


def clean_control_characters(
    df: Union[pd.DataFrame, "SparkDataFrame"],
    pattern: Optional[str] = None,
) -> Union[pd.DataFrame, "SparkDataFrame"]:
    """Remove control characters from all string columns.

    Args:
        df: Input DataFrame (pandas or PySpark).
        pattern: Regex pattern of characters to remove.
            Defaults to common control characters (\\r, \\n, \\t, \\x00, etc.).

    Returns:
        New DataFrame with control characters removed from string columns.
        Original is not mutated.

    Raises:
        TypeError: If df is not a pandas or PySpark DataFrame.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator.cleaning.sanitizer import clean_control_characters
        >>> df = pd.DataFrame({"name": ["Alice\\n", "Bob\\t"]})
        >>> clean_control_characters(df)["name"].tolist()
        ['Alice', 'Bob']
    """
    validate_dataframe_type(df, "df")

    if pattern is None:
        pattern = DEFAULT_CONTROL_CHAR_PATTERN

    if _is_spark_dataframe(df):
        _, _F, _ = _get_spark_imports()
        from pyspark.sql.functions import col, regexp_replace

        return df.select(
            [
                regexp_replace(col(c), pattern, "").alias(c)
                if df.schema[c].dataType.simpleString() == "string"
                else col(c)
                for c in df.columns
            ]
        )

    result = df.copy()
    compiled = re.compile(pattern)
    for col_name in result.columns:
        if pd.api.types.is_string_dtype(result[col_name]):
            result[col_name] = result[col_name].where(
                result[col_name].isna(),
                result[col_name].str.replace(compiled, "", regex=True),
            )
    return result
