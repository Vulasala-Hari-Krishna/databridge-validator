"""Report builders for validation results.

Provides utilities for building mismatch reports, schema diffs, row counts,
duplicate detection, and null analysis on pandas and PySpark DataFrames.
"""

import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from databridge_validator.core.models import SchemaDiff
from databridge_validator.utils.dataframe_helpers import (
    _get_spark_imports,
    _is_spark_dataframe,
    validate_dataframe_type,
    validate_key_columns,
)

logger = logging.getLogger(__name__)


VALID_REPORT_COLUMNS = ("target", "source", "both")


def build_mismatch_report(
    source_df: Union[pd.DataFrame, "SparkDataFrame"],
    target_df: Union[pd.DataFrame, "SparkDataFrame"],
    key_columns: List[str],
    exclude_columns: Optional[List[str]] = None,
    report_columns: str = "target",
) -> Union[pd.DataFrame, Any]:
    """Build a detailed mismatch report showing per-column differences.

    For each row where keys match but values differ, produces a row with the
    requested data columns plus a ``mismatch_columns`` string column listing
    differences in the format ``[{col : (src_val:tgt_val)}, ...]``.

    Args:
        source_df: Source DataFrame.
        target_df: Target DataFrame.
        key_columns: Column names used as join keys.
        exclude_columns: Columns to ignore in comparisons.
        report_columns: Which data columns to include alongside key columns
            and ``mismatch_columns``. One of ``"target"`` (default),
            ``"source"``, or ``"both"``.

    Returns:
        DataFrame with key columns, data columns, and a ``mismatch_columns``
        string column for mismatched rows only.

    Raises:
        TypeError: If inputs are not DataFrames.
        ValueError: If key columns are missing or report_columns is invalid.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator.core.reporter import build_mismatch_report
        >>> src = pd.DataFrame({"id": [1], "name": ["Alice"]})
        >>> tgt = pd.DataFrame({"id": [1], "name": ["Bob"]})
        >>> report = build_mismatch_report(src, tgt, key_columns=["id"])
    """
    validate_dataframe_type(source_df, "source_df")
    validate_dataframe_type(target_df, "target_df")

    if report_columns not in VALID_REPORT_COLUMNS:
        raise ValueError(f"report_columns must be one of {VALID_REPORT_COLUMNS}, got '{report_columns}'")

    if _is_spark_dataframe(source_df):
        return _build_mismatch_report_spark(source_df, target_df, key_columns, exclude_columns, report_columns)

    return _build_mismatch_report_pandas(source_df, target_df, key_columns, exclude_columns, report_columns)


def _build_mismatch_report_pandas(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    key_columns: List[str],
    exclude_columns: Optional[List[str]],
    report_columns: str,
) -> pd.DataFrame:
    """Pandas implementation of mismatch report builder."""
    validate_key_columns(source_df, key_columns)
    validate_key_columns(target_df, key_columns)

    compare_cols = [c for c in source_df.columns if c not in key_columns]
    if exclude_columns:
        compare_cols = [c for c in compare_cols if c not in exclude_columns]

    joined = source_df.merge(target_df, on=key_columns, suffixes=("_source", "_target"), how="inner")

    if len(joined) == 0:
        return pd.DataFrame()

    # Find rows where at least one comparison column differs
    mismatch_mask = pd.Series(False, index=joined.index)
    for col_name in compare_cols:
        src_col = f"{col_name}_source"
        tgt_col = f"{col_name}_target"
        if src_col in joined.columns and tgt_col in joined.columns:
            col_diff = ~joined[src_col].fillna("__NULL__").eq(joined[tgt_col].fillna("__NULL__"))
            mismatch_mask = mismatch_mask | col_diff

    mismatched = joined[mismatch_mask]

    if len(mismatched) == 0:
        return pd.DataFrame()

    # Build mismatch_columns string: [{col : (src_val:tgt_val)}, ...]
    def build_mismatch_string(row):
        parts = []
        for c in compare_cols:
            src_col = f"{c}_source"
            tgt_col = f"{c}_target"
            src_val = "null" if pd.isna(row[src_col]) else str(row[src_col])
            tgt_val = "null" if pd.isna(row[tgt_col]) else str(row[tgt_col])
            if src_val != tgt_val:
                parts.append(f"{{{c} : ({src_val}:{tgt_val})}}")
        return "[" + ", ".join(parts) + "]"

    mismatch_strings = mismatched.apply(build_mismatch_string, axis=1)

    # Assemble result based on report_columns
    result = pd.DataFrame()
    for k in key_columns:
        result[k] = mismatched[k].values

    if report_columns == "target":
        for c in compare_cols:
            tgt_col = f"{c}_target"
            if tgt_col in mismatched.columns:
                result[c] = mismatched[tgt_col].values
    elif report_columns == "source":
        for c in compare_cols:
            src_col = f"{c}_source"
            if src_col in mismatched.columns:
                result[c] = mismatched[src_col].values
    else:  # "both"
        for c in compare_cols:
            src_col = f"{c}_source"
            tgt_col = f"{c}_target"
            if src_col in mismatched.columns:
                result[f"{c}_source"] = mismatched[src_col].values
            if tgt_col in mismatched.columns:
                result[f"{c}_target"] = mismatched[tgt_col].values

    result["mismatch_columns"] = mismatch_strings.values
    return result.reset_index(drop=True)


def _build_mismatch_report_spark(
    source_df,
    target_df,
    key_columns: List[str],
    exclude_columns: Optional[List[str]],
    report_columns: str,
):
    """PySpark implementation of mismatch report builder."""
    _, F, StringType = _get_spark_imports()
    from pyspark.sql.functions import array, array_remove, col, concat, lit, when

    validate_key_columns(source_df, key_columns)
    validate_key_columns(target_df, key_columns)

    compare_cols = [c for c in source_df.columns if c not in key_columns]
    if exclude_columns:
        compare_cols = [c for c in compare_cols if c not in exclude_columns]

    src = source_df.alias("src")
    tgt = target_df.alias("tgt")

    joined = src.join(tgt, key_columns, "inner")

    # Build mismatch_columns string expressions
    mismatch_exprs = [
        when(
            ~col(f"src.{c}").eqNullSafe(col(f"tgt.{c}")),
            concat(
                lit("{"), lit(c), lit(" : ("),
                when(col(f"src.{c}").isNull(), lit("null")).otherwise(col(f"src.{c}").cast("string")),
                lit(":"),
                when(col(f"tgt.{c}").isNull(), lit("null")).otherwise(col(f"tgt.{c}").cast("string")),
                lit(")}"),
            ),
        ).otherwise(lit(""))
        for c in compare_cols
    ]

    mismatch_col_expr = array_remove(array(*mismatch_exprs), "").cast("string").alias("mismatch_columns")

    # Data columns based on report_columns
    if report_columns == "target":
        data_cols = [col(f"tgt.{c}").alias(c) for c in compare_cols]
    elif report_columns == "source":
        data_cols = [col(f"src.{c}").alias(c) for c in compare_cols]
    else:  # "both"
        data_cols = (
            [col(f"src.{c}").alias(f"{c}_source") for c in compare_cols]
            + [col(f"tgt.{c}").alias(f"{c}_target") for c in compare_cols]
        )

    select_expr = (
        [col(f"src.{c}") for c in key_columns]
        + data_cols
        + [mismatch_col_expr]
    )

    result = joined.select(select_expr)
    from pyspark.sql.functions import size

    return result.filter(size(col("mismatch_columns")) > 0).drop("_diff_cols") if "_diff_cols" in [f.name for f in result.schema.fields] else result.filter(F.length(col("mismatch_columns")) > 2)


def get_schema_diff(
    source_df: Union[pd.DataFrame, "SparkDataFrame"],
    target_df: Union[pd.DataFrame, "SparkDataFrame"],
) -> SchemaDiff:
    """Compare schemas between source and target DataFrames.

    Args:
        source_df: Source DataFrame.
        target_df: Target DataFrame.

    Returns:
        SchemaDiff with source-only columns, target-only columns,
        common columns, and type mismatches.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator.core.reporter import get_schema_diff
        >>> src = pd.DataFrame({"id": [1], "name": ["A"]})
        >>> tgt = pd.DataFrame({"id": [1], "extra": [1]})
        >>> diff = get_schema_diff(src, tgt)
        >>> diff.source_only_columns
        ['name']
    """
    validate_dataframe_type(source_df, "source_df")
    validate_dataframe_type(target_df, "target_df")

    source_cols = set(source_df.columns)
    target_cols = set(target_df.columns)

    common = sorted(source_cols & target_cols)
    source_only = sorted(source_cols - target_cols)
    target_only = sorted(target_cols - source_cols)

    type_mismatches: Dict[str, tuple] = {}
    for col_name in common:
        if _is_spark_dataframe(source_df):
            src_type = str(source_df.schema[col_name].dataType)
            tgt_type = str(target_df.schema[col_name].dataType)
        else:
            src_type = str(source_df[col_name].dtype)
            tgt_type = str(target_df[col_name].dtype)
        if src_type != tgt_type:
            type_mismatches[col_name] = (src_type, tgt_type)

    return SchemaDiff(
        source_only_columns=source_only,
        target_only_columns=target_only,
        common_columns=common,
        type_mismatches=type_mismatches,
    )


def get_row_counts(
    source_df: Union[pd.DataFrame, "SparkDataFrame"],
    target_df: Union[pd.DataFrame, "SparkDataFrame"],
) -> Dict[str, Any]:
    """Get row count comparison between source and target.

    Args:
        source_df: Source DataFrame.
        target_df: Target DataFrame.

    Returns:
        Dictionary with source_count, target_count, difference, and is_count_match.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator.core.reporter import get_row_counts
        >>> src = pd.DataFrame({"id": [1, 2, 3]})
        >>> tgt = pd.DataFrame({"id": [1, 2]})
        >>> get_row_counts(src, tgt)
        {'source_count': 3, 'target_count': 2, 'difference': 1, 'is_count_match': False}
    """
    validate_dataframe_type(source_df, "source_df")
    validate_dataframe_type(target_df, "target_df")

    if _is_spark_dataframe(source_df):
        src_count = source_df.count()
        tgt_count = target_df.count()
    else:
        src_count = len(source_df)
        tgt_count = len(target_df)

    return {
        "source_count": src_count,
        "target_count": tgt_count,
        "difference": abs(src_count - tgt_count),
        "is_count_match": src_count == tgt_count,
    }


def get_duplicate_report(
    df: Union[pd.DataFrame, "SparkDataFrame"],
    key_columns: List[str],
) -> Union[pd.DataFrame, Any]:
    """Find duplicate rows based on key columns.

    Args:
        df: DataFrame to check for duplicates.
        key_columns: Columns that should be unique.

    Returns:
        DataFrame containing only rows with duplicate keys.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator.core.reporter import get_duplicate_report
        >>> df = pd.DataFrame({"id": [1, 1, 2], "name": ["A", "B", "C"]})
        >>> get_duplicate_report(df, key_columns=["id"])
           id name
        0   1    A
        1   1    B
    """
    validate_dataframe_type(df, "df")
    validate_key_columns(df, key_columns)

    if _is_spark_dataframe(df):
        _, F, _ = _get_spark_imports()
        from pyspark.sql import Window
        from pyspark.sql.functions import col, count

        w = Window.partitionBy(*key_columns)
        with_count = df.withColumn("_dup_count", count("*").over(w))
        return with_count.filter(col("_dup_count") > 1).drop("_dup_count")

    duplicated_mask = df.duplicated(subset=key_columns, keep=False)
    return df[duplicated_mask].reset_index(drop=True)


def get_null_analysis(
    df: Union[pd.DataFrame, "SparkDataFrame"],
) -> Dict[str, Dict[str, int]]:
    """Analyze null/empty counts per column.

    Args:
        df: DataFrame to analyze.

    Returns:
        Dictionary mapping column names to null count and total count.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator.core.reporter import get_null_analysis
        >>> df = pd.DataFrame({"name": ["Alice", None], "email": [None, None]})
        >>> get_null_analysis(df)
        {'name': {'null_count': 1, 'total_count': 2}, 'email': {'null_count': 2, 'total_count': 2}}
    """
    validate_dataframe_type(df, "df")

    result: Dict[str, Dict[str, int]] = {}

    if _is_spark_dataframe(df):
        total = df.count()
        for col_name in df.columns:
            from pyspark.sql.functions import col, isnan, isnull, sum as spark_sum

            null_count = df.filter(isnull(col(col_name))).count()
            result[col_name] = {"null_count": null_count, "total_count": total}
    else:
        total = len(df)
        for col_name in df.columns:
            null_count = int(df[col_name].isna().sum())
            result[col_name] = {"null_count": null_count, "total_count": total}

    return result
