"""Core comparison engine for DataFrame validation.

Compares source and target DataFrames using hash-based matching to identify
mismatched records, source-only extras, and target-only extras.
"""

import hashlib
import logging
from typing import List, Optional, Union

import pandas as pd

from databridge_validator.core.models import ValidationResult
from databridge_validator.pii.masking import mask_pii_columns
from databridge_validator.utils.dataframe_helpers import (
    _get_spark_imports,
    _is_spark_dataframe,
    normalize_columns,
    validate_dataframe_type,
    validate_key_columns,
)

logger = logging.getLogger(__name__)

ROW_HASH_COL = "_row_sha2"
VALID_REPORT_COLUMNS = ("target", "source", "both")


def compare_dataframes(
    source_df: Union[pd.DataFrame, "SparkDataFrame"],
    target_df: Union[pd.DataFrame, "SparkDataFrame"],
    key_columns: List[str],
    exclude_columns: Optional[List[str]] = None,
    pii_columns: Optional[List[str]] = None,
    mask_strategy: str = "alternate",
    report_columns: str = "target",
    num_partitions: Optional[int] = None,
    persist: bool = False,
    case_sensitive: bool = False,
) -> ValidationResult:
    """Compare source and target DataFrames and generate a validation report.

    Performs a full delta comparison between two DataFrames using hash-based
    matching, then identifies mismatched records, source-only extras, and
    target-only extras.

    The mismatch report includes a ``mismatch_columns`` string column that lists
    per-column differences in the format ``[{col : (src_val:tgt_val)}, ...]``.

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
        report_columns: Which data columns to include in the mismatch report
            alongside key columns and ``mismatch_columns``. One of:

            - ``"target"`` (default): key columns + target DataFrame columns.
            - ``"source"``: key columns + source DataFrame columns.
            - ``"both"``: key columns + source columns (suffixed ``_source``)
              + target columns (suffixed ``_target``).
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
        ValueError: If key_columns are not found in both DataFrames, or if
            report_columns is not a valid option.

    Example:
        >>> import pandas as pd
        >>> from databridge_validator import compare_dataframes
        >>> source = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        >>> target = pd.DataFrame({"id": [1, 2, 4], "name": ["A", "X", "D"]})
        >>> result = compare_dataframes(source, target, key_columns=["id"])
        >>> print(result.mismatch_records[["id", "name", "mismatch_columns"]].to_string())
    """
    validate_dataframe_type(source_df, "source_df")
    validate_dataframe_type(target_df, "target_df")

    if report_columns not in VALID_REPORT_COLUMNS:
        raise ValueError(f"report_columns must be one of {VALID_REPORT_COLUMNS}, got '{report_columns}'")

    if _is_spark_dataframe(source_df) != _is_spark_dataframe(target_df):
        raise TypeError("source_df and target_df must be the same DataFrame type")

    if _is_spark_dataframe(source_df):
        return _compare_spark(
            source_df, target_df, key_columns, exclude_columns,
            pii_columns, mask_strategy, report_columns,
            num_partitions, persist, case_sensitive,
        )

    return _compare_pandas(
        source_df, target_df, key_columns, exclude_columns,
        pii_columns, mask_strategy, report_columns, case_sensitive,
    )


def _compare_pandas(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    key_columns: List[str],
    exclude_columns: Optional[List[str]],
    pii_columns: Optional[List[str]],
    mask_strategy: str,
    report_columns: str,
    case_sensitive: bool,
) -> ValidationResult:
    """Pandas implementation of DataFrame comparison."""
    src = source_df.copy()
    tgt = target_df.copy()

    if not case_sensitive:
        src = normalize_columns(src)
        tgt = normalize_columns(tgt)
        key_columns = [k.lower() for k in key_columns]
        if exclude_columns:
            exclude_columns = [c.lower() for c in exclude_columns]
        if pii_columns:
            pii_columns = [c.lower() for c in pii_columns]

    validate_key_columns(src, key_columns)
    validate_key_columns(tgt, key_columns)

    total_source = len(src)
    total_target = len(tgt)

    if total_source == 0 and total_target == 0:
        return ValidationResult(
            is_match=True,
            total_source_rows=0,
            total_target_rows=0,
            matched_count=0,
            mismatch_count=0,
            source_extra_count=0,
            target_extra_count=0,
            summary=_build_summary(0, 0, 0, 0, 0, 0),
        )

    # Determine comparison columns
    compare_cols = [c for c in src.columns if c not in key_columns]
    if exclude_columns:
        compare_cols = [c for c in compare_cols if c not in exclude_columns]

    # Compute row hashes for comparison columns
    src_hash = _compute_pandas_row_hash(src, compare_cols)
    tgt_hash = _compute_pandas_row_hash(tgt, compare_cols)

    src_with_hash = src.copy()
    src_with_hash[ROW_HASH_COL] = src_hash
    tgt_with_hash = tgt.copy()
    tgt_with_hash[ROW_HASH_COL] = tgt_hash

    # Find records only in source (left anti join on keys)
    merged_keys = src[key_columns].merge(tgt[key_columns], on=key_columns, how="outer", indicator=True)
    source_only_keys = merged_keys[merged_keys["_merge"] == "left_only"][key_columns]
    target_only_keys = merged_keys[merged_keys["_merge"] == "right_only"][key_columns]

    source_extra_records = src.merge(source_only_keys, on=key_columns, how="inner")
    target_extra_records = tgt.merge(target_only_keys, on=key_columns, how="inner")

    # Find common keys
    common_keys = merged_keys[merged_keys["_merge"] == "both"][key_columns]

    # Join source and target on common keys to find mismatches
    src_common = src_with_hash.merge(common_keys, on=key_columns, how="inner")
    tgt_common = tgt_with_hash.merge(common_keys, on=key_columns, how="inner")

    # Merge on keys and compare hashes
    joined = src_common.merge(
        tgt_common, on=key_columns, suffixes=("_source", "_target"), how="inner"
    )

    hash_src_col = f"{ROW_HASH_COL}_source"
    hash_tgt_col = f"{ROW_HASH_COL}_target"

    mismatched_rows = joined[joined[hash_src_col] != joined[hash_tgt_col]]
    matched_count = len(joined) - len(mismatched_rows)

    # Build mismatch report with legacy mismatch_columns string format
    if len(mismatched_rows) > 0:
        drop_cols = [hash_src_col, hash_tgt_col]
        raw_mismatch = mismatched_rows.drop(columns=drop_cols).reset_index(drop=True)

        # Build the mismatch_columns string: [{col : (src_val:tgt_val)}, ...]
        mismatch_col_strings = _build_mismatch_columns_string(
            raw_mismatch, compare_cols, pii_columns, mask_strategy,
        )

        # Assemble final mismatch DataFrame based on report_columns
        mismatch_records = _assemble_mismatch_df(
            raw_mismatch, key_columns, compare_cols, mismatch_col_strings,
            report_columns, pii_columns, mask_strategy,
        )
    else:
        mismatch_records = pd.DataFrame()

    source_extra_count = len(source_extra_records)
    target_extra_count = len(target_extra_records)
    mismatch_count = len(mismatched_rows)

    # Apply PII masking to extras
    if pii_columns:
        pii_extra_cols = [c for c in pii_columns if c in src.columns]
        if pii_extra_cols:
            if source_extra_count > 0:
                source_extra_records = mask_pii_columns(source_extra_records, pii_extra_cols, mask_strategy)
            if target_extra_count > 0:
                target_extra_records = mask_pii_columns(target_extra_records, pii_extra_cols, mask_strategy)

    is_match = mismatch_count == 0 and source_extra_count == 0 and target_extra_count == 0

    return ValidationResult(
        is_match=is_match,
        total_source_rows=total_source,
        total_target_rows=total_target,
        matched_count=matched_count,
        mismatch_count=mismatch_count,
        source_extra_count=source_extra_count,
        target_extra_count=target_extra_count,
        mismatch_records=mismatch_records if mismatch_count > 0 else None,
        source_extra_records=source_extra_records.reset_index(drop=True) if source_extra_count > 0 else None,
        target_extra_records=target_extra_records.reset_index(drop=True) if target_extra_count > 0 else None,
        summary=_build_summary(
            total_source, total_target, matched_count, mismatch_count,
            source_extra_count, target_extra_count,
        ),
    )


def _compare_spark(
    source_df,
    target_df,
    key_columns: List[str],
    exclude_columns: Optional[List[str]],
    pii_columns: Optional[List[str]],
    mask_strategy: str,
    report_columns: str,
    num_partitions: Optional[int],
    persist_intermediates: bool,
    case_sensitive: bool,
) -> ValidationResult:
    """PySpark implementation of DataFrame comparison.

    Uses the same efficient two-stage algorithm as the original validation_util:

    Stage 1 — Hash anti-join (runs on full data, eliminates all matching rows):
        Hash key_columns + compare_cols into a single SHA-256 per row.
        Left-anti join on hash eliminates all rows that match perfectly.
        Result: small delta sets containing ONLY extras and mismatches.

    Stage 2 — Key-based joins (runs on the SMALL delta sets only):
        Key anti-join on deltas → extras (rows with no matching key).
        Key inner-join on deltas → mismatches (same key, different values).

    All comparison logic uses native Spark SQL functions (JVM execution).
    Python UDFs are only used for PII masking on the tiny result sets.
    """
    _, F, _ = _get_spark_imports()
    from pyspark.sql.functions import col, concat_ws, lit, sha2, when
    from pyspark import StorageLevel

    src = source_df
    tgt = target_df

    if not case_sensitive:
        src = src.toDF(*[c.lower() for c in src.columns])
        tgt = tgt.toDF(*[c.lower() for c in tgt.columns])
        key_columns = [k.lower() for k in key_columns]
        if exclude_columns:
            exclude_columns = [c.lower() for c in exclude_columns]
        if pii_columns:
            pii_columns = [c.lower() for c in pii_columns]

    validate_key_columns(src, key_columns)
    validate_key_columns(tgt, key_columns)

    if num_partitions is not None:
        src = src.repartition(num_partitions, *[col(c) for c in key_columns])
        tgt = tgt.repartition(num_partitions, *[col(c) for c in key_columns])

    # Determine comparison columns (value columns excluding any excluded ones)
    compare_cols = [c for c in src.columns if c not in key_columns]
    if exclude_columns:
        compare_cols = [c for c in compare_cols if c not in exclude_columns]

    # Hash includes BOTH key columns and compare columns.
    # This ensures the hash anti-join eliminates matching rows AND preserves
    # extras (rows whose keys only exist on one side) in the delta sets —
    # because different keys produce different hashes.
    hash_cols = key_columns + compare_cols

    src_with_hash = src.withColumn(
        ROW_HASH_COL,
        sha2(
            concat_ws(
                "|",
                *[when(col(c).isNotNull(), col(c).cast("string")).otherwise(lit("__NULL__")) for c in hash_cols],
            ),
            256,
        ),
    )
    tgt_with_hash = tgt.withColumn(
        ROW_HASH_COL,
        sha2(
            concat_ws(
                "|",
                *[when(col(c).isNotNull(), col(c).cast("string")).otherwise(lit("__NULL__")) for c in hash_cols],
            ),
            256,
        ),
    )

    # Persist hashed DataFrames — they are read multiple times:
    # once for the hash anti-join (stage 1) and again as the join partner.
    if persist_intermediates:
        src_with_hash = src_with_hash.persist(StorageLevel.MEMORY_AND_DISK)
        tgt_with_hash = tgt_with_hash.persist(StorageLevel.MEMORY_AND_DISK)

    # Get total counts from the hashed DataFrames (avoids a separate full scan).
    total_source = src_with_hash.count()
    total_target = tgt_with_hash.count()

    # ── Stage 1: Hash anti-join on FULL data ──
    # Eliminates all perfectly matching rows in a single JVM-native operation.
    # After this, delta sets contain ONLY extras + mismatches (typically tiny).
    hash_join_cond = src_with_hash[ROW_HASH_COL] == tgt_with_hash[ROW_HASH_COL]
    src_delta = src_with_hash.join(tgt_with_hash, hash_join_cond, "left_anti")
    tgt_delta = tgt_with_hash.join(src_with_hash, hash_join_cond, "left_anti")

    # Persist deltas — they are read 3 times each:
    # extras (key anti-join), mismatches (key inner-join), and count.
    if persist_intermediates:
        src_delta = src_delta.persist(StorageLevel.MEMORY_AND_DISK)
        tgt_delta = tgt_delta.persist(StorageLevel.MEMORY_AND_DISK)

    # Hashed full DataFrames are no longer needed — free the cache.
    if persist_intermediates:
        src_with_hash.unpersist()
        tgt_with_hash.unpersist()

    # ── Stage 2: Key-based joins on SMALL delta sets ──
    # Extras: rows in one delta whose keys don't exist in the other delta
    source_extra = src_delta.join(tgt_delta, key_columns, "left_anti").drop(ROW_HASH_COL)
    target_extra = tgt_delta.join(src_delta, key_columns, "left_anti").drop(ROW_HASH_COL)

    source_extra_count = source_extra.count()
    target_extra_count = target_extra.count()

    # Mismatches: rows in both deltas with the same key (key matched, values differed)
    mismatch_src = src_delta.drop(ROW_HASH_COL)
    mismatch_tgt = tgt_delta.drop(ROW_HASH_COL)

    # Build mismatch_columns string column: [{col : (src_val:tgt_val)}, ...]
    joined_mismatch = mismatch_src.alias("src").join(
        mismatch_tgt.alias("tgt"), key_columns, "inner"
    )

    # Build per-column mismatch string expressions
    from pyspark.sql.functions import array, array_remove, concat

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

    # Assemble select based on report_columns
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

    from pyspark.sql.functions import length

    mismatch_records = joined_mismatch.select(select_expr).filter(
        length(col("mismatch_columns")) > 2  # filter out "[]"
    )

    # Apply PII masking
    if pii_columns:
        mismatch_records = _apply_pii_masking_spark(
            mismatch_records, pii_columns, mask_strategy, report_columns,
        )

    mismatch_count = mismatch_records.count()
    matched_count = total_source - mismatch_count - source_extra_count

    # Apply PII masking to extras
    if pii_columns:
        pii_extra_cols = [c for c in pii_columns if c in src.columns]
        if pii_extra_cols:
            if source_extra_count > 0:
                source_extra = mask_pii_columns(source_extra, pii_extra_cols, mask_strategy)
            if target_extra_count > 0:
                target_extra = mask_pii_columns(target_extra, pii_extra_cols, mask_strategy)

    is_match = mismatch_count == 0 and source_extra_count == 0 and target_extra_count == 0

    return ValidationResult(
        is_match=is_match,
        total_source_rows=total_source,
        total_target_rows=total_target,
        matched_count=matched_count,
        mismatch_count=mismatch_count,
        source_extra_count=source_extra_count,
        target_extra_count=target_extra_count,
        mismatch_records=mismatch_records if mismatch_count > 0 else None,
        source_extra_records=source_extra if source_extra_count > 0 else None,
        target_extra_records=target_extra if target_extra_count > 0 else None,
        summary=_build_summary(
            total_source, total_target, matched_count, mismatch_count,
            source_extra_count, target_extra_count,
        ),
    )


def _compute_pandas_row_hash(df: pd.DataFrame, columns: List[str]) -> pd.Series:
    """Compute SHA-256 hash for each row across specified columns.

    Null values are represented as '__NULL__' in the hash input to ensure
    consistent hashing behavior.
    """
    def row_hash(row):
        values = []
        for c in columns:
            val = row[c]
            if pd.isna(val):
                values.append("__NULL__")
            else:
                values.append(str(val))
        return hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()

    if columns and len(df) > 0:
        return df[columns].apply(row_hash, axis=1)
    return pd.Series([""] * len(df), dtype="str")


def _build_summary(
    total_source: int,
    total_target: int,
    matched: int,
    mismatched: int,
    source_extras: int,
    target_extras: int,
) -> dict:
    """Build a summary dictionary for the validation result."""
    return {
        "total_source": total_source,
        "total_target": total_target,
        "matched": matched,
        "mismatched": mismatched,
        "source_extras": source_extras,
        "target_extras": target_extras,
    }


def _build_mismatch_columns_string(
    raw_mismatch: pd.DataFrame,
    compare_cols: List[str],
    pii_columns: Optional[List[str]],
    mask_strategy: str,
) -> pd.Series:
    """Build the legacy-format mismatch_columns string for each row.

    Format: [{col : (src_val:tgt_val)}, {col2 : (src_val2:tgt_val2)}]
    Only includes columns that actually differ per row.
    PII column values are masked in the string if pii_columns is specified.
    """
    from databridge_validator.pii.masking import _get_mask_function

    mask_fn = None
    pii_set: set = set()
    if pii_columns:
        mask_fn = _get_mask_function(mask_strategy)
        pii_set = {c.lower() for c in pii_columns}

    def build_row_string(row):
        parts = []
        for c in compare_cols:
            src_col = f"{c}_source"
            tgt_col = f"{c}_target"
            src_val = row[src_col]
            tgt_val = row[tgt_col]

            src_str = "null" if pd.isna(src_val) else str(src_val)
            tgt_str = "null" if pd.isna(tgt_val) else str(tgt_val)

            if src_str != tgt_str:
                # Apply PII masking to values in the string
                if mask_fn and c.lower() in pii_set:
                    src_display = mask_fn(src_str) if src_str != "null" else "null"
                    tgt_display = mask_fn(tgt_str) if tgt_str != "null" else "null"
                else:
                    src_display = src_str
                    tgt_display = tgt_str
                parts.append(f"{{{c} : ({src_display}:{tgt_display})}}")

        return "[" + ", ".join(parts) + "]"

    return raw_mismatch.apply(build_row_string, axis=1)


def _assemble_mismatch_df(
    raw_mismatch: pd.DataFrame,
    key_columns: List[str],
    compare_cols: List[str],
    mismatch_col_strings: pd.Series,
    report_columns: str,
    pii_columns: Optional[List[str]],
    mask_strategy: str,
) -> pd.DataFrame:
    """Assemble the final mismatch DataFrame with the right column set.

    - "target": key_columns + target data columns + mismatch_columns
    - "source": key_columns + source data columns + mismatch_columns
    - "both": key_columns + source cols (_source suffix) + target cols (_target suffix) + mismatch_columns
    """
    result = pd.DataFrame()

    # Key columns (take from either side — they're identical on matched keys)
    for k in key_columns:
        # Keys are un-suffixed in the merged df
        result[k] = raw_mismatch[k]

    if report_columns == "target":
        for c in compare_cols:
            result[c] = raw_mismatch[f"{c}_target"]
    elif report_columns == "source":
        for c in compare_cols:
            result[c] = raw_mismatch[f"{c}_source"]
    else:  # "both"
        for c in compare_cols:
            result[f"{c}_source"] = raw_mismatch[f"{c}_source"]
            result[f"{c}_target"] = raw_mismatch[f"{c}_target"]

    result["mismatch_columns"] = mismatch_col_strings.values

    # Apply PII masking to data columns (not the mismatch_columns string — that's already masked)
    if pii_columns:
        if report_columns == "both":
            pii_data_cols = (
                [f"{c}_source" for c in pii_columns if f"{c}_source" in result.columns]
                + [f"{c}_target" for c in pii_columns if f"{c}_target" in result.columns]
            )
        else:
            pii_data_cols = [c for c in pii_columns if c in result.columns]
        if pii_data_cols:
            result = mask_pii_columns(result, pii_data_cols, mask_strategy)

    return result.reset_index(drop=True)


def _apply_pii_masking_spark(df, pii_columns, mask_strategy, report_columns):
    """Apply PII masking to a Spark mismatch DataFrame."""
    if report_columns == "both":
        pii_data_cols = (
            [f"{c}_source" for c in pii_columns if f"{c}_source" in df.columns]
            + [f"{c}_target" for c in pii_columns if f"{c}_target" in df.columns]
        )
    else:
        pii_data_cols = [c for c in pii_columns if c in df.columns]

    if pii_data_cols:
        df = mask_pii_columns(df, pii_data_cols, mask_strategy)

    # Also mask PII values inside the mismatch_columns string via UDF
    _, F, StringType = _get_spark_imports()
    from pyspark.sql.functions import udf

    from databridge_validator.pii.masking import _get_mask_function

    import re as _re

    mask_fn = _get_mask_function(mask_strategy)
    pii_set = {c.lower() for c in pii_columns}

    def mask_mismatch_str(value):
        if value is None:
            return value
        pattern = _re.compile(r"\{(\w+) : \(([^:]*):([^)]*)\)\}")
        matches = pattern.findall(value)
        parts = []
        for col_name, val1, val2 in matches:
            if col_name.lower() in pii_set:
                masked_v1 = mask_fn(val1) if val1 != "null" else "null"
                masked_v2 = mask_fn(val2) if val2 != "null" else "null"
                parts.append(f"{{{col_name} : ({masked_v1}:{masked_v2})}}")
            else:
                parts.append(f"{{{col_name} : ({val1}:{val2})}}")
        return "[" + ", ".join(parts) + "]"

    mask_mismatch_udf = udf(mask_mismatch_str, StringType())
    return df.withColumn("mismatch_columns", mask_mismatch_udf(F.col("mismatch_columns")))
